/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection.h"
#include <iostream>
#include <sstream>
#include <vector>
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils/falcon_shmem_allocator.h"
#include "remote_connection_utils/error_code_def.h"
#include "connection_pool/connection_pool_config.h"

extern "C" {
#include "utils/error_code.h"
#include "utils/utils_standalone.h"
}

PGConnection::PGConnection(PGConnectionWorkFinishNotifyFunc func, const char *ip, const int port, const char *userName)
{
    m_working = true;
    std::stringstream ss;
    ss << "hostaddr=" << ip << " port=" << port << " user=" << userName << " dbname=postgres";
    m_conn = PQconnectdb(ss.str().c_str());
    if (PQstatus(m_conn) != CONNECTION_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQerrorMessage(m_conn));
    }
    PGresult *res = PQexec(m_conn, "SELECT falcon_prepare_commands();");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQresultErrorMessage(res));
    }

    SerializedDataInit(&m_replyData, NULL, 0, 0, NULL);
    m_jobProcessThread = std::thread(&PGConnection::BackgroundWorker, this);
}

void PGConnection::BackgroundWorker()
{
    // batch dequeue up to FalconConnectionPoolBatchSize jobs
    int maxBatch = FalconConnectionPoolBatchSize > 0 ? FalconConnectionPoolBatchSize : 1;
    std::vector<BaseMetaServiceJob *> jobs;
    jobs.resize(maxBatch);
    while (m_working) {
        if (!m_working)
            break;
        // wait_dequeue_bulk will block until at least one element is available
        size_t dequeued = m_workerTaskQueue.wait_dequeue_bulk(jobs.data(), maxBatch);
        if (dequeued == 0)
            continue;

        DoWork(jobs, dequeued);
        for (size_t i = 0; i < dequeued; ++i) {
            delete jobs[i];
            jobs[i] = nullptr;
        }
    }
}

void PGConnection::Exec(BaseMetaServiceJob *jobPtr)
{
    while (!this->m_workerTaskQueue.enqueue(jobPtr)) {
        std::cout << "PGConnection::Exec: enqueue failed" << std::endl;
        std::this_thread::yield();
    }
}

void PGConnection::DoWork(const std::vector<BaseMetaServiceJob *> &jobs, size_t realSize)
{
    // Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(m_conn)) != NULL)
        PQclear(res);

    // process jobs sequentially but allow batching of consecutive non-plain jobs with same serviceType
    size_t idx = 0;
    while (idx < realSize) {
        BaseMetaServiceJob *job = jobs[idx];
        FalconMetaServiceType serviceType = job->GetFalconMetaServiceType(0);
        if (serviceType == FalconMetaServiceType::PLAIN_COMMAND) {
            HandlePlainCommand(job);
            ++idx; // move to next job
        } else {
            // group consecutive non-plain jobs (serviceType may differ) into a single batch
            size_t j = idx;
            // include consecutive jobs until a PLAIN_COMMAND is encountered
            while (j < realSize && jobs[j]->GetFalconMetaServiceType(0) != FalconMetaServiceType::PLAIN_COMMAND) {
                ++j;
            }
            HandleBatchJobs(jobs, idx, j);
            // advance idx to after group
            idx = j;
        }
    }
}

void PGConnection::HandlePlainCommand(BaseMetaServiceJob *job)
{
    // inspect job param to determine sizes and service type
    size_t requestParamSize = job->GetReqDatasize();
    int requestServiceCount = job->GetReqServiceCnt();
    // for plain commands we expect exactly one service param
    if (requestServiceCount != 1) {
        throw std::runtime_error("requestServiceCount must be 1");
    }

    // copy job param to temporary buffer to read service type (safe on heap)
    std::unique_ptr<char[]> tmpBuf(new char[requestParamSize]);
    job->CopyOutData(tmpBuf.get(), requestParamSize);
    SerializedData tmpReq;
    if (!SerializedDataInit(&tmpReq, tmpBuf.get(), requestParamSize, requestParamSize, NULL))
        throw std::runtime_error("request attachment is corrupt.");
    uint64_t curSeg = 0;
    uint32_t segSize = SerializedDataNextSeveralItemSize(&tmpReq, curSeg, 1);
    char *segBuf = tmpBuf.get() + curSeg + SERIALIZED_DATA_ALIGNMENT;
    int segPayloadSize = segSize - SERIALIZED_DATA_ALIGNMENT;
    flatbuffers::Verifier verifier((uint8_t *)segBuf, segPayloadSize);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>())
        throw std::runtime_error("request param is corrupt. 1");
    const falcon::meta_fbs::MetaParam *param = falcon::meta_fbs::GetMetaParam(segBuf);

    // handle plain command individually
    auto plainCommandParam = param->param_as_PlainCommandParam();
    if (plainCommandParam == nullptr) {
        throw std::runtime_error("param_as_PlainCommandParam is nullptr, type mismatch.");
    }
    std::string command = plainCommandParam->command()->c_str();

    int sendQuerySucceed = PQsendQuery(m_conn, command.c_str());
    if (sendQuerySucceed != 1) {
        throw std::runtime_error(PQerrorMessage(m_conn));
    }

    // collect results
    std::vector<PGresult *> result;
    PGresult *res;
    while ((res = PQgetResult(m_conn)) != NULL) {
        result.push_back(res);
    }

    // process result similar to single plain command
    m_flatBufferBuilder.Clear();
    SerializedData replyData;
    SerializedDataInit(&replyData, NULL, 0, 0, NULL);
    for (size_t r = 0; r < result.size(); ++r) {
        PGresult *cres = result[r];
        if (PQresultStatus(cres) != PGRES_TUPLES_OK) {
            char *totalErrorMsg = PQresultErrorMessage(cres);
            const char *validErrorMsg = NULL;
            FalconErrorCode errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
            if (errorCode == SUCCESS)
                errorCode = PROGRAM_ERROR;

            m_flatBufferBuilder.Clear();
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(m_flatBufferBuilder, errorCode);
            m_flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, m_flatBufferBuilder.GetSize());
            memcpy(buf, m_flatBufferBuilder.GetBufferPointer(), m_flatBufferBuilder.GetSize());
        } else {
            m_flatBufferBuilder.Clear();
            std::vector<flatbuffers::Offset<flatbuffers::String>> plainCommandResponseData;
            int row = PQntuples(cres);
            int col = PQnfields(cres);
            for (int i = 0; i < row; ++i)
                for (int j = 0; j < col; ++j)
                    plainCommandResponseData.push_back(m_flatBufferBuilder.CreateString(PQgetvalue(cres, i, j)));
            auto plainCommandResponse = falcon::meta_fbs::CreatePlainCommandResponse(
                m_flatBufferBuilder, row, col, m_flatBufferBuilder.CreateVector(plainCommandResponseData));
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(
                m_flatBufferBuilder,
                SUCCESS,
                falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_PlainCommandResponse,
                plainCommandResponse.Union());
            m_flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, m_flatBufferBuilder.GetSize());
            memcpy(buf, m_flatBufferBuilder.GetBufferPointer(), m_flatBufferBuilder.GetSize());
        }
    }

    job->ProcessResponse(replyData.buffer, replyData.size, NULL);
    job->Done();

    for (size_t r = 0; r < result.size(); ++r) {
        PQclear(result[r]);
    }
}

void PGConnection::HandleBatchJobs(const std::vector<BaseMetaServiceJob *> &jobs, size_t startIdx, size_t endIdx)
{
    FalconMetaServiceType serviceType = jobs[startIdx]->GetFalconMetaServiceType(0);
    uint32_t totalRequestServiceCount = 0;
    size_t totalParamSize = 0;
    for (size_t k = startIdx; k < endIdx; ++k) {
        totalRequestServiceCount += jobs[k]->GetReqServiceCnt();
        totalParamSize += jobs[k]->GetReqDatasize();
    }

    // allocate shared shmem and write params sequentially
    int64_t signature = FalconShmemAllocatorGetUniqueSignature(GetFalconConnectionPoolShmemAllocator());
    uint64_t sharedParamDataAddrShift = FalconShmemAllocatorMalloc(GetFalconConnectionPoolShmemAllocator(), totalParamSize);
    if (sharedParamDataAddrShift == 0) {
        printf("Shmem of connection pool is exhausted, totalParamSize: %zu.\n", totalParamSize);
        fflush(stdout);
        throw std::runtime_error("memory exceed limit.");
    }

    uint64_t curOffset = sharedParamDataAddrShift;
    for (size_t k = startIdx; k < endIdx; ++k) {
        size_t curSize = jobs[k]->GetReqDatasize();
        jobs[k]->CopyOutData(FALCON_SHMEM_ALLOCATOR_GET_POINTER(GetFalconConnectionPoolShmemAllocator(), curOffset), curSize);
        curOffset += curSize;
    }
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(FALCON_SHMEM_ALLOCATOR_GET_POINTER(GetFalconConnectionPoolShmemAllocator(), sharedParamDataAddrShift),
                                         signature);

    // construct command
    char command[128];
    sprintf(command,
            "select falcon_meta_call_by_serialized_shmem_internal(%d, %u, %ld, %ld);",
            serviceType,
            totalRequestServiceCount,
            (int64_t)sharedParamDataAddrShift,
            signature);

    // send and wait for single result (no pipelining)
    int sendQuerySucceed = PQsendQuery(m_conn, command);
    if (sendQuerySucceed != 1)
        throw std::runtime_error(PQerrorMessage(m_conn));

    // get result
    PGresult *res = PQgetResult(m_conn);
    if (res == NULL)
        throw std::runtime_error(PQerrorMessage(m_conn));

    // free request shmem
    FalconErrorCode errorCode = SUCCESS;
    FalconShmemAllocatorFree(GetFalconConnectionPoolShmemAllocator(), sharedParamDataAddrShift);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        char *totalErrorMsg = PQresultErrorMessage(res);
        const char *validErrorMsg = NULL;
        errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
        if (errorCode == SUCCESS)
            errorCode = PROGRAM_ERROR;
    }

    if (errorCode != SUCCESS) {
        // send same error response to all jobs in group
        SerializedDataClear(&m_replyData);
        m_flatBufferBuilder.Clear();
        auto metaResponse = falcon::meta_fbs::CreateMetaResponse(m_flatBufferBuilder, errorCode);
        m_flatBufferBuilder.Finish(metaResponse);
        char *buf = SerializedDataApplyForSegment(&m_replyData, m_flatBufferBuilder.GetSize());
        memcpy(buf, m_flatBufferBuilder.GetBufferPointer(), m_flatBufferBuilder.GetSize());

        for (size_t k = startIdx; k < endIdx; ++k) {
            char *data = (char *)malloc(m_replyData.size);
            memcpy(data, m_replyData.buffer, m_replyData.size);
            jobs[k]->ProcessResponse(data, m_replyData.size, NULL);
            jobs[k]->Done();
        }
    } else {
        if (PQntuples(res) != 1 || PQnfields(res) != 1) {
            throw std::runtime_error("returned reply is corrupt.");
        }
        uint64_t replyShift = (uint64_t)StringToInt64(PQgetvalue(res, 0, 0));
        if (replyShift != 0) {
            char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(GetFalconConnectionPoolShmemAllocator(), replyShift);
            uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);
            SerializedData replyData;
            if (!SerializedDataInit(&replyData, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                throw std::runtime_error("reply data is corrupt.");

            uint32_t p = 0;
            for (size_t k = startIdx; k < endIdx; ++k) {
                int count = jobs[k]->GetReqServiceCnt();
                uint32_t sz = SerializedDataNextSeveralItemSize(&replyData, p, count);
                if (sz == (sd_size_t)-1)
                    throw std::runtime_error("response is corrupt.");
                char *data = (char *)malloc(sz);
                memcpy(data, replyBuffer + p, sz);
                jobs[k]->ProcessResponse(data, sz, NULL);
                jobs[k]->Done();
                p += sz;
            }
            FalconShmemAllocatorFree(GetFalconConnectionPoolShmemAllocator(), replyShift);
        } else {
            for (size_t k = startIdx; k < endIdx; ++k) {
                jobs[k]->Done();
            }
        }
    }

    // clear PQ result
    PQclear(res);
}

void PGConnection::Stop() { m_working = false; }

PGConnection::~PGConnection()
{
    Stop();
    m_jobProcessThread.join();
    if (m_conn) {
        PQfinish(m_conn);
        m_conn = nullptr;
    }
    SerializedDataDestroy(&m_replyData);
}
