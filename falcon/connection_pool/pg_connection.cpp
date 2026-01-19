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
    working = true;
    std::stringstream ss;
    ss << "hostaddr=" << ip << " port=" << port << " user=" << userName << " dbname=postgres";
    conn = PQconnectdb(ss.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQerrorMessage(conn));
    }
    PGresult *res = PQexec(conn, "SELECT falcon_prepare_commands();");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQresultErrorMessage(res));
    }

    SerializedDataInit(&replyBuilder, NULL, 0, 0, NULL);
    this->thread = std::thread(&PGConnection::BackgroundWorker, this);
}

void PGConnection::BackgroundWorker()
{
    while (working) {
        if (!working)
            break;

        // batch dequeue up to FalconConnectionPoolBatchSize jobs
        int maxBatch = FalconConnectionPoolBatchSize > 0 ? FalconConnectionPoolBatchSize : 1;
        std::vector<BaseMetaServiceJob *> jobs;
        jobs.resize(maxBatch);

        // wait_dequeue_bulk will block until at least one element is available
        size_t dequeued = m_workerTaskQueue.wait_dequeue_bulk(jobs.data(), maxBatch);
        if (dequeued == 0)
            continue;

        // build a compact vector of actual jobs
        std::vector<BaseMetaServiceJob *> actualJobs;
        actualJobs.reserve(dequeued);
        for (size_t i = 0; i < dequeued; ++i) {
            if (jobs[i] != nullptr)
                actualJobs.push_back(jobs[i]);
        }
        if (!actualJobs.empty()) {
            DoWork(actualJobs, conn, flatBufferBuilder, replyBuilder);
            for (auto j : actualJobs)
                delete j;
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

void PGConnection::DoWork(const std::vector<BaseMetaServiceJob *> &jobs,
                          PGconn *conn,
                          flatbuffers::FlatBufferBuilder &flatBufferBuilder,
                          SerializedData &replyBuilder)
{
    // Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);

    // process jobs sequentially but allow batching of consecutive non-plain jobs with same serviceType
    size_t idx = 0;
    while (idx < jobs.size()) {
        BaseMetaServiceJob *job = jobs[idx];
        if (job == nullptr) {
            ++idx;
            continue;
        }

        FalconMetaServiceType serviceType = job->GetFalconMetaServiceType(0);
        if (serviceType == FalconMetaServiceType::PLAIN_COMMAND) {
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

            int sendQuerySucceed = PQsendQuery(conn, command.c_str());
            if (sendQuerySucceed != 1) {
                throw std::runtime_error(PQerrorMessage(conn));
            }

            // collect results
            std::vector<PGresult *> result;
            while ((res = PQgetResult(conn)) != NULL) {
                result.push_back(res);
            }

            // process result similar to single plain command
            flatBufferBuilder.Clear();
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

                    flatBufferBuilder.Clear();
                    auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
                    flatBufferBuilder.Finish(metaResponse);

                    char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
                    memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
                } else {
                    flatBufferBuilder.Clear();
                    std::vector<flatbuffers::Offset<flatbuffers::String>> plainCommandResponseData;
                    int row = PQntuples(cres);
                    int col = PQnfields(cres);
                    for (int i = 0; i < row; ++i)
                        for (int j = 0; j < col; ++j)
                            plainCommandResponseData.push_back(flatBufferBuilder.CreateString(PQgetvalue(cres, i, j)));
                    auto plainCommandResponse = falcon::meta_fbs::CreatePlainCommandResponse(
                        flatBufferBuilder, row, col, flatBufferBuilder.CreateVector(plainCommandResponseData));
                    auto metaResponse = falcon::meta_fbs::CreateMetaResponse(
                        flatBufferBuilder,
                        SUCCESS,
                        falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_PlainCommandResponse,
                        plainCommandResponse.Union());
                    flatBufferBuilder.Finish(metaResponse);

                    char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
                    memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
                }
            }

            job->ProcessResponse(replyData.buffer, replyData.size, NULL);
            job->Done();

            for (size_t r = 0; r < result.size(); ++r) {
                PQclear(result[r]);
            }

            ++idx; // move to next job
        } else {
            // group consecutive non-plain jobs (serviceType may differ) into a single batch
            size_t j = idx;
            uint32_t totalRequestServiceCount = 0;
            size_t totalParamSize = 0;
            // include consecutive jobs until a PLAIN_COMMAND is encountered
            while (j < jobs.size() && jobs[j]->GetFalconMetaServiceType(0) != FalconMetaServiceType::PLAIN_COMMAND) {
                totalRequestServiceCount += jobs[j]->GetReqServiceCnt();
                totalParamSize += jobs[j]->GetReqDatasize();
                ++j;
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
            for (size_t k = idx; k < j; ++k) {
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
            int sendQuerySucceed = PQsendQuery(conn, command);
            if (sendQuerySucceed != 1)
                throw std::runtime_error(PQerrorMessage(conn));

            // get result
            res = PQgetResult(conn);
            if (res == NULL)
                throw std::runtime_error(PQerrorMessage(conn));

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
                SerializedDataClear(&replyBuilder);
                flatBufferBuilder.Clear();
                auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
                flatBufferBuilder.Finish(metaResponse);
                char *buf = SerializedDataApplyForSegment(&replyBuilder, flatBufferBuilder.GetSize());
                memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());

                for (size_t k = idx; k < j; ++k) {
                    char *data = (char *)malloc(replyBuilder.size);
                    memcpy(data, replyBuilder.buffer, replyBuilder.size);
                    jobs[k]->ProcessResponse(data, replyBuilder.size, NULL);
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
                    for (size_t k = idx; k < j; ++k) {
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
                    for (size_t k = idx; k < j; ++k) {
                        jobs[k]->Done();
                    }
                }
            }

            // clear PQ result
            PQclear(res);

            // advance idx to after group
            idx = j;
        }
    }
}

void PGConnection::Stop() { working = false; }

PGConnection::~PGConnection()
{
    Stop();
    thread.join();
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
    SerializedDataDestroy(&replyBuilder);
}
