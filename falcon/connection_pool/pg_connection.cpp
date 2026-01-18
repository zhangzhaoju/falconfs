/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection.h"
#include <iostream>
#include <sstream>
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils/falcon_shmem_allocator.h"
#include "remote_connection_utils/error_code_def.h"

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
        std::shared_ptr<BaseWorkerTask> baseWorkerTaskPtr(nullptr);
        m_workerTaskQueue.wait_dequeue(baseWorkerTaskPtr);
        if (auto singleTask = dynamic_cast<SingleWorkerTask *>(baseWorkerTaskPtr.get())) {
            BaseMetaServiceJob *job = singleTask->GetJob();
            DoWork(job, conn, flatBufferBuilder, replyBuilder);
            delete job;
        } else if (auto batchTask = dynamic_cast<BatchWorkerTask *>(baseWorkerTaskPtr.get())) {
            for (auto job : batchTask->GetJobList()) {
                DoWork(job, conn, flatBufferBuilder, replyBuilder);
                delete job;
            }
        }
        // now no one handle the ptr, auto release WorkerTask
        baseWorkerTaskPtr = nullptr;
    }
}

void PGConnection::Exec(std::shared_ptr<BaseWorkerTask> workerTaskPtr)
{
    while (!this->m_workerTaskQueue.enqueue(workerTaskPtr)) {
        std::cout << "PGConnection::Exec: enqueue failed" << std::endl;
        std::this_thread::yield();
    }
}

void PGConnection::DoWork(BaseMetaServiceJob *job,
                          PGconn *conn,
                          flatbuffers::FlatBufferBuilder &flatBufferBuilder,
                          SerializedData &replyBuilder)
{
    // 1. Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);

    // Construct send command
    std::stringstream m_toSendCommand;
    bool m_isPlainCommand = false;
    int64_t m_signature = 0;
    uint64_t m_sharedParamDataAddrShift = 0;

    // Reset member variables
    m_toSendCommand.clear();

    // Retrieve information and allocate memory
    size_t requestParamSize = job->GetReqDatasize();
    int requestServiceCount = job->GetReqServiceCnt();
    if (requestServiceCount != 1) {
        throw std::runtime_error("requestServiceCount must be 1");
    }
    m_sharedParamDataAddrShift = FalconShmemAllocatorMalloc(GetFalconConnectionPoolShmemAllocator(), requestParamSize);
    if (m_sharedParamDataAddrShift == 0) {
        printf("Shmem of connection pool is exhausted, requestParamSize: %zu. There may be "
               "several reasons, 1) shmem size is too small, 2) allocate too much memory "
               "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
               requestParamSize);
        fflush(stdout);
        throw std::runtime_error("memory exceed limit.");
    }
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(GetFalconConnectionPoolShmemAllocator(), m_sharedParamDataAddrShift);
    job->CopyOutData(paramBuffer, requestParamSize);
    SerializedData requestData;
    if (!SerializedDataInit(&requestData, paramBuffer, requestParamSize, requestParamSize, NULL))
        throw std::runtime_error("request attachment is corrupt.");

    // Build command (requestServiceCount == 1)
    FalconMetaServiceType serviceType = job->GetFalconMetaServiceType(0);
    uint64_t currentParamSegment = 0;
    int currentParamSegmentCount = 1;
    uint32_t currentParamSegmentSize =
        SerializedDataNextSeveralItemSize(&requestData, currentParamSegment, currentParamSegmentCount);
    char *buf = paramBuffer + currentParamSegment + SERIALIZED_DATA_ALIGNMENT;
    int size = currentParamSegmentSize - SERIALIZED_DATA_ALIGNMENT;
    flatbuffers::Verifier verifier((uint8_t *)buf, size);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>())
        throw std::runtime_error("request param is corrupt. 1");
    const falcon::meta_fbs::MetaParam *param = falcon::meta_fbs::GetMetaParam(buf);

    if (serviceType == FalconMetaServiceType::PLAIN_COMMAND) {
        // PLAIN_COMMAND just using the origin request content.
        auto plainCommandParam = param->param_as_PlainCommandParam();
        if (plainCommandParam == nullptr) {
            throw std::runtime_error("param_as_PlainCommandParam is nullptr, type mismatch.");
        }

        m_toSendCommand << plainCommandParam->command()->c_str();
        m_isPlainCommand = true;
        m_signature = 0;
    } else {
        // construct meta service request, meta service using user defined
        m_signature = FalconShmemAllocatorGetUniqueSignature(GetFalconConnectionPoolShmemAllocator());
        m_toSendCommand << "select falcon_meta_call_by_serialized_shmem_internal(" << serviceType << ", "
                        << currentParamSegmentCount << ", " << m_sharedParamDataAddrShift + currentParamSegment << ", "
                        << m_signature << ");";

        m_isPlainCommand = false;
    }

    // 2. Send request to PG worker process, Send command already constructed in connection pool
    int sendQuerySucceed = PQsendQuery(conn, m_toSendCommand.str().c_str());
    if (sendQuerySucceed != 1) {
        throw std::runtime_error(PQerrorMessage(conn));
    }

    // 3. wait for process Result return
    std::vector<PGresult *> result;
    while ((res = PQgetResult(conn)) != NULL) {
        result.push_back(res);
    }

    FalconShmemAllocatorFree(GetFalconConnectionPoolShmemAllocator(), m_sharedParamDataAddrShift);
    if (!m_isPlainCommand && result.size() != 1) {
        throw std::runtime_error(
            "reply count cannot match request. maybe there is a request containing several plain commands."
            " msg size: " +
            std::to_string(result.size()) +
            " msg type: " + std::to_string(static_cast<int>(job->GetFalconMetaServiceType(0))));
    }

    // 4. Process result
    flatBufferBuilder.Clear();
    SerializedData replyData;
    SerializedDataInit(&replyData, NULL, 0, 0, NULL);
    for (size_t i = 0; i < result.size(); ++i) {
        res = result[i];
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            char *totalErrorMsg = PQresultErrorMessage(res);
            const char *validErrorMsg = NULL;
            FalconErrorCode errorCode = FalconErrorMsgAnalyse(totalErrorMsg, &validErrorMsg);
            if (errorCode == SUCCESS)
                errorCode = PROGRAM_ERROR;

            flatBufferBuilder.Clear();
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(flatBufferBuilder, errorCode);
            flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
            memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
        } else if (m_isPlainCommand) {
            flatBufferBuilder.Clear();
            std::vector<flatbuffers::Offset<flatbuffers::String>> plainCommandResponseData;
            int row = PQntuples(res);
            int col = PQnfields(res);
            for (int i = 0; i < row; ++i)
                for (int j = 0; j < col; ++j)
                    plainCommandResponseData.push_back(flatBufferBuilder.CreateString(PQgetvalue(res, i, j)));
            auto plainCommandResponse =
                falcon::meta_fbs::CreatePlainCommandResponse(flatBufferBuilder,
                                                             row,
                                                             col,
                                                             flatBufferBuilder.CreateVector(plainCommandResponseData));
            auto metaResponse = falcon::meta_fbs::CreateMetaResponse(
                flatBufferBuilder,
                SUCCESS,
                falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_PlainCommandResponse,
                plainCommandResponse.Union());
            flatBufferBuilder.Finish(metaResponse);

            char *buf = SerializedDataApplyForSegment(&replyData, flatBufferBuilder.GetSize());
            memcpy(buf, flatBufferBuilder.GetBufferPointer(), flatBufferBuilder.GetSize());
        } else {
            int64_t signature = m_signature;
            if (PQntuples(res) != 1 || PQnfields(res) != 1)
                throw std::runtime_error("returned reply is corrupt in non-batch operation. 1");
            uint64_t replyShift = (uint64_t)StringToInt64(PQgetvalue(res, 0, 0));
            char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(GetFalconConnectionPoolShmemAllocator(), replyShift);
            if (FALCON_SHMEM_ALLOCATOR_GET_SIGNATURE(replyBuffer) != signature)
                throw std::runtime_error("returned reply is corrupt in non-batch operation. 2");
            uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);

            SerializedData oneReply;
            if (!SerializedDataInit(&oneReply, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                throw std::runtime_error("reply data is corrupt.");
            SerializedDataAppend(&replyData, &oneReply);
            FalconShmemAllocatorFree(GetFalconConnectionPoolShmemAllocator(), replyShift);
        }
    }

    // 2.5.1 SendResponse & recycle resource
    job->ProcessResponse(replyData.buffer, replyData.size, NULL);
    job->Done();

    for (size_t i = 0; i < result.size(); ++i) {
        PQclear(res);
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
