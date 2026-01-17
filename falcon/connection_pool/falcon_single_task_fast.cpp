/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#include "connection_pool/falcon_single_task_fast.h"
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "remote_connection_utils/error_code_def.h"
#include "remote_connection_utils/serialized_data.h"

extern "C" {
#include "utils/error_code.h"
#include "utils/utils_standalone.h"
}

void FalconSingleTaskFast::ConstructSendCommand()
{
    // Reset member variables
    m_toSendCommand.clear();

    // Retrieve information and allocate memory
    size_t requestParamSize = m_job->GetReqDatasize();
    int requestServiceCount = m_job->GetReqServiceCnt();
    if (requestServiceCount != 1) {
        throw std::runtime_error("requestServiceCount must be 1");
    }
    m_sharedParamDataAddrShift = FalconShmemAllocatorMalloc(m_allocator, requestParamSize);
    if (m_sharedParamDataAddrShift == 0) {
        printf("Shmem of connection pool is exhausted, requestParamSize: %zu. There may be "
               "several reasons, 1) shmem size is too small, 2) allocate too much memory "
               "once exceed FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE.",
               requestParamSize);
        fflush(stdout);
        throw std::runtime_error("memory exceed limit.");
    }
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, m_sharedParamDataAddrShift);
    m_job->CopyOutData(paramBuffer, requestParamSize);
    SerializedData requestData;
    if (!SerializedDataInit(&requestData, paramBuffer, requestParamSize, requestParamSize, NULL))
        throw std::runtime_error("request attachment is corrupt.");

    // Build command (requestServiceCount == 1)
    FalconMetaServiceType serviceType = m_job->GetFalconMetaServiceType(0);
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
        m_signature = FalconShmemAllocatorGetUniqueSignature(m_allocator);
        m_toSendCommand << "select falcon_meta_call_by_serialized_shmem_internal(" << serviceType << ", "
                        << currentParamSegmentCount << ", " << m_sharedParamDataAddrShift + currentParamSegment << ", "
                        << m_signature << ");";

        m_isPlainCommand = false;
    }
}

void FalconSingleTaskFast::DoWork(PGconn *conn,
                                  flatbuffers::FlatBufferBuilder &flatBufferBuilder,
                                  SerializedData &replyBuilder)
{
    // 1. Reset status and check validity of input
    PGresult *res{nullptr};
    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);

    ConstructSendCommand();

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

    FalconShmemAllocatorFree(m_allocator, m_sharedParamDataAddrShift);
    if (!m_isPlainCommand && result.size() != 1) {
        throw std::runtime_error(
            "reply count cannot match request. maybe there is a request containing several plain commands."
            " msg size: " +
            std::to_string(result.size()) +
            " msg type: " + std::to_string(static_cast<int>(m_job->GetFalconMetaServiceType(0))));
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
            char *replyBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(m_allocator, replyShift);
            if (FALCON_SHMEM_ALLOCATOR_GET_SIGNATURE(replyBuffer) != signature)
                throw std::runtime_error("returned reply is corrupt in non-batch operation. 2");
            uint64_t replyBufferSize = FALCON_SHMEM_ALLOCATOR_POINTER_GET_SIZE(replyBuffer);

            SerializedData oneReply;
            if (!SerializedDataInit(&oneReply, replyBuffer, replyBufferSize, replyBufferSize, NULL))
                throw std::runtime_error("reply data is corrupt.");
            SerializedDataAppend(&replyData, &oneReply);
            FalconShmemAllocatorFree(m_allocator, replyShift);
        }
    }

    // 2.5.1 SendResponse & recycle resource
    m_job->ProcessResponse(replyData.buffer, replyData.size, NULL);
    m_job->Done();

    for (size_t i = 0; i < result.size(); ++i) {
        PQclear(res);
    }

    delete m_job;
    m_job = nullptr;
}
