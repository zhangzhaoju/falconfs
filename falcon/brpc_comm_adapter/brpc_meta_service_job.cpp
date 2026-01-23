/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#include "brpc_comm_adapter/brpc_meta_service_job.h"

#include <vector>

#define FALCON_REMOTE_CONNECTION_DEF_SERIALIZED_DATA_IMPLEMENT
#include "remote_connection_utils/serialized_data.h"

FalconMetaServiceType BrpcMetaServiceJob::AnyMetaParamToFalconMetaServiceType(falcon::meta_fbs::AnyMetaParam type)
{
    switch (type) {
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_PlainCommandParam:
        return FalconMetaServiceType::PLAIN_COMMAND;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirParam:
        return FalconMetaServiceType::MKDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubMkdirParam:
        return FalconMetaServiceType::MKDIR_SUB_MKDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubCreateParam:
        return FalconMetaServiceType::MKDIR_SUB_CREATE;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_CreateParam:
        return FalconMetaServiceType::CREATE;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_StatParam:
        return FalconMetaServiceType::STAT;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_OpenParam:
        return FalconMetaServiceType::OPEN;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_CloseParam:
        return FalconMetaServiceType::CLOSE;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_UnlinkParam:
        return FalconMetaServiceType::UNLINK;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ReadDirParam:
        return FalconMetaServiceType::READDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_OpendirParam:
        return FalconMetaServiceType::OPENDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirParam:
        return FalconMetaServiceType::RMDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubRmdirParam:
        return FalconMetaServiceType::RMDIR_SUB_RMDIR;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubUnlinkParam:
        return FalconMetaServiceType::RMDIR_SUB_UNLINK;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameParam:
        return FalconMetaServiceType::RENAME;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubRenameLocallyParam:
        return FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubCreateParam:
        return FalconMetaServiceType::RENAME_SUB_CREATE;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_UtimeNsParam:
        return FalconMetaServiceType::UTIMENS;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChownParam:
        return FalconMetaServiceType::CHOWN;
    case falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChmodParam:
        return FalconMetaServiceType::CHMOD;
    default:
        return FalconMetaServiceType::NOT_SUPPORTED;
    }
}

// get falcon support meta service types from FlatBuffer
FalconMetaServiceType BrpcMetaServiceJob::GetFalconMetaServiceType(int index)
{
    if (index != 0) {
        printf("try to get value from idx:%d, only index 0 is supported", index);
        fflush(stdout);
        throw std::runtime_error("input index must be 0.");
    }

    size_t dataSize = m_cntl->request_attachment().size();
    std::vector<uint8_t> buffer(dataSize);
    m_cntl->request_attachment().copy_to(buffer.data(), dataSize);

    // Properly initialize SerializedData object
    SerializedData serializedData;
    SerializedDataInit(&serializedData, (char *)buffer.data(), dataSize, dataSize, nullptr);

    sd_size_t size = SerializedDataNextSeveralItemSize(&serializedData, 0, 1);
    if (size == (sd_size_t)-1)
        throw std::runtime_error("serialized param is corrupt.");

    uint8_t *itemBuffer = buffer.data() + SERIALIZED_DATA_ALIGNMENT;
    size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
    flatbuffers::Verifier verifier(itemBuffer, itemSize);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL))
        throw std::runtime_error("request param is corrupt.");

    auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);
    FalconMetaServiceType supportType = AnyMetaParamToFalconMetaServiceType(metaParam->param_type());
    if (supportType == FalconMetaServiceType::NOT_SUPPORTED) {
        printf("Got unsupport serviceType:%d", metaParam->param_type());
        fflush(stdout);
        throw std::runtime_error("got unsupport serviceType.");
    }

    return supportType;
}

// check whether request is empty
bool BrpcMetaServiceJob::IsEmptyRequest()
{
    // Simply check if the attachment size is 0
    return m_cntl->request_attachment().size() == 0;
}