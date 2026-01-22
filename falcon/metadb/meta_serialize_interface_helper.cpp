/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

extern "C" {
#include "metadb/meta_serialize_interface_helper.h"
}

#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"

#include <unordered_map>
#include <unordered_set>
#include <functional>

static flatbuffers::FlatBufferBuilder FlatBufferBuilderPerProcess;

// Table-driven decoder for meta param types
using ParamDecoderFunc = std::function<void(const falcon::meta_fbs::MetaParam *, MetaProcessInfo)>;

// Table-driven encoder for meta param types
using ParamEncoderFunc = std::function<flatbuffers::Offset<falcon::meta_fbs::MetaParam>(
    flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)>;

static const std::unordered_map<int, ParamDecoderFunc> g_paramDecoderTable = {
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::MKDIR;
            info->path = metaParam->param_as_MkdirParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_CreateParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::CREATE;
            info->path = metaParam->param_as_CreateParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_StatParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::STAT;
            info->path = metaParam->param_as_StatParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_OpenParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::OPEN;
            info->path = metaParam->param_as_OpenParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_UnlinkParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::UNLINK;
            info->path = metaParam->param_as_UnlinkParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_OpendirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::OPENDIR;
            info->path = metaParam->param_as_OpendirParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RMDIR;
            info->path = metaParam->param_as_RmdirParam()->path()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubMkdirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::MKDIR_SUB_MKDIR;
            auto mkdirSubMkdirParam = metaParam->param_as_MkdirSubMkdirParam();
            info->parentId = mkdirSubMkdirParam->parent_id();
            info->name = const_cast<char *>(mkdirSubMkdirParam->name()->c_str());
            info->inodeId = mkdirSubMkdirParam->inode_id();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubCreateParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::MKDIR_SUB_CREATE;
            auto mkdirSubCreateParam = metaParam->param_as_MkdirSubCreateParam();
            info->parentId_partId = mkdirSubCreateParam->parent_id_part_id();
            info->name = const_cast<char *>(mkdirSubCreateParam->name()->c_str());
            info->inodeId = mkdirSubCreateParam->inode_id();
            info->st_mode = mkdirSubCreateParam->st_mode();
            info->st_size = mkdirSubCreateParam->st_size();
            info->st_mtim = mkdirSubCreateParam->st_mtim();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_CloseParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::CLOSE;
            auto closeParam = metaParam->param_as_CloseParam();
            info->path = closeParam->path()->c_str();
            info->st_size = closeParam->st_size();
            info->st_mtim = closeParam->st_mtim();
            info->node_id = closeParam->node_id();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ReadDirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::READDIR;
            auto readDirParam = metaParam->param_as_ReadDirParam();
            info->path = readDirParam->path()->c_str();
            info->readDirMaxReadCount = readDirParam->max_read_count();
            info->readDirLastShardIndex = readDirParam->last_shard_index();
            info->readDirLastFileName = readDirParam->last_file_name()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubRmdirParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RMDIR_SUB_RMDIR;
            auto rmdirSubRmdirParam = metaParam->param_as_RmdirSubRmdirParam();
            info->parentId = rmdirSubRmdirParam->parent_id();
            info->name = const_cast<char *>(rmdirSubRmdirParam->name()->c_str());
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubUnlinkParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RMDIR_SUB_UNLINK;
            auto rmdirSubUnlinkParam = metaParam->param_as_RmdirSubUnlinkParam();
            info->parentId_partId = rmdirSubUnlinkParam->parent_id_part_id();
            info->name = const_cast<char *>(rmdirSubUnlinkParam->name()->c_str());
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RENAME;
            auto renameParam = metaParam->param_as_RenameParam();
            info->path = renameParam->src()->c_str();
            info->dstPath = renameParam->dst()->c_str();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubRenameLocallyParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY;
            auto renameSubRenameLocallyParam = metaParam->param_as_RenameSubRenameLocallyParam();
            info->parentId = renameSubRenameLocallyParam->src_parent_id();
            info->parentId_partId = renameSubRenameLocallyParam->src_parent_id_part_id();
            info->name = const_cast<char *>(renameSubRenameLocallyParam->src_name()->c_str());
            info->dstParentId = renameSubRenameLocallyParam->dst_parent_id();
            info->dstParentIdPartId = renameSubRenameLocallyParam->dst_parent_id_part_id();
            info->dstName = const_cast<char *>(renameSubRenameLocallyParam->dst_name()->c_str());
            info->targetIsDirectory = renameSubRenameLocallyParam->target_is_directory();
            info->srcLockOrder = renameSubRenameLocallyParam->src_lock_order();
            info->inodeId = renameSubRenameLocallyParam->directory_inode_id();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubCreateParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::RENAME_SUB_CREATE;
            auto renameSubCreateParam = metaParam->param_as_RenameSubCreateParam();
            info->parentId_partId = renameSubCreateParam->parentid_partid();
            info->name = const_cast<char *>(renameSubCreateParam->name()->c_str());
            info->inodeId = renameSubCreateParam->st_ino();
            info->st_dev = renameSubCreateParam->st_dev();
            info->st_mode = renameSubCreateParam->st_mode();
            info->st_nlink = renameSubCreateParam->st_nlink();
            info->st_uid = renameSubCreateParam->st_uid();
            info->st_gid = renameSubCreateParam->st_gid();
            info->st_rdev = renameSubCreateParam->st_rdev();
            info->st_size = renameSubCreateParam->st_size();
            info->st_blksize = renameSubCreateParam->st_blksize();
            info->st_blocks = renameSubCreateParam->st_blocks();
            info->st_atim = renameSubCreateParam->st_atim();
            info->st_mtim = renameSubCreateParam->st_mtim();
            info->st_ctim = renameSubCreateParam->st_ctim();
            info->node_id = renameSubCreateParam->node_id();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_UtimeNsParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::UTIMENS;
            auto utimeNsParam = metaParam->param_as_UtimeNsParam();
            info->path = utimeNsParam->path()->c_str();
            info->st_atim = utimeNsParam->st_atim();
            info->st_mtim = utimeNsParam->st_mtim();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChownParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::CHOWN;
            auto chownParam = metaParam->param_as_ChownParam();
            info->path = chownParam->path()->c_str();
            info->st_uid = chownParam->st_uid();
            info->st_gid = chownParam->st_gid();
        }
    },
    {
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChmodParam,
        [](const falcon::meta_fbs::MetaParam *metaParam, MetaProcessInfo info) {
            info->serviceType = FalconMetaServiceType::CHMOD;
            auto chmodParam = metaParam->param_as_ChmodParam();
            info->path = chmodParam->path()->c_str();
            info->st_mode = chmodParam->st_mode();
        }
    }
};

static const std::unordered_map<int, ParamEncoderFunc> g_paramEncoderTable = {
    {
        FalconMetaServiceType::MKDIR_SUB_MKDIR,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto mkdirSubMkdirParam =
                falcon::meta_fbs::CreateMkdirSubMkdirParamDirect(builder, info->parentId, info->name, info->inodeId);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_MkdirSubMkdirParam,
                                                     mkdirSubMkdirParam.Union());
        }
    },
    {
        FalconMetaServiceType::MKDIR_SUB_CREATE,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto mkdirSubCreateParam = falcon::meta_fbs::CreateMkdirSubCreateParamDirect(builder,
                                                                                          info->parentId_partId,
                                                                                          info->name,
                                                                                          info->inodeId,
                                                                                          info->st_mode,
                                                                                          info->st_mtim,
                                                                                          info->st_size);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_MkdirSubCreateParam,
                                                     mkdirSubCreateParam.Union());
        }
    },
    {
        FalconMetaServiceType::RMDIR_SUB_RMDIR,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto rmdirSubRmdirParam =
                falcon::meta_fbs::CreateRmdirSubRmdirParamDirect(builder, info->parentId, info->name);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_RmdirSubRmdirParam,
                                                     rmdirSubRmdirParam.Union());
        }
    },
    {
        FalconMetaServiceType::RMDIR_SUB_UNLINK,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto rmdirSubUnlinkParam =
                falcon::meta_fbs::CreateRmdirSubUnlinkParamDirect(builder, info->parentId_partId, info->name);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_RmdirSubUnlinkParam,
                                                     rmdirSubUnlinkParam.Union());
        }
    },
    {
        FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto renameSubRenameLocallyParam =
                falcon::meta_fbs::CreateRenameSubRenameLocallyParamDirect(builder,
                                                                          info->parentId,
                                                                          info->parentId_partId,
                                                                          info->name,
                                                                          info->dstParentId,
                                                                          info->dstParentIdPartId,
                                                                          info->dstName,
                                                                          info->targetIsDirectory,
                                                                          info->inodeId,
                                                                          info->srcLockOrder);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_RenameSubRenameLocallyParam,
                                                     renameSubRenameLocallyParam.Union());
        }
    },
    {
        FalconMetaServiceType::RENAME_SUB_CREATE,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaParam> {
            auto renameSubCreateParam = falcon::meta_fbs::CreateRenameSubCreateParamDirect(builder,
                                                                                           info->parentId_partId,
                                                                                           info->name,
                                                                                           info->inodeId,
                                                                                           info->st_dev,
                                                                                           info->st_mode,
                                                                                           info->st_nlink,
                                                                                           info->st_uid,
                                                                                           info->st_gid,
                                                                                           info->st_rdev,
                                                                                           info->st_size,
                                                                                           info->st_blksize,
                                                                                           info->st_blocks,
                                                                                           info->st_atim,
                                                                                           info->st_mtim,
                                                                                           info->st_ctim,
                                                                                           info->node_id);
            return falcon::meta_fbs::CreateMetaParam(builder,
                                                     falcon::meta_fbs::AnyMetaParam_RenameSubCreateParam,
                                                     renameSubCreateParam.Union());
        }
    }
};

// Response encoder: for creating meta responses
using ResponseEncoderFunc = std::function<flatbuffers::Offset<falcon::meta_fbs::MetaResponse>(
    flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)>;

// Response decoder/encoder helper: for types that only need error code
static const std::unordered_set<int> g_responseErrorOnlyTypes = {
    FalconMetaServiceType::MKDIR,
    FalconMetaServiceType::MKDIR_SUB_MKDIR,
    FalconMetaServiceType::MKDIR_SUB_CREATE,
    FalconMetaServiceType::CLOSE,
    FalconMetaServiceType::RMDIR,
    FalconMetaServiceType::RMDIR_SUB_RMDIR,
    FalconMetaServiceType::RMDIR_SUB_UNLINK,
    FalconMetaServiceType::RENAME,
    FalconMetaServiceType::RENAME_SUB_CREATE,
    FalconMetaServiceType::UTIMENS,
    FalconMetaServiceType::CHOWN,
    FalconMetaServiceType::CHMOD
};

// Helper function for error code only response
static flatbuffers::Offset<falcon::meta_fbs::MetaResponse> CreateErrorCodeOnlyResponse(
    flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
{
    return falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
}

static const std::unordered_map<int, ResponseEncoderFunc> g_responseEncoderTable = {
    {
        FalconMetaServiceType::CREATE,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            auto createResponse = falcon::meta_fbs::CreateCreateResponse(builder,
                                                                         info->inodeId,
                                                                         info->node_id,
                                                                         info->st_dev,
                                                                         info->st_mode,
                                                                         info->st_nlink,
                                                                         info->st_uid,
                                                                         info->st_gid,
                                                                         info->st_rdev,
                                                                         info->st_size,
                                                                         info->st_blksize,
                                                                         info->st_blocks,
                                                                         info->st_atim,
                                                                         info->st_mtim,
                                                                         info->st_ctim);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_CreateResponse,
                                                        createResponse.Union());
        }
    },
    {
        FalconMetaServiceType::STAT,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            auto statResponse = falcon::meta_fbs::CreateStatResponse(builder,
                                                                     info->inodeId,
                                                                     info->st_dev,
                                                                     info->st_mode,
                                                                     info->st_nlink,
                                                                     info->st_uid,
                                                                     info->st_gid,
                                                                     info->st_rdev,
                                                                     info->st_size,
                                                                     info->st_blksize,
                                                                     info->st_blocks,
                                                                     info->st_atim,
                                                                     info->st_mtim,
                                                                     info->st_ctim);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_StatResponse,
                                                        statResponse.Union());
        }
    },
    {
        FalconMetaServiceType::OPEN,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            auto openResponse = falcon::meta_fbs::CreateOpenResponse(builder,
                                                                     info->inodeId,
                                                                     info->node_id,
                                                                     info->st_dev,
                                                                     info->st_mode,
                                                                     info->st_nlink,
                                                                     info->st_uid,
                                                                     info->st_gid,
                                                                     info->st_rdev,
                                                                     info->st_size,
                                                                     info->st_blksize,
                                                                     info->st_blocks,
                                                                     info->st_atim,
                                                                     info->st_mtim,
                                                                     info->st_ctim);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_OpenResponse,
                                                        openResponse.Union());
        }
    },
    {
        FalconMetaServiceType::UNLINK,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            auto unlinkResponse =
                falcon::meta_fbs::CreateUnlinkResponse(builder, info->inodeId, info->st_size, info->node_id);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_UnlinkResponse,
                                                        unlinkResponse.Union());
        }
    },
    {
        FalconMetaServiceType::READDIR,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            std::vector<flatbuffers::Offset<falcon::meta_fbs::OneReadDirResponse>> readDirResultList;
            for (int j = 0; j < info->readDirResultCount; ++j)
                readDirResultList.push_back(
                    falcon::meta_fbs::CreateOneReadDirResponseDirect(builder,
                                                                     info->readDirResultList[j]->fileName,
                                                                     info->readDirResultList[j]->mode));
            auto readDirResponse = falcon::meta_fbs::CreateReadDirResponseDirect(builder,
                                                                                 info->readDirLastShardIndex,
                                                                                 info->readDirLastFileName,
                                                                                 &readDirResultList);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_ReadDirResponse,
                                                        readDirResponse.Union());
        }
    },
    {
        FalconMetaServiceType::OPENDIR,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            auto openDirResponse = falcon::meta_fbs::CreateOpenDirResponse(builder, info->inodeId);
            return falcon::meta_fbs::CreateMetaResponse(builder,
                                                        info->errorCode,
                                                        falcon::meta_fbs::AnyMetaResponse_OpenDirResponse,
                                                        openDirResponse.Union());
        }
    },
    {
        FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY,
        [](flatbuffers::FlatBufferBuilder &builder, MetaProcessInfo info)
            -> flatbuffers::Offset<falcon::meta_fbs::MetaResponse> {
            if (info->parentId_partId != 0 && info->dstParentIdPartId == 0) {
                auto renameSubRenameLocallyResponse =
                    falcon::meta_fbs::CreateRenameSubRenameLocallyResponse(builder,
                                                                           info->inodeId,
                                                                           info->st_dev,
                                                                           info->st_mode,
                                                                           info->st_nlink,
                                                                           info->st_uid,
                                                                           info->st_gid,
                                                                           info->st_rdev,
                                                                           info->st_size,
                                                                           info->st_blksize,
                                                                           info->st_blocks,
                                                                           info->st_atim,
                                                                           info->st_mtim,
                                                                           info->st_ctim,
                                                                           info->node_id);
                return falcon::meta_fbs::CreateMetaResponse(
                    builder,
                    info->errorCode,
                    falcon::meta_fbs::AnyMetaResponse_RenameSubRenameLocallyResponse,
                    renameSubRenameLocallyResponse.Union());
            } else {
                return falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
            }
        }
    }
};

bool SerializedDataMetaParamDecode(int count,
                                   SerializedData *param,
                                   MetaProcessInfoData *infoArray)
{
    sd_size_t p = 0;
    for (int i = 0; i < count; ++i) {
        uint8_t *buffer = (uint8_t *)param->buffer + p;
        sd_size_t size = SerializedDataNextSeveralItemSize(param, p, 1);
        if (size == (sd_size_t)-1) {
            printf("[debug] serialized param is corrupt: %s:%d\n", __FILE__, __LINE__);
            return false;
        }

        uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
        size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
        flatbuffers::Verifier verifier(itemBuffer, itemSize);
        if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL)) {
            printf("[debug] itemSize = %lu, serialized param is corrupt: %s:%d\n", itemSize, __FILE__, __LINE__);
            return false;
        }
        auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);

        MetaProcessInfo info = infoArray + i;
        auto it = g_paramDecoderTable.find(metaParam->param_type());
        if (it != g_paramDecoderTable.end()) {
            it->second(metaParam, info);
        } else {
            info->serviceType = FalconMetaServiceType::NOT_SUPPORTED;
            printf("[debug] serialized param is corrupt: %s:%d\n", __FILE__, __LINE__);
            return false;
        }

        p += size;
    }
    return true;
}

bool SerializedDataMetaParamEncode(FalconMetaServiceType metaService,
                                   MetaProcessInfo *infoArray,
                                   int32_t *index,
                                   int count,
                                   flatbuffers::FlatBufferBuilder &builder,
                                   SerializedData *param)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = index == NULL ? (infoArray[i]) : (infoArray[index[i]]);

        builder.Clear();
        auto it = g_paramEncoderTable.find(metaService);
        if (it == g_paramEncoderTable.end()) {
            return false;
        }
        flatbuffers::Offset<falcon::meta_fbs::MetaParam> metaParam = it->second(builder, info);
        builder.Finish(metaParam);

        char *buffer = SerializedDataApplyForSegment(param, builder.GetSize());
        memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    }
    return true;
}

bool SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(FalconMetaServiceType metaService,
                                                                  MetaProcessInfo *infoArray,
                                                                  int32_t *index,
                                                                  int count,
                                                                  SerializedData *param)
{
    return SerializedDataMetaParamEncode(metaService, infoArray, index, count, FlatBufferBuilderPerProcess, param);
}

bool SerializedDataMetaResponseDecode(FalconMetaServiceType metaService,
                                      int count,
                                      SerializedData *response,
                                      MetaProcessInfoData *infoArray)
{
    sd_size_t p = 0;
    for (int i = 0; i < count; i++) {
        uint8_t *buffer = (uint8_t *)response->buffer + p;
        sd_size_t size = SerializedDataNextSeveralItemSize(response, p, 1);
        if (size == (sd_size_t)-1)
            return false;

        uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
        size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
        flatbuffers::Verifier verifier(itemBuffer, itemSize);
        if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaResponse>(NULL))
            return false;
        auto metaResponse = falcon::meta_fbs::GetMetaResponse(itemBuffer);

        MetaProcessInfo info = infoArray + i;
        info->errorCode = (FalconErrorCode)metaResponse->error_code();

        if (g_responseErrorOnlyTypes.count(metaService) > 0) {
            // Error code only. Do nothing.
        } else if (metaService == FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY) {
            if (metaResponse->response_type() == falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_NONE) {
                // No extra data returned. Do nothing.
            } else if (metaResponse->response_type() ==
                       falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_RenameSubRenameLocallyResponse) {
                auto renameSubRenameLocallyResponse = metaResponse->response_as_RenameSubRenameLocallyResponse();
                info->inodeId = renameSubRenameLocallyResponse->st_ino();
                info->st_dev = renameSubRenameLocallyResponse->st_dev();
                info->st_mode = renameSubRenameLocallyResponse->st_mode();
                info->st_nlink = renameSubRenameLocallyResponse->st_nlink();
                info->st_uid = renameSubRenameLocallyResponse->st_uid();
                info->st_gid = renameSubRenameLocallyResponse->st_gid();
                info->st_rdev = renameSubRenameLocallyResponse->st_rdev();
                info->st_size = renameSubRenameLocallyResponse->st_size();
                info->st_blksize = renameSubRenameLocallyResponse->st_blksize();
                info->st_blocks = renameSubRenameLocallyResponse->st_blocks();
                info->st_atim = renameSubRenameLocallyResponse->st_atim();
                info->st_mtim = renameSubRenameLocallyResponse->st_mtim();
                info->st_ctim = renameSubRenameLocallyResponse->st_ctim();
                info->node_id = renameSubRenameLocallyResponse->node_id();
            } else {
                return false;
            }
        } else {
            return false;
        }

        p += size;
    }
    return true;
}

static bool SerializedDataMetaResponseEncode(int count,
                                             MetaProcessInfoData *infoArray,
                                             flatbuffers::FlatBufferBuilder &builder,
                                             SerializedData *response)
{
    for (int i = 0; i < count; ++i) {
        builder.Clear();
        MetaProcessInfo info = infoArray + i;
        flatbuffers::Offset<falcon::meta_fbs::MetaResponse> metaResponse;
        if (info->errorCode != SUCCESS && info->errorCode != FILE_EXISTS) {
            metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
        } else if (g_responseErrorOnlyTypes.count(info->serviceType) > 0) {
            metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
        } else {
            auto it = g_responseEncoderTable.find(info->serviceType);
            if (it == g_responseEncoderTable.end()) {
                return false;
            }
            metaResponse = it->second(builder, info);
        }
        builder.Finish(metaResponse);

        char *buffer = SerializedDataApplyForSegment(response, builder.GetSize());
        memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    }
    return true;
}

bool SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(int count,
                                                                     MetaProcessInfoData *infoArray,
                                                                     SerializedData *response)
{
    return SerializedDataMetaResponseEncode(count, infoArray, FlatBufferBuilderPerProcess, response);
}
