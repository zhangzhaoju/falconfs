/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/palloc.h"
#include "varatt.h"

#include <unistd.h>
#include "metadb/meta_serialize_interface_helper.h"
#include "utils/error_log.h"
#include "utils/falcon_shmem_allocator.h"

PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_shmem_internal);
PG_FUNCTION_INFO_V1(falcon_meta_call_by_serialized_data);

static SerializedData MetaProcess(int count, char *paramBuffer)
{
    SerializedData param;

    if (!SerializedDataInit(&param, paramBuffer, SD_SIZE_T_MAX, SD_SIZE_T_MAX, NULL))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "SerializedDataInit failed.");

    void *data = palloc((sizeof(MetaProcessInfoData) + sizeof(MetaProcessInfoData *)) * count);
    MetaProcessInfoData *infoDataArray = data;
    MetaProcessInfo *infoArray = (MetaProcessInfo *)(infoDataArray + count);
    if (!SerializedDataMetaParamDecode(count, &param, infoDataArray))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized param is corrupt.");
    for (int i = 0; i < count; i++)
        infoArray[i] = infoDataArray + i;

    // 排序按serviceType, 方便同一类型的任务批量处理
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_service_type);

    int i = 0;
    while (i < count) {
        FalconMetaServiceType currentType = infoArray[i]->serviceType;
        if (currentType == MKDIR || currentType == MKDIR_SUB_MKDIR || currentType == MKDIR_SUB_CREATE ||
            currentType == CREATE || currentType == STAT || currentType == OPEN || currentType == CLOSE || currentType == UNLINK) {
            // 支持批量
            int start = i;
            while (i < count && infoArray[i]->serviceType == currentType) i++;
            int batchSize = i - start;
            switch (currentType) {
            case MKDIR:
                FalconMkdirHandle(infoArray + start, batchSize);
                break;
            case MKDIR_SUB_MKDIR:
                FalconMkdirSubMkdirHandle(infoArray + start, batchSize);
                break;
            case MKDIR_SUB_CREATE:
                FalconMkdirSubCreateHandle(infoArray + start, batchSize);
                break;
            case CREATE:
                FalconCreateHandle(infoArray + start, batchSize, false);
                break;
            case STAT:
                FalconStatHandle(infoArray + start, batchSize);
                break;
            case OPEN:
                FalconOpenHandle(infoArray + start, batchSize);
                break;
            case CLOSE:
                FalconCloseHandle(infoArray + start, batchSize);
                break;
            case UNLINK:
                FalconUnlinkHandle(infoArray + start, batchSize);
                break;
            }
        } else {
            // 不支持批量，单个调用
            switch (currentType) {
            case READDIR:
                FalconReadDirHandle(infoArray[i]);
                break;
            case OPENDIR:
                FalconOpenDirHandle(infoArray[i]);
                break;
            case RMDIR:
                FalconRmdirHandle(infoArray[i]);
                break;
            case RMDIR_SUB_RMDIR:
                FalconRmdirSubRmdirHandle(infoArray[i]);
                break;
            case RMDIR_SUB_UNLINK:
                FalconRmdirSubUnlinkHandle(infoArray[i]);
                break;
            case RENAME:
                FalconRenameHandle(infoArray[i]);
                break;
            case RENAME_SUB_RENAME_LOCALLY:
                FalconRenameSubRenameLocallyHandle(infoArray[i]);
                break;
            case RENAME_SUB_CREATE:
                FalconRenameSubCreateHandle(infoArray[i]);
                break;
            case UTIMENS:
                FalconUtimeNsHandle(infoArray[i]);
                break;
            case CHOWN:
                FalconChownHandle(infoArray[i]);
                break;
            case CHMOD:
                FalconChmodHandle(infoArray[i]);
                break;
            default:
                FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "unexpected serviceType: %d", currentType);
            }
            i++;
        }
    }

    SerializedData response;
    SerializedDataInit(&response, NULL, 0, 0, &PgMemoryManager);
    if (!SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(count, infoDataArray, &response))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "failed when serializing response.");

    return response;
}

Datum falcon_meta_call_by_serialized_shmem_internal(PG_FUNCTION_ARGS)
{
    int32_t type = PG_GETARG_INT32(0);
    int32_t count = PG_GETARG_INT32(1);
    uint64_t paramShmemShift = (uint64_t)PG_GETARG_INT64(2);
    int64_t signature = PG_GETARG_INT64(3);

    // set type to FalconMetaServiceType from the send end.
    FalconMetaServiceType metaService = (FalconMetaServiceType)type;
    FalconShmemAllocator *allocator = GetFalconConnectionPoolShmemAllocator();
    if (paramShmemShift > allocator->pageCount * FALCON_SHMEM_ALLOCATOR_PAGE_SIZE)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "paramShmemShift is invalid.");
    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, paramShmemShift);

    SerializedData response = MetaProcess(count, paramBuffer);

    uint64_t responseShmemShift = FalconShmemAllocatorMalloc(allocator, response.size);
    if (responseShmemShift == 0)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "FalconShmemAllocMalloc failed. Size: %u.", response.size);
    char *responseBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, responseShmemShift);
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(responseBuffer, signature);
    memcpy(responseBuffer, response.buffer, response.size);

    PG_RETURN_INT64(responseShmemShift);
}

Datum falcon_meta_call_by_serialized_data(PG_FUNCTION_ARGS)
{
    int32_t type = PG_GETARG_INT32(0);
    int32_t count = PG_GETARG_INT32(1);
    bytea *param = PG_GETARG_BYTEA_P(2);

    FalconMetaServiceType metaService = (FalconMetaServiceType)type;
    char *paramBuffer = VARDATA_ANY(param);

    SerializedData response = MetaProcess(count, paramBuffer);

    bytea *reply = (bytea *)palloc(VARHDRSZ + response.size);
    memcpy(VARDATA_4B(reply), response.buffer, response.size);
    SET_VARSIZE_4B(reply, VARHDRSZ + response.size);

    PG_RETURN_BYTEA_P(reply);
}
