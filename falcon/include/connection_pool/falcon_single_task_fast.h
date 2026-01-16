/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_SINGLE_TASK_FAST_H
#define FALCON_SINGLE_TASK_FAST_H

#include "connection_pool/falcon_worker_task.h"
#include <sstream>

class FalconSingleTaskFast : public BaseWorkerTask {
  public:
    FalconSingleTaskFast(FalconShmemAllocator *allocator, BaseMetaServiceJob *job)
        : BaseWorkerTask(allocator),
          m_job(job),
          m_isPlainCommand(false),
          m_signature(0),
          m_sharedParamDataAddrShift(0)
    {
    }
    ~FalconSingleTaskFast() override {}

    // implement logic of SingleWorker process
    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) override;

    // construct send command to PG worker process
    void ConstructSendCommand();

    // get shard key for connection selection
    std::string GetShardKey() const { return m_shard_key; }

  private:

    BaseMetaServiceJob *m_job{nullptr};
    // command to send to PG worker process
    std::stringstream m_toSendCommand;
    // whether is plain command
    bool m_isPlainCommand;
    // signature for shmem allocation
    int64_t m_signature;
    // shared memory address shift for request param
    uint64_t m_sharedParamDataAddrShift;
    // shard key extracted from request param
    std::string m_shard_key;
};

#endif // FALCON_SINGLE_TASK_FAST_H
