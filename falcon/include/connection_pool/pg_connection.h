/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_POOLER_PG_CONNECTION_H
#define FALCON_POOLER_PG_CONNECTION_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "concurrentqueue/blockingconcurrentqueue.h"
#include <flatbuffers/flatbuffers.h>
#include "base_comm_adapter/base_meta_service_job.h"
#include "libpq-fe.h"
#include "remote_connection_utils/serialized_data.h"
#include "utils/falcon_shmem_allocator.h"

class PGConnection {
  public:
    typedef std::function<void(PGConnection *conn)> PGConnectionWorkFinishNotifyFunc;

    PGConnection(PGConnectionWorkFinishNotifyFunc func, const char *ip, const int port, const char *userName);
    ~PGConnection();

    void BackgroundWorker();

    void Exec(BaseMetaServiceJob *jobPtr);

    void DoWork(BaseMetaServiceJob *job, PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder);

    void Stop();

  private:
    bool working;
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;

    moodycamel::BlockingConcurrentQueue<BaseMetaServiceJob *> m_workerTaskQueue;
    std::thread thread;
    PGconn *conn;
};

#endif
