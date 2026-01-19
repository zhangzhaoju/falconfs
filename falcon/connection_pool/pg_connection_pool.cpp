/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection_pool.h"
#include <memory>
#include <string>
#include <map>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <atomic>
#include "base_comm_adapter/base_meta_service_job.h"
#include "concurrentqueue/concurrentqueue.h"
#include "connection_pool/connection_pool_config.h"
#include "connection_pool/falcon_batch_service_def.h"
#include "connection_pool/pg_connection.h"
#include "falcon_meta_param_generated.h"
#include "metadb/meta_serialize_interface_helper.h"
#include "utils/falcon_shmem_allocator.h"

class PGConnectionPool {
  public:
    ~PGConnectionPool() = default;

    // single instance interface
    static PGConnectionPool &GetInstance()
    {
        static PGConnectionPool pgConnectionPool;
        return pgConnectionPool;
    }

    // interface for communication server to call, used to dispatch meta service job to connection pool
    void DispatchMetaServiceJob(BaseMetaServiceJob *job);

    bool Init(const uint16_t port,
              const char *userName,
              const int connPoolSize,
              const uint16_t pendingTaskBufferMaxSize,
              const uint16_t batchTaskBufferMaxSize);
    void Destroy();

  private:
    // define private construct function to avoid create single instance
    PGConnectionPool() = default;
    // vector of connections, used to dispatch job to specified connection
    std::vector<PGConnection *> m_connVec;
    bool working{false};
    std::atomic<size_t> m_roundRobinIndex{0};
};


// lifetime of job must be longer than this function. it will be freed later
void PGConnectionPool::DispatchMetaServiceJob(BaseMetaServiceJob *job)
{
    // if job is empty or pool is not working, return directly
    if (job == nullptr || job->IsEmptyRequest() || working == false) {
        throw std::runtime_error("job is empty.");
    }

    // 轮询选择一个 connection
    // 因为PG要求同一个连接不能流水线式并发执行多个查询，所以轮询选择一个连接来执行任务, 多个连接一起向同一分区表执行查询提升吞吐量
    size_t idx = m_roundRobinIndex.fetch_add(1) % m_connVec.size();
    m_connVec[idx]->Exec(job);
}

bool PGConnectionPool::Init(const uint16_t port,
                            const char *userName,
                            const int connPoolSize,
                            const uint16_t pendingTaskBufferMaxSize,
                            const uint16_t batchTaskBufferMaxSize)
{
    // init m_connVec here
    for (int i = 0; i < connPoolSize; ++i) {
        PGConnection *conn = new PGConnection(nullptr, "127.0.0.1", port, userName);
        m_connVec.push_back(conn);
    }

    working = true;
    return true;
}

void PGConnectionPool::Destroy()
{
    // wait all jobs finished, max wait times is 10 second.
    working = false;
    for (auto conn : m_connVec) {
        conn->Stop();
    }
    for (auto conn : m_connVec) {
        delete conn;
    }
    m_connVec.clear();
}

bool StartPGConnectionPool()
{
    // postgres connection pool init for process jobs dispatched by communication Server
    char *userName = getenv("USER");
    return PGConnectionPool::GetInstance().Init(FalconPGPort, userName, FalconConnectionPoolSize, 20, 400);
}

void DestroyPGConnectionPool() { PGConnectionPool::GetInstance().Destroy(); }

// communication server callback function used to dispatch request to PGConnectionPool
void FalconDispatchMetaJob2PGConnectionPool(void *job)
{
    BaseMetaServiceJob *metaJob = static_cast<BaseMetaServiceJob *>(job);
    PGConnectionPool::GetInstance().DispatchMetaServiceJob(metaJob);
}
