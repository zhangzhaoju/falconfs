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
#include <thread>
#include <chrono>
#include "base_comm_adapter/base_meta_service_job.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
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
    void DispatchPendingJobs();
    // define private construct function to avoid create single instance
    PGConnectionPool() = default;
    // vector of connections, used to dispatch job to specified connection
    std::vector<PGConnection *> m_connVec;
    std::atomic<bool> working{false};
    moodycamel::BlockingConcurrentQueue<BaseMetaServiceJob *> m_jobsWaitingProcessQueue;
    moodycamel::BlockingConcurrentQueue<PGConnection *> m_idleConnQueue;
    std::thread m_dispatchThread;
};

void PGConnectionPool::DispatchPendingJobs()
{
    // batch size aligns with connection-side processing
    const int maxBatch = (FalconConnectionPoolBatchSize > 0) ? FalconConnectionPoolBatchSize : 1;
    std::vector<BaseMetaServiceJob *> jobs;
    jobs.resize(maxBatch);
    while (working || m_jobsWaitingProcessQueue.size_approx() > 0) {
        size_t dequeued = m_jobsWaitingProcessQueue.wait_dequeue_bulk(jobs.data(), maxBatch);
        if (dequeued == 0) {
            continue;
        }

        PGConnection *conn = nullptr;
        // wait for an idle connection; bail out if shutting down
        while (working && !m_idleConnQueue.wait_dequeue_timed(conn, std::chrono::milliseconds(100))) {
            continue;
        }

        if (conn == nullptr) {
            // shutdown path; requeue the chunk and exit loop
            for (size_t i = 0; i < dequeued; ++i) {
                m_jobsWaitingProcessQueue.enqueue(jobs[i]);
            }
            break;
        }

        conn->ExecBulk(jobs.data(), dequeued);
    }
}


// lifetime of job must be longer than this function. it will be freed later
void PGConnectionPool::DispatchMetaServiceJob(BaseMetaServiceJob *job)
{
    // if job is empty or pool is not working, return directly
    if (job == nullptr || job->IsEmptyRequest() || working == false) {
        throw std::runtime_error("job is empty.");
    }

    while (!m_jobsWaitingProcessQueue.enqueue(job)) {
        std::this_thread::yield();
    }
}

bool PGConnectionPool::Init(const uint16_t port,
                            const char *userName,
                            const int connPoolSize,
                            const uint16_t pendingTaskBufferMaxSize,
                            const uint16_t batchTaskBufferMaxSize)
{
    // init m_connVec here
    auto onConnectionIdle = [this](PGConnection *conn) {
        // connection notifies pool it is idle again
        m_idleConnQueue.enqueue(conn);
    };
    for (int i = 0; i < connPoolSize; ++i) {
        PGConnection *conn = new PGConnection(onConnectionIdle, "127.0.0.1", port, userName);
        m_connVec.push_back(conn);
        m_idleConnQueue.enqueue(conn);
    }

    working = true;
    m_dispatchThread = std::thread(&PGConnectionPool::DispatchPendingJobs, this);
    return true;
}

void PGConnectionPool::Destroy()
{
    // wait all jobs finished, max wait times is 10 second.
    working = false;
    m_jobsWaitingProcessQueue.enqueue(nullptr);
    m_idleConnQueue.enqueue(nullptr);
    if (m_dispatchThread.joinable()) {
        m_dispatchThread.join();
    }
    for (auto conn : m_connVec) {
        conn->Stop();
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
