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
#include "base_comm_adapter/base_meta_service_job.h"
#include "concurrentqueue/concurrentqueue.h"
#include "connection_pool/connection_pool_config.h"
#include "connection_pool/falcon_batch_service_def.h"
#include "connection_pool/falcon_single_task_fast.h"
#include "connection_pool/pg_connection.h"
#include "falcon_meta_param_generated.h"
#include "metadb/meta_serialize_interface_helper.h"

struct FalconShardRangeInfo {
    int rangePointMax;
    int serverId;
};

class PGConnectionPoolFast {
  public:
    ~PGConnectionPoolFast() = default;

    // single instance interface
    static PGConnectionPoolFast &GetInstance()
    {
        static PGConnectionPoolFast pgConnectionPool;
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
    PGConnectionPoolFast() = default;

    // Init shard range Info, get range point and server id list from shard table
    void InitShardRangeInfo();

    // map shard id to connection, used to dispatch job to specified shard connection
    std::map<int, PGConnection *> m_connShardMap;

    // shard info get from shard table
    std::vector<FalconShardRangeInfo> m_shardRangeInfoList;

    bool working{false};
};

void PGConnectionPoolFast::InitShardRangeInfo()
{
    // TODO: get shard table data from metadb and init m_shardRangeInfoList
    // now just add a dummy data for test
    int shardCount = 5;
    for (int i = 0; i < shardCount; ++i) {
        int32_t rangePoint;
        if (i == shardCount - 1) {
            rangePoint = INT32_MAX;
        } else {
            rangePoint = ((int64_t)INT32_MAX) * (i + 1) / shardCount;
        }
        // only one server id for test, set server id to 1
        m_shardRangeInfoList.push_back({rangePoint, 1});
    }
}


static int HashPartId(const char *fileName)
{
    uint16_t hashValue = 0;
    for (size_t i = 0; i < strlen(fileName); ++i) {
        hashValue = hashValue * 31 + fileName[i];
    }
    return hashValue & 0x1FFF;
}

static inline uint32_t RotateLeft32(uint32_t word, int n) { return (word << n) | (word >> (32 - n)); }

static uint32_t HashBytesUint32(uint32_t k)
{
    uint32_t a;
    uint32_t b;
    uint32_t c;

    a = b = c = 0x9e3779b9 + static_cast<uint32_t>(sizeof(uint32_t)) + 3923095;
    a += k;

    c ^= b;
    c -= RotateLeft32(b, 14);
    a ^= c;
    a -= RotateLeft32(c, 11);
    b ^= a;
    b -= RotateLeft32(a, 25);
    c ^= b;
    c -= RotateLeft32(b, 16);
    a ^= c;
    a -= RotateLeft32(c, 4);
    b ^= a;
    b -= RotateLeft32(a, 14);
    c ^= b;
    c -= RotateLeft32(b, 24);

    return c;
}

static uint32_t HashInt8(int64_t val)
{
    auto lohalf = static_cast<uint32_t>(val);
    auto hihalf = static_cast<uint32_t>(val >> 32);

    lohalf ^= (val >= 0) ? hihalf : ~hihalf;

    int32_t res = HashBytesUint32(lohalf);

    res &= ~(1u << 31);
    return res;
}


// lifetime of job must be longer than this function. it will be freed later
void PGConnectionPoolFast::DispatchMetaServiceJob(BaseMetaServiceJob *job)
{
    // if job is empty or pool is not working, return directly
    if (job == nullptr || job->IsEmptyRequest() || working == false) {
        std::runtime_error("job is empty.");
        return;
    }

    // construct FalconSingleTaskFast to extract shard key
    std::shared_ptr<FalconSingleTaskFast> task =
        std::make_shared<FalconSingleTaskFast>(GetFalconConnectionPoolShmemAllocator(), job);
    task->ConstructSendCommand();
    std::string extracted_path = task->GetShardKey();

    // 如果有 path，计算分片 hash 并 dispatch 到对应 connection；否则随机选择一个 connection
    PGConnection *targetConn = nullptr;
    if (!extracted_path.empty()) {
        std::string path_copy = extracted_path;
        char *name = basename(const_cast<char*>(path_copy.c_str()));
        uint16_t partId = HashPartId(name);
        auto shardIt = m_connShardMap.lower_bound(HashInt8(partId));
        if (shardIt == m_connShardMap.end()) {
            throw std::runtime_error("shard table is corrupt. cannot find target.");
        }
        targetConn = shardIt->second;
    } else {
        // 随机选择一个 connection
        static bool seeded = false;
        if (!seeded) {
            srand(time(NULL));
            seeded = true;
        }
        int idx = rand() % m_connShardMap.size();
        auto it = m_connShardMap.begin();
        std::advance(it, idx);
        targetConn = it->second;
    }

    // 将 job 投递到目标 connection
    if (targetConn) {
        targetConn->Exec(task);
    } else {
        throw std::runtime_error("No target connection found, for path: " + extracted_path);
    }
}

bool PGConnectionPoolFast::Init(const uint16_t port,
                            const char *userName,
                            const int connPoolSize,
                            const uint16_t pendingTaskBufferMaxSize,
                            const uint16_t batchTaskBufferMaxSize)
{
    // Init shard range info first
    InitShardRangeInfo();

    // init m_connShardMap here, using rangepoint as key
    for (const auto &shardInfo : m_shardRangeInfoList) {
        PGConnection *conn = new PGConnection(nullptr, "127.0.0.1", port, userName);
        m_connShardMap[shardInfo.rangePointMax] = conn;
    }

    working = true;
    return true;
}

void PGConnectionPoolFast::Destroy()
{
    // wait all jobs finished, max wait times is 10 second.
    working = false;
    for (auto it = m_connShardMap.begin(); it != m_connShardMap.end(); ++it) {
        it->second->Stop();
    }
    for (auto it = m_connShardMap.begin(); it != m_connShardMap.end(); ++it) {
        delete it->second;
    }
    m_connShardMap.clear();
}

bool StartPGConnectionPool()
{
    // postgres connection pool init for process jobs dispatched by communication Server
    char *userName = getenv("USER");
    return PGConnectionPoolFast::GetInstance().Init(FalconPGPort, userName, FalconConnectionPoolSize, 20, 400);
}

void DestroyPGConnectionPool() { PGConnectionPoolFast::GetInstance().Destroy(); }

// communication server callback function used to dispatch request to PGConnectionPool
void FalconDispatchMetaJob2PGConnectionPool(void *job)
{
    BaseMetaServiceJob *metaJob = static_cast<BaseMetaServiceJob *>(job);
    PGConnectionPoolFast::GetInstance().DispatchMetaServiceJob(metaJob);
}
