/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#ifndef BRPC_META_SERVICE_JOB_H
#define BRPC_META_SERVICE_JOB_H

#include <brpc/server.h>
#include "base_comm_adapter/base_meta_service_job.h"
#include "falcon_meta_rpc.pb.h"
#include "falcon_meta_param_generated.h"

using namespace falcon::meta_proto;
class BrpcMetaServiceJob : public BaseMetaServiceJob {
  private:
    brpc::Controller *m_cntl;
    const MetaRequest *m_request;
    Empty *m_response;
    google::protobuf::Closure *m_done;

  private:
    FalconMetaServiceType AnyMetaParamToFalconMetaServiceType(falcon::meta_fbs::AnyMetaParam type);

  public:
    BrpcMetaServiceJob(brpc::Controller *cntl,
                       const MetaRequest *request,
                       Empty *response,
                       google::protobuf::Closure *done)
        : m_cntl(cntl),
          m_request(request),
          m_response(response),
          m_done(done)
    {
    }

    // Call this function after Job is done to send response and release resource
    void Done() override { m_done->Run(); }

    // always return false since each request contains only one service type
    bool IsAllowBatchProcess() override
    {
        return false;
    }

    // check whether request is empty
    bool IsEmptyRequest() override;

    // get Request Service count - always 1 since each request contains only one service type
    int GetReqServiceCnt() override { return 1; }

    // get Request Data Size
    size_t GetReqDatasize() override { return m_cntl->request_attachment().size(); }

    // copy data to dst
    size_t CopyOutData(void *dst, size_t dstSize) override { return m_cntl->request_attachment().copy_to(dst, dstSize); }

    // get falcon support meta service types from FlatBuffer
    FalconMetaServiceType GetFalconMetaServiceType(int index) override;

    // using shared flatBufferBuilder generate error response msg and reply to client
    void ProcessResponse(void *data, size_t size, FalDataDeleter /* deleter */) override
    {
        // now data transfer to response, and delete by response.
        m_cntl->response_attachment().append_user_data(data, size, NULL);
    }
};

#endif // BRPC_META_SERVICE_JOB_H
