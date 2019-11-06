/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */
#include "rdmaio.hpp"
namespace rdmaio {

// extern __thread RdmaDevice **rdma_devices_;
int tcp_base_port;
int num_rc_qps;
int num_uc_qps;
int num_ud_qps;
int node_id;

std::vector<std::string> network;

// seems that zeromq requires to use one context per process
zmq::context_t context(12);


// per-thread allocator
__thread RdmaDevice **rdma_devices_;

bool Qp::get_ud_connect_info(int remote_id, int idx) {

    // already gotten
    if (ahs_[remote_id] != NULL) {
        return true;
    }

    //int qid = _QP_ENCODE_ID(0,UD_ID_BASE + tid * num_ud_qps + idx);
    uint64_t qid = _QP_ENCODE_ID(tid + UD_ID_BASE, UD_ID_BASE + idx);

    char address[30];
    snprintf(address, 30, "tcp://%s:%d", network[remote_id].c_str(), tcp_base_port);

    // prepare tcp connection
    zmq::socket_t socket(context, ZMQ_REQ);
    socket.connect(address);

    zmq::message_t request(sizeof(QPConnArg));

    QPConnArg *argp = (QPConnArg *)(request.data());
    argp->qid = qid;
    argp->sign = MAGIC_NUM;
    argp->calculate_checksum();

    socket.send(request);

    zmq::message_t reply;
    socket.recv(&reply);

    // the first byte of the message is used to identify the status of the request
    if (((char *)reply.data())[0] == TCPSUCC) {

    } else if (((char *)reply.data())[0] == TCPFAIL) {
        return false;

    } else {
        fprintf(stdout, "QP connect fail!, val %d\n", ((char *)reply.data())[0]);
        assert(false);
    }
    zmq_close(&socket);
    RdmaQpAttr qp_attr;
    memcpy(&qp_attr, (char *)reply.data() + 1, sizeof(RdmaQpAttr));

    // verify the checksum
    uint64_t checksum = ip_checksum((void *)(&(qp_attr.buf)), sizeof(RdmaQpAttr) - sizeof(uint64_t));
    assert(checksum == qp_attr.checksum);

    int dlid = qp_attr.lid;

    ahs_[remote_id] = RdmaCtrl::create_ah(dlid, port_idx, dev_);
    memcpy(&(ud_attrs_[remote_id]), (char *)reply.data() + 1, sizeof(RdmaQpAttr));

    return true;
}

bool Qp::get_ud_connect_info_specific(int remote_id, int thread_id, int idx) {

    auto key = _QP_ENCODE_ID(remote_id, thread_id);
    if (ahs_.find(key) != ahs_.end()) {
        return true;
    }

    uint64_t qid = _QP_ENCODE_ID(thread_id + UD_ID_BASE, UD_ID_BASE + idx);

    char address[30];
    snprintf(address, 30, "tcp://%s:%d", network[remote_id].c_str(), tcp_base_port);

    // prepare tcp connection
    zmq::socket_t socket(context, ZMQ_REQ);
    socket.connect(address);

    zmq::message_t request(sizeof(QPConnArg));

    QPConnArg *argp = (QPConnArg *)(request.data());
    argp->qid = qid;
    argp->sign = MAGIC_NUM;
    argp->calculate_checksum();

    socket.send(request);

    zmq::message_t reply;
    socket.recv(&reply);

    // the first byte of the message is used to identify the status of the request
    if (((char *)reply.data())[0] == TCPSUCC) {

    } else if (((char *)reply.data())[0] == TCPFAIL) {
        return false;

    } else {
        fprintf(stdout, "QP connect fail!, val %d\n", ((char *)reply.data())[0]);
        assert(false);
    }
    zmq_close(&socket);
    RdmaQpAttr qp_attr;
    memcpy(&qp_attr, (char *)reply.data() + 1, sizeof(RdmaQpAttr));

    // verify the checksum
    uint64_t checksum = ip_checksum((void *)(&(qp_attr.buf)), sizeof(RdmaQpAttr) - sizeof(uint64_t));
    assert(checksum == qp_attr.checksum);

    int dlid = qp_attr.lid;

    ahs_.insert(std::make_pair(key, RdmaCtrl::create_ah(dlid, port_idx, dev_)));
    ud_attrs_.insert(std::make_pair(key, RdmaQpAttr()));
    memcpy(&(ud_attrs_[key]), (char *)reply.data() + 1, sizeof(RdmaQpAttr));

    return true;
}



}

// private helper functions ////////////////////////////////////////////////////////////////////

void rc_ready2init(ibv_qp * qp, int port_id) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = port_id;
    qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_ATOMIC;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify RC to INIT state, %s\n", strerror(errno));
}

void rc_init2rtr(ibv_qp * qp, int port_id, int qpn, int dlid) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = IBV_MTU_4096;
    qp_attr.dest_qp_num = qpn;
    qp_attr.rq_psn = DEFAULT_PSN;
    qp_attr.max_dest_rd_atomic = 16;
    qp_attr.min_rnr_timer = 12;

    qp_attr.ah_attr.is_global = 0;
    qp_attr.ah_attr.dlid = dlid;
    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = port_id; /* Local port! */

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN
            | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify RC to RTR state, %s\n", strerror(errno));
}

void rc_rtr2rts(ibv_qp * qp) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = DEFAULT_PSN;
    qp_attr.timeout = 15;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.max_rd_atomic = 16;
    qp_attr.max_dest_rd_atomic = 16;

    flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
            IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify RC to RTS state, %s\n", strerror(errno));
}

void uc_ready2init(ibv_qp * qp, int port_id) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = port_id;
    qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify UC to INIT state, %s\n", strerror(errno));
}

void uc_init2rtr(ibv_qp * qp, int port_id, int qpn, int dlid) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = IBV_MTU_4096;
    qp_attr.dest_qp_num = qpn;
    qp_attr.rq_psn = DEFAULT_PSN;

    qp_attr.ah_attr.is_global = 0;
    qp_attr.ah_attr.dlid = dlid;
    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = port_id; /* Local port! */

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    rc = ibv_modify_qp(qp, &qp_attr, flags);

    CE_1(rc, "[librdma] qp: Failed to modify UC to RTR state, %s\n", strerror(errno));
}

void uc_rtr2rts(ibv_qp * qp) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = DEFAULT_PSN;

    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify RC to RTS state, %s\n", strerror(errno));
}

void ud_ready2init(ibv_qp * qp, int port_id) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset((void *) &qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = port_id;
    qp_attr.qkey = DEFAULT_QKEY;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify UD to INIT state, %s\n", strerror(errno));
}

void ud_init2rtr(ibv_qp * qp) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset((void *) &qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTR;

    flags = IBV_QP_STATE;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify UD to RTR state, %s\n", strerror(errno));
}

void ud_rtr2rts(ibv_qp * qp) {
    int rc, flags;
    struct ibv_qp_attr qp_attr;
    memset((void *) &qp_attr, 0, sizeof(struct ibv_qp_attr));
    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = DEFAULT_PSN;

    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
    rc = ibv_modify_qp(qp, &qp_attr, flags);
    CE_1(rc, "[librdma] qp: Failed to modify UD to RTS state, %s\n", strerror(errno));
}
