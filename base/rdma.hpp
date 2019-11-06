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

/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#pragma GCC diagnostic warning "-fpermissive"

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include "base/node.hpp"

using namespace std;

#ifdef HAS_RDMA

#include "utils/timer.hpp"
#include "rdmalib/rdmaio.hpp"

using namespace rdmaio;

class RDMA {
    class RDMA_Device {
     public:
        RdmaCtrl* ctrl = NULL;

        RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) : num_threads_(num_threads) {
            // record IPs of ndoes
            vector<string> ipset;
            for (const auto & node : nodes)
                ipset.push_back(node.ibname);

            int rdma_port = nodes[0].rdma_port;
            // initialization of new librdma
            // nid, ipset, port, thread_id-no use, enable single memory region
            ctrl = new RdmaCtrl(nid, ipset, rdma_port, true);
            ctrl->open_device();
            ctrl->set_connect_mr(mem, mem_sz);
            ctrl->register_connect_mr();  // single
            ctrl->start_server();
            for (uint i = 0; i < num_nodes; ++i) {
                if (i == nid) {
                    // skip local qp
                    continue;
                }
                for (uint j = 0; j < num_threads * 2; ++j) {
                    Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                    assert(qp != NULL);
                }
            }

            while (1) {
                int connected = 0;
                for (uint i = 0; i < num_nodes; ++i) {
                    if (i == nid) {
                        continue;
                    }
                    for (uint j = 0; j < num_threads * 2; ++j) {
                        Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                        if (qp->inited_) {
                            connected += 1;
                        } else {
                            if (qp->connect_rc()) {
                                connected += 1;
                            }
                        }
                    }
                }
                if (connected == (num_nodes - 1) * num_threads * 2)
                    break;
                else
                    sleep(1);
            }
        }

        // 0 on success, -1 otherwise
        int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            // virtual tid for read
            int vir_tid = dst_tid + num_threads_;

            Qp* qp = ctrl->get_rc_qp(vir_tid, dst_nid);
            qp->rc_post_send(IBV_WR_RDMA_READ, local, size, off, IBV_SEND_SIGNALED);
            if (!qp->first_send())
                qp->poll_completion();

            qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_READ);
        }

        int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);

            // int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            int flags = IBV_SEND_SIGNALED;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            // if(qp->need_poll())
            qp->poll_completion();

            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            // int flags = IBV_SEND_SIGNALED;

            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            if (qp->need_poll())
                qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = 0;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
            return 0;
        }

     private:
        int num_threads_;
    };

 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { if (dev != NULL) delete dev; }

    void init_dev(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) {
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem, mem_sz, nodes);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes);

#else

class RDMA {
    class RDMA_Device {
     public:
        RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
        }

        int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t remote_offset) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }

        int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            cout << "This system is compiled without RDMA support." << endl;
            assert(false);
            return 0;
        }
    };

 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { }

    void init_dev(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) {
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem, mem_sz, nodes);
    }

    inline static bool has_rdma() { return false; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes);

#endif
