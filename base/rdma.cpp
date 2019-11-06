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

#include "base/rdma.hpp"

#ifdef HAS_RDMA

void RDMA_init(int num_nodes,  int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) {
    uint64_t t = timer::get_usec();

    // init RDMA device
    RDMA &rdma = RDMA::get_rdma();
    rdma.init_dev(num_nodes, num_threads, nid, mem, mem_sz, nodes);

    t = timer::get_usec() - t;
    cout << "INFO: initializing RDMA done (" << t / 1000  << " ms)" << endl;
}

#else

void RDMA_init(int num_nodes,  int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes) {
    std::cout << "This system is compiled without RDMA support." << std::endl;
}

#endif


