/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef EKVSTORE_HPP_
#define EKVSTORE_HPP_

#include <stdint.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <pthread.h>

#include "base/rdma.hpp"
#include "base/type.hpp"
#include "base/serialization.hpp"
#include "base/node_util.hpp"
#include "core/buffer.hpp"
#include "storage/layout.hpp"
#include "utils/mymath.hpp"
#include "third_party/zmq.hpp"
#include "utils/unit.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"
#include "utils/tool.hpp"

/* EKVStore:
 * For Edge-Properties
 * key (main-header and indirect-header region) | value (entry region)
 * The head region is a cluster chaining hash-table (with associativity),
 * while entry region is a varying-size array.
 * */

class EKVStore {
 public:
    EKVStore(Buffer * buf);

    void init(vector<Node> & nodes);

    // Insert a list of Edge properties
    void insert_edge_properties(vector<EProperty*> & eplist);

    // Get properties by key locally
    void get_property_local(uint64_t pid, value_t & val);

    // Get properties by key remotely
    void get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val);

    // Get label by key locally
    void get_label_local(uint64_t pid, label_t & label);

    // Get label by key remotely
    void get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label);

    // Get key locally
    void get_key_local(uint64_t pid, ikey_t & key);

    // Get key remotely
    void get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key);

    // analysis
    void print_mem_usage();

    void ReadSnapshot();
    void WriteSnapshot();

 private:
    Config * config_;
    Buffer * buf_;

    static const int NUM_LOCKS = 1024;

    static const int ASSOCIATIVITY = 8;  // the associativity of slots in each bucket

    // Memory Layout:
    //     NOTE: below two parameters can be configured in .ini file
    //     Header - Entry : 70% : 30%
    //
    //     Header:
    //        Main_Header: 80% Header
    //        Extra_Header: 20% Header
    //     Entry:
    //        Size get from user

    int HD_RATIO;  // header / (header + entry)
    static const int MHD_RATIO = 80;  // main-header / (main-header + indirect-header)

    // size of ekvstore and offset to rdma start point
    char* mem;
    uint64_t mem_sz;
    uint64_t offset;

    // kvstore key
    ikey_t *keys;
    // kvstore value
    char* values;

    uint64_t num_slots;        // 1 bucket = ASSOCIATIVITY slots
    uint64_t num_buckets;      // main-header region (static)
    uint64_t num_buckets_ext;  // indirect-header region (dynamical)
    uint64_t num_entries;      // entry region (dynamical)

    // used
    uint64_t last_ext;
    uint64_t last_entry;

    pthread_spinlock_t entry_lock;
    pthread_spinlock_t bucket_ext_lock;
    pthread_spinlock_t bucket_locks[NUM_LOCKS];  // lock virtualization (see paper: vLokc CGO'13)

    // cluster chaining hash-table (see paper: DrTM SOSP'15)
    uint64_t insert_id(uint64_t _pid);

    // Insert all properties for one edge
    void insert_single_edge_property(EProperty* ep);

    uint64_t sync_fetch_and_alloc_values(uint64_t n);

    // For TCP use
    zmq::context_t context;
    vector<zmq::socket_t *> requesters;
    pthread_spinlock_t req_lock;

    void SendReq(int dst_nid, ibinstream & m);
    bool RecvRep(int nid, obinstream & um);
};

#endif /* EKVSTORE_HPP_ */
