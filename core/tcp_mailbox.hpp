/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)

*/

#pragma once

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <sstream>

#include "base/node_util.hpp"
#include "base/thread_safe_queue.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/message.hpp"
#include "third_party/zmq.hpp"

#include <tbb/concurrent_unordered_map.h>
#define CLINE 64

using namespace std;

class TCPMailbox : public AbstractMailbox {
 private:
    typedef tbb::concurrent_unordered_map<int, zmq::socket_t *> socket_map;
    typedef vector<zmq::socket_t *> socket_vector;

    // The communication over zeromq, a socket library.
    zmq::context_t context;
    socket_vector receivers_;
    socket_map senders_;

    Node & my_node_;
    Config * config_;

    pthread_spinlock_t *locks;

    // each thread uses a round-robin strategy to check its physical-queues
    struct scheduler_t {
        // round-robin
        uint64_t rr_cnt;  // choosing local or remote
    } __attribute__((aligned(CLINE)));
    scheduler_t *schedulers;

    ThreadSafeQueue<Message>** local_msgs;

    // Ratio for choosing local msg over choosing remote msg
    int local_remote_ratio;

    inline int port_code (int nid, int tid) { return nid * config_->global_num_threads + tid; }

 public:
    TCPMailbox(Node & my_node) : my_node_(my_node), context(1) {
        config_ = Config::GetInstance();
    }

    ~TCPMailbox();

    void Init(vector<Node> & nodes) override;
    int Send(int tid, const Message & msg) override;
    void Recv(int tid, Message & msg) override;
    bool TryRecv(int tid, Message & msg) override;
    void Sweep(int tid) override;
};
