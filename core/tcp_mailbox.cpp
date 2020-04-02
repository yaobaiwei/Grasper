/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)

*/

#include "core/tcp_mailbox.hpp"

using namespace std;

TCPMailbox::~TCPMailbox() {
    for (auto &r : receivers_)
        if (r != NULL) delete r;

    for (auto &s : senders_) {
        if (s.second != NULL) {
            delete s.second;
            s.second = NULL;
        }
    }
}

void TCPMailbox::Init(vector<Node> & nodes) {
    receivers_.resize(config_->global_num_threads);
    for (int tid = 0; tid < config_->global_num_threads; tid++) {
        receivers_[tid] = new zmq::socket_t(context, ZMQ_PULL);
        // Set 10 ms timeout to avoid permanent blocking of local recev buffer
        receivers_[tid]->setsockopt(ZMQ_RCVTIMEO, 10);
        char addr[64] = "";
        sprintf(addr, "tcp://*:%d", my_node_.tcp_port + 1 + tid);
        receivers_[tid]->bind(addr);
    }

    for (int nid = 0; nid < config_->global_num_workers; nid++) {
        Node & r_node = GetNodeById(nodes, nid + 1);
        string ibname = r_node.ibname;

        for (int tid = 0; tid < config_->global_num_threads; tid++) {
            int pcode = port_code(nid, tid);

            senders_[pcode] = new zmq::socket_t(context, ZMQ_PUSH);
            char addr[64] = "";
            sprintf(addr, "tcp://%s:%d", ibname.c_str(), r_node.tcp_port + 1 + tid);
            // FIXME: check return value
            senders_[pcode]->connect(addr);
        }
    }

    locks = (pthread_spinlock_t *)malloc(sizeof(pthread_spinlock_t) * (config_->global_num_threads * config_->global_num_workers));
    for (int n = 0; n < config_->global_num_workers; n++) {
        for (int t = 0; t < config_->global_num_threads; t++)
            pthread_spin_init(&locks[n * config_->global_num_threads + t], 0);
    }

    schedulers = (scheduler_t *)malloc(sizeof(scheduler_t) * config_->global_num_threads);
    memset(schedulers, 0, sizeof(scheduler_t) * config_->global_num_threads);

    local_msgs = (ThreadSafeQueue<Message> **)malloc(sizeof(ThreadSafeQueue<Message>*) * config_->global_num_threads);
    for (int i = 0; i < config_->global_num_threads; i++) {
        local_msgs[i] = new ThreadSafeQueue<Message>();
    }

    local_remote_ratio = 3;
}

int TCPMailbox::Send(int tid, const Message & msg) {
    if (msg.meta.recver_nid == my_node_.get_local_rank()) {
        local_msgs[msg.meta.recver_tid]->Push(msg);
    } else {
        int pcode = port_code(msg.meta.recver_nid, msg.meta.recver_tid);

        ibinstream m;
        m << msg;

        zmq::message_t zmq_msg(m.size());
        memcpy((void *)zmq_msg.data(), m.get_buf(), m.size());

        pthread_spin_lock(&locks[pcode]);
        if (senders_.find(pcode) == senders_.end()) {
            cout << "Cannot find dst_node port num" << endl;
            pthread_spin_unlock(&locks[pcode]);
            return 0;
        }

        senders_[pcode]->send(zmq_msg, ZMQ_DONTWAIT);
        pthread_spin_unlock(&locks[pcode]);
    }
}

bool TCPMailbox::TryRecv(int tid, Message & msg) {
    int type = (schedulers[tid].rr_cnt++) % local_remote_ratio;
    if (type != 0) {
        // Try local msg queue with higher priority
        if (local_msgs[tid]->Size() != 0) {
            local_msgs[tid]->WaitAndPop(msg);
            return true;
        }
    }

    zmq::message_t zmq_msg;
    obinstream um;

    // Try tcp recv
    if (receivers_[tid]->recv(&zmq_msg)) {
        char* buf = new char[zmq_msg.size()];
        memcpy(buf, zmq_msg.data(), zmq_msg.size());
        um.assign(buf, zmq_msg.size(), 0);
        um >> msg;
        return true;
    }
    return false;
}

void TCPMailbox::Recv(int tid, Message & msg) { return; }
void TCPMailbox::Sweep(int tid) { return; }
