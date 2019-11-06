/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#ifndef PROGRESS_MONITOR_HPP_
#define PROGRESS_MONITOR_HPP_

#include <mutex>

#include "base/node.hpp"
#include "base/communication.hpp"

class Monitor {
 public:
    Monitor(Node & node) : node_(node), num_task_(0), report_end_(false) {}

    void Init() {
        progress_.resize(2);
        progress_[0] = node_.get_world_rank();
    }

    void IncreaseCounter(int num) {
        std::lock_guard<std::mutex> lck(mtx_);
        num_task_ += num;
    }

    void DecreaseCounter(int num) {
        std::lock_guard<std::mutex> lck(mtx_);
        num_task_ -= num;
    }

    void ProgressReport() {
        while (!report_end_) {
            progress_[1] = num_task_;
            send_data(node_, progress_, MASTER_RANK, true, MONITOR_CHANNEL);
            sleep(COMMUN_TIME);
        }
        progress_[1] = -1;
        send_data(node_, progress_, MASTER_RANK, true, MONITOR_CHANNEL);
    }

    void Start() {
        Init();
        thread_ = thread(&Monitor::ProgressReport, this);
    }

    void Stop() {
        report_end_ = true;
        send_data(node_, DONE, MASTER_RANK, true, MSCOMMUN_CHANNEL);
        thread_.join();
    }

 private:
    Node & node_;
    uint32_t num_task_;
    thread thread_;
    vector<uint32_t> progress_;
    bool report_end_;
    std::mutex mtx_;
};

#endif /* PROGRESS_MONITOR_HPP_ */
