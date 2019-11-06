/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <mutex>
#include <queue>
#include <condition_variable>

#include "base/abstract_thread_safe_queue.hpp"

template <typename T>
class ThreadSafeQueue : public AbstractThreadSafeQueue<T> {
 public:
    ThreadSafeQueue() = default;
    ~ThreadSafeQueue() = default;
    ThreadSafeQueue(const ThreadSafeQueue &) = delete;
    ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;
    ThreadSafeQueue(ThreadSafeQueue &&) = delete;
    ThreadSafeQueue &operator=(ThreadSafeQueue &&) = delete;

    void Push(T elem) override {
        mu_.lock();
        queue_.push(std::move(elem));
        mu_.unlock();
        cond_.notify_all();
    }

    void WaitAndPop(T & elem) override {
        std::unique_lock<std::mutex> lk(mu_);
        cond_.wait(lk, [this] { return !queue_.empty(); });
        elem = std::move(queue_.front());
        queue_.pop();
    }

    int Size() override {
        std::lock_guard<std::mutex> lk(mu_);
        return queue_.size();
    }

 private:
    std::mutex mu_;
    std::queue<T> queue_;
    std::condition_variable cond_;
};
