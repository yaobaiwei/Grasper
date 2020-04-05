/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <pthread.h>

class SimpleSpinLockGuard {
 public:
    explicit SimpleSpinLockGuard(pthread_spinlock_t* lock) {
        lock_ = lock;
        if (lock_ != nullptr)
            pthread_spin_lock(lock);
    }

    ~SimpleSpinLockGuard() {
        Unlock();
    }

    void Unlock() {
        if (!unlocked_) {
            unlocked_ = true;
            if (lock_ != nullptr)
                pthread_spin_unlock(lock_);
        }
    }

 private:
    pthread_spinlock_t* lock_ = nullptr;
    bool unlocked_ = false;
};
