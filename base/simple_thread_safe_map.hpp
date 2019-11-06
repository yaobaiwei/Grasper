/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <pthread.h>


namespace std {

template <typename __KEY_T, typename __VALUE_T>
class SimpleThreadSafeMap {
 private:
    map<__KEY_T, pair<pthread_spinlock_t, __VALUE_T>> base_map_;
    pthread_spinlock_t comm_lock_;

 public:
    SimpleThreadSafeMap() {pthread_spin_init(&comm_lock_, 0);}
    ~SimpleThreadSafeMap() {}

    // this can only guarantee that
    __VALUE_T Get(__KEY_T key) {  // ignore key error, just do it
        __VALUE_T ret_val;
        pthread_spin_lock(&comm_lock_);
        ret_val = base_map_[key].second;
        pthread_spin_unlock(&comm_lock_);
        return ret_val;
    }

    void Set(__KEY_T key, const __VALUE_T& value) {
        pthread_spin_lock(&comm_lock_);

        base_map_[key] = make_pair(pthread_spinlock_t(), value);
        // init lock
        pthread_spin_init(&base_map_[key].first, 0);

        pthread_spin_unlock(&comm_lock_);
    }

    int Count(__KEY_T key) {
        int ret_val;
        pthread_spin_lock(&comm_lock_);
        ret_val = base_map_.count(key);
        pthread_spin_unlock(&comm_lock_);
        return ret_val;
    }

    __VALUE_T GetAndLock(__KEY_T key) {
        // get the value in a specific slot, and lock it
        __VALUE_T ret_val;
        pthread_spin_lock(&comm_lock_);  // this should be locked first in case of dead lock
        pthread_spin_lock(&base_map_[key].first);
        ret_val = base_map_[key].second;
        pthread_spin_unlock(&comm_lock_);

        return ret_val;
    }

    void SetAndUnlock(__KEY_T key, const __VALUE_T& value) {
        // assuming that the key exists.
        pthread_spin_lock(&comm_lock_);

        base_map_[key].second = value;

        pthread_spin_unlock(&base_map_[key].first);
        pthread_spin_unlock(&comm_lock_);
    }
};

};  // namespace std



