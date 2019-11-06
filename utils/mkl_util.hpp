/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>
#include <string>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>
#include <mkl.h>

#include "base/simple_thread_safe_map.hpp"
#include "utils/tid_mapper.hpp"

namespace std {
// one instance for one tid
// before using this, the thread must have registed in TidMapper

class MKLUtil {
 private:
    MKLUtil(const MKLUtil&);
    MKLUtil& operator=(const MKLUtil&);
    ~MKLUtil() {
        printf("MKLUtil::~MKLUtil()\n");
    }
    MKLUtil(int tid);

    // get instance from the map with key tid
    static MKLUtil* GetInstanceActual(int tid) {
        static SimpleThreadSafeMap<int, MKLUtil*> instance_map;

        if (instance_map.Count(tid) == 0) {
            MKLUtil* p = new MKLUtil(tid);
            instance_map.Set(tid, p);
        }

        return instance_map.Get(tid);
    }

    int tid_;

    // RNG related
    VSLStreamStatePtr rng_stream_;

 public:
    /* Currently, assuming that each tid has only one instance of MKLUtil.
     * Usage:
     * Register tid for one thread to obtain thread-bound random number.
     */
    static MKLUtil* GetInstance() {
        auto tid = TidMapper::GetInstance()->GetTidUnique();

        return GetInstanceActual(tid);
    }

    // Interface for testing
    void Test();

    bool UniformRNGI4(int* dst, int len, int min, int max);
    bool UniformRNGF4(float* dst, int len, float min, float max);
    bool UniformRNGF8(double* dst, int len, double min, double max);
};

}  // namespace std



