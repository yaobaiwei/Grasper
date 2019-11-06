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

class TidMapper {
 private:
    TidMapper(const TidMapper&);
    TidMapper& operator=(const TidMapper&);
    ~TidMapper() {}

    map<pthread_t, int> manual_tid_map_;
    map<pthread_t, int> unique_tid_map_;

    pthread_spinlock_t lock_;

 public:
    static TidMapper* GetInstance() {
        static TidMapper thread_mapper_single_instance;
        return &thread_mapper_single_instance;
    }

    void Register(int tid);
    int GetTid();
    int GetTidUnique();

 private:
    TidMapper();
};

};  // namespace std
