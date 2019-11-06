/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "tid_mapper.hpp"

using namespace std;

TidMapper::TidMapper() {
    pthread_spin_init(&lock_, 0);
}

void TidMapper::Register(int tid) {
    pthread_spin_lock(&lock_);

    unique_tid_map_.insert(make_pair(pthread_self(), unique_tid_map_.size()));
    manual_tid_map_.insert(make_pair(pthread_self(), tid));

    pthread_spin_unlock(&lock_);
}

int TidMapper::GetTid() {
    assert(manual_tid_map_.count(pthread_self()) != 0);
    return manual_tid_map_[pthread_self()];
}

int TidMapper::GetTidUnique() {
    assert(unique_tid_map_.count(pthread_self()) != 0);
    return unique_tid_map_[pthread_self()];
}
