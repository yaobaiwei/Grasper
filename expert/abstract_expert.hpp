/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#ifndef ABSTRACT_EXPERT_HPP_
#define ABSTRACT_EXPERT_HPP_

#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "base/core_affinity.hpp"
#include "core/message.hpp"
#include "storage/data_store.hpp"
#include "utils/tid_mapper.hpp"

class AbstractExpert {
 public:
    AbstractExpert(int id, DataStore* data_store, CoreAffinity* core_affinity):id_(id), data_store_(data_store), core_affinity_(core_affinity) {}

    virtual ~AbstractExpert() {}

    const int GetExpertId() {return id_;}

    virtual void process(const vector<Expert_Object> & experts, Message & msg) = 0;

 protected:
    // Data Store
    DataStore* data_store_;

    // Core affinity
    CoreAffinity* core_affinity_;

 private:
    // Expert ID
    int id_;
};

#endif /* ABSTRACT_EXPERT_HPP_ */
