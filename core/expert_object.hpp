/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include <string>
#include <vector>
#include "utils/tool.hpp"
#include "base/type.hpp"

using namespace std;

class Expert_Object {
 public:
    // type
    EXPERT_T expert_type;

    // parameters
    vector<value_t> params;

    // index of next expert
    int next_expert;

    // flag for sending data to remote nodes
    bool send_remote;

    Expert_Object() : next_expert(-1), send_remote(false) {}
    Expert_Object(EXPERT_T type) : expert_type(type), next_expert(-1), send_remote(false) {}

    void AddParam(int key);
    bool AddParam(string s);
    bool IsBarrier() const;

    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Expert_Object& msg);

obinstream& operator>>(obinstream& m, Expert_Object& msg);
