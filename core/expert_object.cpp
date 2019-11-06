/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#include "core/expert_object.hpp"

void Expert_Object::AddParam(int key) {
    value_t v;
    Tool::str2int(to_string(key), v);
    params.push_back(move(v));
}

bool Expert_Object::AddParam(string s) {
    value_t v;
    string s_value = Tool::trim(s, " ");  // delete all spaces
    if (!Tool::str2value_t(s_value, v)) {
        return false;
    }
    params.push_back(move(v));
    return true;
}

bool Expert_Object::IsBarrier() const {
    switch (expert_type) {
    case EXPERT_T::AGGREGATE:
    case EXPERT_T::COUNT:
    case EXPERT_T::CAP:
    case EXPERT_T::GROUP:
    case EXPERT_T::DEDUP:
    case EXPERT_T::MATH:
    case EXPERT_T::ORDER:
    case EXPERT_T::RANGE:
    case EXPERT_T::COIN:
    case EXPERT_T::END:
        return true;
    default:
        return false;
    }
}

string Expert_Object::DebugString() const {
    string s = "Expert type: " + string(ExpertType[static_cast<int>(expert_type)]);
    s += ", params: ";
    for (auto v : params) {
        s += Tool::DebugString(v) + " ";
    }
    s += ", Next Expert: " + to_string(next_expert);
    s += ", Remote: ";
    s += send_remote ? "Yes" : "No";
    return s;
}

ibinstream& operator<<(ibinstream& m, const Expert_Object& obj) {
    m << obj.expert_type;
    m << obj.next_expert;
    m << obj.send_remote;
    m << obj.params;
    return m;
}

obinstream& operator>>(obinstream& m, Expert_Object& obj) {
    m >> obj.expert_type;
    m >> obj.next_expert;
    m >> obj.send_remote;
    m >> obj.params;
    return m;
}
