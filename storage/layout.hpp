/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <sstream>

#include "base/type.hpp"
#include "base/serialization.hpp"

using namespace std;

struct Vertex {
    vid_t id;
    // label_t label;
    vector<vid_t> in_nbs;
    vector<vid_t> out_nbs;
    vector<label_t> vp_list;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Vertex& v);

obinstream& operator>>(obinstream& m, Vertex& v);

struct Edge {
    // vid_t v_1;
    // vid_t v_2;
    eid_t id;
    // label_t label;
    vector<label_t> ep_list;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Edge& e);

obinstream& operator>>(obinstream& m, Edge& e);

struct V_KVpair {
    vpid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair);

obinstream& operator>>(obinstream& m, V_KVpair& pair);

struct VProperty {
    vid_t id;
    vector<V_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const VProperty& vp);

obinstream& operator>>(obinstream& m, VProperty& vp);

struct E_KVpair {
    epid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair);

obinstream& operator>>(obinstream& m, E_KVpair& pair);

struct EProperty {
    // vid_t v_1;
    // vid_t v_2;
    eid_t id;
    vector<E_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const EProperty& ep);

obinstream& operator>>(obinstream& m, EProperty& ep);
