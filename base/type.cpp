/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "base/type.hpp"

ibinstream& operator<<(ibinstream& m, const ptr_t& p) {
    uint64_t v = ptr_t2uint(p);
    m << v;
    return m;
}

obinstream& operator>>(obinstream& m, ptr_t& p) {
    uint64_t v;
    m >> v;
    uint2ptr_t(v, p);
    return m;
}

uint64_t ptr_t2uint(const ptr_t & p) {
    uint64_t r = 0;
    r += p.off;
    r <<= NBITS_SIZE;
    r += p.size;
    return r;
}

void uint2ptr_t(uint64_t v, ptr_t & p) {
    p.size = (v & _28LFLAG);
    p.off = (v >> NBITS_SIZE);
}

bool operator == (const ptr_t &p1, const ptr_t &p2) {
    if ((p1.off == p2.off) && (p1.size == p2.size))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const ikey_t& p) {
    m << p.pid;
    m << p.ptr;
    return m;
}

obinstream& operator>>(obinstream& m, ikey_t& p) {
    m >> p.pid;
    m >> p.ptr;
    return m;
}

bool operator == (const ikey_t &p1, const ikey_t &p2) {
    if ((p1.pid == p2.pid) && (p1.ptr == p2.ptr))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const vid_t& v) {
    uint32_t value = vid_t2uint(v);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, vid_t& v) {
    uint32_t value;
    m >> value;
    uint2vid_t(value, v);
    return m;
}

uint32_t vid_t2uint(const vid_t & vid) {
    uint32_t r = vid.vid;
    return r;
}

void uint2vid_t(uint32_t v, vid_t & vid) {
    vid.vid = (v & _28LFLAG);
}

bool operator == (const vid_t &p1, const vid_t &p2) {
    if (p1.vid == p2.vid)
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const eid_t& e) {
    uint64_t value = eid_t2uint(e);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, eid_t& e) {
    uint64_t value;
    m >> value;
    uint2eid_t(value, e);
    return m;
}

uint64_t eid_t2uint(const eid_t & eid) {
    uint64_t r = 0;
    r += eid.in_v;
    r <<= VID_BITS;
    r += eid.out_v;
    return r;
}

void uint2eid_t(uint64_t v, eid_t & eid) {
    eid.out_v = (v & _28LFLAG);
    eid.in_v = ((v >> VID_BITS) & _28LFLAG);
}

bool operator == (const eid_t &p1, const eid_t &p2) {
    if ((p1.in_v == p2.in_v) && (p1.out_v == p2.out_v))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const vpid_t& vp) {
    uint64_t value = vpid_t2uint(vp);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, vpid_t& vp) {
    uint64_t value;
    m >> value;
    uint2vpid_t(value, vp);
    return m;
}

uint64_t vpid_t2uint(const vpid_t & vp) {
    uint64_t r = 0;
    r += vp.vid;
    r <<= VID_BITS;
    r <<= PID_BITS;
    r += vp.pid;
    return r;
}

void uint2vpid_t(uint64_t v, vpid_t & vp) {
    vp.pid = (v & _12LFLAG);
    v >>= PID_BITS;
    v >>= VID_BITS;
    vp.vid = (v & _28LFLAG);
}

bool operator == (const vpid_t &p1, const vpid_t &p2) {
    if ((p1.pid == p2.pid) && (p1.vid == p2.vid))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const epid_t& ep) {
    uint64_t value = epid_t2uint(ep);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, epid_t& ep) {
    uint64_t value;
    m >> value;
    uint2epid_t(value, ep);
    return m;
}

uint64_t epid_t2uint(const epid_t & ep) {
    uint64_t r = 0;
    r += ep.in_vid;
    r <<= VID_BITS;
    r += ep.out_vid;
    r <<= PID_BITS;
    r += ep.pid;
    return r;
}

void uint2epid_t(uint64_t v, epid_t & ep) {
    ep.pid = (v & _12LFLAG);
    v >>= PID_BITS;
    ep.out_vid = (v & _28LFLAG);
    v >>= VID_BITS;
    ep.in_vid = (v & _28LFLAG);
}

bool operator == (const epid_t &p1, const epid_t &p2) {
    if ((p1.in_vid == p2.in_vid) && (p1.out_vid == p2.out_vid) && (p1.pid == p2.pid))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const value_t& v) {
    m << v.content;
    m << v.type;
    return m;
}

obinstream& operator>>(obinstream& m, value_t& v) {
    m >> v.content;
    m >> v.type;
    return m;
}

string kv_pair::DebugString() const {
    stringstream ss;
    ss << "kv_pair: { key = " << key << ", value.type = " << (int)value.type << " }"<< endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const kv_pair& p) {
    m << p.key;
    m << p.value;
    return m;
}

obinstream& operator>>(obinstream& m, kv_pair& p) {
    m >> p.key;
    m >> p.value;
    return m;
}

ibinstream& operator<<(ibinstream& m, const vp_list& vp) {
    m << vp.vid;
    m << vp.pkeys;
    return m;
}

obinstream& operator>>(obinstream& m, vp_list& vp) {
    m >> vp.vid;
    m >> vp.pkeys;
    return m;
}

ibinstream& operator<<(ibinstream& m, const elem_t& e) {
    vector<char> vec(e.content, e.content+e.sz);
    m << e.type;
    m << vec;
    return m;
}

obinstream& operator>>(obinstream& m, elem_t& e) {
    vector<char> vec;
    m >> e.type;
    m >> vec;
    e.content = new char[vec.size()];
    e.sz = vec.size();
    return m;
}

ibinstream& operator<<(ibinstream& m, const MSG_T& type) {
    int t = static_cast<int>(type);
    m << t;
    return m;
}

obinstream& operator>>(obinstream& m, MSG_T& type) {
    int tmp;
    m >> tmp;
    type = static_cast<MSG_T>(tmp);
    return m;
}

ibinstream& operator<<(ibinstream& m, const EXPERT_T& type) {
    int t = static_cast<int>(type);
    m << t;
    return m;
}

obinstream& operator>>(obinstream& m, EXPERT_T& type) {
    int tmp;
    m >> tmp;
    type = static_cast<EXPERT_T>(tmp);
    return m;
}

void uint2qid_t(uint64_t v, qid_t & qid) {
    qid.nid = (v & _32LFLAG);
    qid.qr = ((v >> 32) & _32LFLAG);
}
