/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Changji Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef TYPE_HPP_
#define TYPE_HPP_

#include <stdint.h>
#include <unordered_map>
#include <string.h>
#include <sstream>
#include <ext/hash_map>
#include <ext/hash_set>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>

#include "utils/mymath.hpp"
#include "base/serialization.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

using namespace std;

// 64-bit internal pointer (size < 256M and off < 64GB)
enum { NBITS_SIZE = 28 };
enum { NBITS_PTR = 36 };

static uint64_t _32LFLAG = 0xFFFFFFFF;
static uint64_t _28LFLAG = 0xFFFFFFF;
static uint64_t _12LFLAG = 0xFFF;

struct ptr_t {
    uint64_t size: NBITS_SIZE;
    uint64_t off: NBITS_PTR;

    ptr_t(): size(0), off(0) { }

    ptr_t(uint64_t s, uint64_t o): size(s), off(o) {
        assert((size == s) && (off == o));
    }

    bool operator == (const ptr_t &ptr) {
        if ((size == ptr.size) && (off == ptr.off))
            return true;
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += off;
        r <<= NBITS_SIZE;
        r += size;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const ptr_t& p);

obinstream& operator>>(obinstream& m, ptr_t& p);

uint64_t ptr_t2uint(const ptr_t & p);

void uint2ptr_t(uint64_t v, ptr_t & p);

bool operator == (const ptr_t &p1, const ptr_t &p2);

struct ikey_t {
    uint64_t pid;
    ptr_t ptr;

    ikey_t() {
        pid = 0;
    }

    ikey_t(uint64_t _pid, ptr_t _ptr) {
        pid = _pid;
        ptr = _ptr;
    }

    bool operator == (const ikey_t &key) {
        if (pid == key.pid)
            return true;
        return false;
    }

    bool is_empty() { return pid == 0; }
};

ibinstream& operator<<(ibinstream& m, const ikey_t& p);

obinstream& operator>>(obinstream& m, ikey_t& p);

bool operator == (const ikey_t &p1, const ikey_t &p2);

enum {VID_BITS = 26};  // <32; the total # of vertices should no be more than 2^26
enum {EID_BITS = (VID_BITS * 2)};  // eid = v1_id | v2_id (52 bits)
enum {PID_BITS = (64 - EID_BITS)};  // 12, the total # of property should no be more than 2^PID_BITS

// vid: 32bits 0000|00-vid
struct vid_t {
    uint32_t vid: VID_BITS;

    vid_t(): vid(0) {}
    vid_t(int _vid): vid(_vid) {}

    bool operator == (const vid_t & vid1) {
        if ((vid == vid1.vid))
            return true;
        return false;
    }

    vid_t& operator =(int i) {
        this->vid = i;
        return *this;
    }

    uint32_t value() {
        return (uint32_t)vid;
    }

    uint64_t hash() {
        uint64_t r = value();
        return mymath::hash_u64(r);  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const vid_t& v);

obinstream& operator>>(obinstream& m, vid_t& v);

uint32_t vid_t2uint(const vid_t & vid);

void uint2vid_t(uint32_t v, vid_t & vid);

bool operator == (const vid_t &p1, const vid_t &p2);

namespace __gnu_cxx {
template <>
struct hash<vid_t> {
    size_t operator()(const vid_t& vid) const {
        int key = (int)vid.vid;
        size_t seed = 0;
        mymath::hash_combine(seed, key);
        return seed;
    }
};
}  // namespace __gnu_cxx

// vid: 64bits 0000|0000|0000|in_v|out_v
struct eid_t {
    uint64_t in_v : VID_BITS;
    uint64_t out_v : VID_BITS;

    eid_t(): in_v(0), out_v(0) {}

    eid_t(int _in_v, int _out_v): in_v(_in_v), out_v(_out_v) {
        assert((in_v == _in_v) && (out_v == _out_v) );  // no key truncate
    }

    bool operator == (const eid_t &eid) {
        if ((in_v == eid.in_v) && (out_v == eid.out_v))
            return true;
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += in_v;
        r <<= VID_BITS;
        r += out_v;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const eid_t& e);

obinstream& operator>>(obinstream& m, eid_t& e);

uint64_t eid_t2uint(const eid_t & eid);

void uint2eid_t(uint64_t v, eid_t & eid);

bool operator == (const eid_t &p1, const eid_t &p2);

namespace __gnu_cxx {
template <>
struct hash<eid_t> {
    size_t operator()(const eid_t& eid) const {
        int k1 = (int)eid.in_v;
        int k2 = (int)eid.out_v;
        size_t seed = 0;
        mymath::hash_combine(seed, k1);
        mymath::hash_combine(seed, k2);
        return seed;
    }
};
}  // namespace __gnu_cxx

// vpid: 64bits  vid|0x26|pid
struct vpid_t {
    uint64_t vid : VID_BITS;
    uint64_t pid : PID_BITS;

    vpid_t(): vid(0), pid(0) { }

    vpid_t(int _vid, int _pid): vid(_vid), pid(_pid) {
        assert((vid == _vid) && (pid == _pid) );  // no key truncate
    }

    vpid_t(vid_t _vid, int _pid): pid(_pid) {
        vid = _vid.vid;
        assert((vid == _vid.vid) && (pid == _pid) );  // no key truncate
    }

    bool operator == (const vpid_t &vpid) {
        if ((vid == vpid.vid) && (pid == vpid.pid))
            return true;
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += vid;
        r <<= VID_BITS;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const vpid_t& vp);

obinstream& operator>>(obinstream& m, vpid_t& vp);

uint64_t vpid_t2uint(const vpid_t & vp);

void uint2vpid_t(uint64_t v, vpid_t & vp);

bool operator == (const vpid_t &p1, const vpid_t &p2);

// vpid: 64bits  v_in|v_out|pid
struct epid_t {
    uint64_t in_vid : VID_BITS;
    uint64_t out_vid : VID_BITS;
    uint64_t pid : PID_BITS;

    epid_t(): in_vid(0), out_vid(0), pid(0) {}

    epid_t(eid_t _eid, int _pid): pid(_pid) {
        in_vid = _eid.in_v;
        out_vid = _eid.out_v;
        assert((in_vid == _eid.in_v) && (out_vid == _eid.out_v) && (pid == _pid) );  // no key truncate
    }

    epid_t(int _in_v, int _out_v, int _pid): in_vid(_in_v), out_vid(_out_v), pid(_pid) {
        assert((in_vid == _in_v) && (out_vid == _out_v) && (pid == _pid) );  // no key truncate
    }

    bool operator == (const epid_t &epid) {
        if ((in_vid == epid.in_vid) && (out_vid == epid.out_vid) && (pid == epid.pid))
            return true;
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += in_vid;
        r <<= VID_BITS;
        r += out_vid;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const epid_t& ep);

obinstream& operator>>(obinstream& m, epid_t& ep);

uint64_t epid_t2uint(const epid_t & ep);

void uint2epid_t(uint64_t v, epid_t & ep);

bool operator == (const epid_t &p1, const epid_t &p2);

typedef uint16_t label_t;

// type
// 1->int, 2->double, 3->char, 4->string, 5->uint64_t
struct value_t {
    uint8_t type;
    vector<char> content;
    string DebugString() const;
};

struct ValueTHash {
    // To limit the upper bound of cost for hashing
    constexpr static int max_hash_len = 8;

    size_t operator() (const value_t& val) const{
        uint64_t hash_tmp = mymath::hash_u64(val.type) + mymath::hash_u64(val.content.size());

        int hash_loop_len = (max_hash_len > val.content.size()) ? val.content.size() : max_hash_len;

        // Only use the former elements to compute the hash value
        for (int i = 0; i < hash_loop_len; i++)
            hash_tmp = mymath::hash_u64(hash_tmp + val.content[i]);

        return hash_tmp;
    }
};

ibinstream& operator<<(ibinstream& m, const value_t& v);

obinstream& operator>>(obinstream& m, value_t& v);

struct kv_pair {
    uint32_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const kv_pair& p);

obinstream& operator>>(obinstream& m, kv_pair& p);

struct vp_list {
    vid_t vid;
    vector<label_t> pkeys;
};

ibinstream& operator<<(ibinstream& m, const vp_list& vp);

obinstream& operator>>(obinstream& m, vp_list& vp);

// the return value from kv_store
// type
// 1->int, 2->double, 3->char, 4->string
struct elem_t {
    elem_t() : sz(0), type(0), content(NULL) {}
    uint8_t type;
    uint32_t sz;
    char * content;
};

ibinstream& operator<<(ibinstream& m, const elem_t& e);

obinstream& operator>>(obinstream& m, elem_t& e);

struct string_index {
    unordered_map<string, label_t> str2el;  // map to edge_label
    unordered_map<label_t, string> el2str;
    unordered_map<string, label_t> str2epk;  // map to edge's property key
    unordered_map<label_t, string> epk2str;
    unordered_map<string, uint8_t> str2eptype;
    unordered_map<string, label_t> str2vl;  // map to vtx_label
    unordered_map<label_t, string> vl2str;
    unordered_map<string, label_t> str2vpk;  // map to vtx's property key
    unordered_map<label_t, string> vpk2str;
    unordered_map<string, uint8_t> str2vptype;
};

enum Index_T { E_LABEL, E_PROPERTY, V_LABEL, V_PROPERTY };

// Spawn: spawn a new expert
// Feed: "proxy" feed expert a input
// Reply: expert returns the intermidiate result to expert
enum class MSG_T : char { INIT, SPAWN, FEED, REPLY, BARRIER, BRANCH, EXIT };
static const char *MsgType[] = {"init", "spawn", "feed", "reply", "barrier", "branch", "exit"};

ibinstream& operator<<(ibinstream& m, const MSG_T& type);

obinstream& operator>>(obinstream& m, MSG_T& type);

enum class EXPERT_T : char {
    INIT, AGGREGATE, AS, BRANCH, BRANCHFILTER, CAP, CONFIG, COUNT, DEDUP, GROUP, HAS, HASLABEL, INDEX,
    IS, KEY, LABEL, MATH, ORDER, PROPERTY, RANGE, SELECT, TRAVERSAL, VALUES, WHERE, COIN, REPEAT, UNTIL, END
};

static const char *ExpertType[] = { "INIT", "AGGREGATE", "AS", "BRANCH", "BRANCHFILTER", "CAP", "CONFIG", "COUNT", "DEDUP", "GROUP", "HAS",
"HASLABEL", "INDEX", "IS", "KEY", "LABEL", "MATH", "ORDER", "PROPERTY", "RANGE", "SELECT", "TRAVERSAL", "VALUES", "WHERE" , "COIN", "REPEAT", "UNTIL", "END"};

ibinstream& operator<<(ibinstream& m, const EXPERT_T& type);

obinstream& operator>>(obinstream& m, EXPERT_T& type);

// Enums for experts
enum Filter_T{ AND, OR, NOT };
enum Math_T { SUM, MAX, MIN, MEAN };
enum Element_T{ VERTEX, EDGE };
enum Direction_T{ IN, OUT, BOTH };
enum Order_T {INCR, DECR};
enum Predicate_T{ ANY, NONE, EQ, NEQ, LT, LTE, GT, GTE, INSIDE, OUTSIDE, BETWEEN, WITHIN, WITHOUT };

struct qid_t {
    uint32_t nid;
    uint32_t qr;

    qid_t(): nid(0), qr(0) {}

    qid_t(uint32_t _nid, uint32_t _qr): nid(_nid), qr(_qr) {}

    bool operator == (const qid_t & qid) {
        if ((nid == qid.nid) && (qr == qid.qr))
            return true;
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += qr;
        r <<= 32;
        r += nid;
        return r;
    }
};

void uint2qid_t(uint64_t v, qid_t & qid);

// msg key for branch and barrier experts
struct mkey_t {
    uint64_t qid;    // qid
    uint64_t mid;    // msg id of branch parent
    int index;       // index of branch
    mkey_t() : qid(0), index(0), mid(0) {}
    mkey_t(uint64_t qid_, uint64_t mid_, int index_) : qid(qid_), index(index_), mid(mid_) {}

    bool operator==(const mkey_t& key) const {
        if ((qid == key.qid) && (mid == key.mid) && (index == key.index)) {
            return true;
        }
        return false;
    }

    bool operator<(const mkey_t& key) const {
        if (qid < key.qid) {
            return true;
        } else if (qid == key.qid) {
            if (mid < key.mid) {
                return true;
            } else if (mid == key.mid) {
                return index < key.index;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
};

// Provide hash function for tbb hash_map
struct MkeyHashCompare {
    static size_t hash(const mkey_t& x) {
        size_t value = mymath::hash_u128_to_u64(x.qid, x.mid);
        mymath::hash_combine(value, x.index);
        return value;
    }

    // True if strings are equal
    static bool equal(const mkey_t& x, const mkey_t& y) {
        return x == y;
    }
};

// Aggregate data Key
//  qr | nid | label_key
//  32 |  16 |        16
enum { NBITS_QR = 32 };
enum { NBITS_NID = 16 };
enum { NBITS_SEK = 16 };
struct agg_t {
    uint64_t qr : NBITS_QR;
    uint64_t nid : NBITS_NID;
    uint64_t label_key : NBITS_SEK;

    agg_t() : qr(0), nid(0), label_key(0) {}
    agg_t(qid_t _qid, int _label_key) : label_key(_label_key) {
        qr = _qid.qr;
        nid = _qid.nid;
    }

    agg_t(uint64_t qid_value, int _label_key) : label_key(_label_key) {
        qid_t _qid;
        uint2qid_t(qid_value, _qid);

        qr = _qid.qr;
        nid = _qid.nid;
    }

    bool operator==(const agg_t & key) const {
        if ((qr == key.qr) && (nid == key.nid) && (label_key == key.label_key)) {
            return true;
        }
        return false;
    }

    uint64_t value() {
        uint64_t r = 0;
        r += qr;
        r <<= NBITS_QR;
        r += nid;
        r <<= NBITS_NID;
        r += label_key;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());
    }
};

namespace std {
template <>
struct hash<agg_t> {
    size_t operator()(const agg_t& key) const {
        int k1 = (int)key.qr;
        int k2 = (int)key.nid;
        int k3 = (int)key.label_key;
        size_t seed = 0;
        mymath::hash_combine(seed, k1);
        mymath::hash_combine(seed, k2);
        mymath::hash_combine(seed, k3);
        return seed;
    }
};
}  // namespace std

// Thread_division for experts
// Cache_Sequential : LabelExpert, HasLabelExpert, PropertiesExpert, ValuesExpert, HasExpert, KeyExpert [1/4 #threads]
// Cache_Barrier : GroupExpert, OrderExpert [1/6 #threads]
// TRAVERSAL : TraversalExpert [1/12 #threads]
// Normal_Barrier : CountExpert, AggregateExpert, CapExpert, DedupExpert, MathExpert [1/6 #threads]
// Normal_Branch : RangeExpert, CoinExpert, BranchFilterExpert, BranchExpert [1/6 #threads]
// Normal_Sequential : AsExpert, SelectExpert, WhereExpert, IsExpert [1/6 #threads]
enum ExpertDivisionType { CACHE_SEQ, CACHE_BARR, TRAVERSAL, NORMAL_BARR, NORMAL_BRANCH, NORMAL_SEQ };
enum ResidentThread_T { MAIN, RECVREQ, SENDQUERY, MONITOR };

static const int NUM_THREAD_DIVISION = 6;
static const int NUM_RESIDENT_THREAD = 4;

#endif /* TYPE_HPP_ */
