/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef EXPERT_LRU_CACHE_HPP_
#define EXPERT_LRU_CACHE_HPP_

#include <string>
#include <vector>
#include <type_traits>
#include <mutex>

#include "core/message.hpp"
#include "utils/tool.hpp"
#include "storage/data_store.hpp"

#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include "tbb/concurrent_lru_cache.h"

class ExpertLRUCache {
 public:
    bool get_label_from_cache(uint64_t id, label_t & label) {
        value_t val;
        if (!lookup(id, val)) {
            return false;
        }

        label = Tool::value_t2int(val);
        return true;
    }

    bool get_property_from_cache(uint64_t id, value_t & val) {
        if (!lookup(id, val)) {
            return false;
        }
        return true;
    }

    void insert_properties(uint64_t id, value_t & val) {
        insert(id, val);
    }

    void insert_label(uint64_t id, label_t & label) {
        value_t val;
        Tool::str2int(to_string(label), val);

        insert(id, val);
    }

    void print_cache() {
        cout << "[Cache]" << vl.size() << " / " << NUM_CACHE << endl;
    }

 private:
    // uint64_t : eid_t.value(), vid_t.value(), vpid_t.value(), epid_t.value()
    typedef pair<uint64_t, value_t> cacheItem;
    list<cacheItem> vl;
    unordered_map<uint64_t, list<cacheItem>::iterator> refer_map;

    static const int NUM_CACHE = 1000000;
    mutex cache_mtx;

    bool lookup(uint64_t id, value_t & val) {
        lock_guard<mutex> lock(cache_mtx);
        if (refer_map.find(id) == refer_map.end()) {  // Not found in cache
            return false;
        } else {
            cacheItem p = *refer_map[id];
            val = p.second;
            // move this element to begin
            vl.splice(vl.begin(), vl, refer_map[id]);
        }

        return true;
    }

    void insert(uint64_t id, value_t & val) {
        lock_guard<mutex> lock(cache_mtx);
        // size() time complexity
        if (vl.size() == NUM_CACHE) {  // Cache is full
            // erase last element
            cacheItem last = vl.back();
            refer_map.erase(last.first);
            vl.pop_back();
        }

        // update
        vl.emplace_front(id, val);
        refer_map[id] = vl.begin();
    }
};

#endif /* EXPERT_LRU_CACHE_HPP_ */
