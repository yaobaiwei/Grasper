/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#ifndef RESULT_COLLECTOR_HPP_
#define RESULT_COLLECTOR_HPP_

#include <ext/hash_map>
#include <algorithm>
#include <iostream>
#include <list>
#include <vector>
#include <mutex>
#include <queue>
#include <string>

#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"

using __gnu_cxx::hash_map;
using namespace std;

struct reply {
    string hostname;
    uint64_t qid;
    vector<value_t> results;
};

class Result_Collector {
 public:
    void Register(uint64_t qid, string hostname) {
        lock_guard<mutex> lck(m_mutex_);
        reply_list_.push_front(move(hostname));
        mp_[qid] = reply_list_.begin();
    }

    void InsertResult(uint64_t qid, vector<value_t> & data) {
        m_mutex_.lock();
        indexItr it = mp_.find(qid);

        if (it == mp_.end()) {
            cout << "ERROR: Impossible branch in Result_Collector!\n";
            exit(-1);
        }

        itemItr re_pos = it->second;
        reply re;
        re.hostname = move(*re_pos);
        re.results = move(data);
        re.qid = qid;
        reply_list_.erase(re_pos);
        mp_.erase(it);

        m_mutex_.unlock();
        reply_queue_.Push(move(re));
    }

    void Pop(reply & result) {
        reply_queue_.WaitAndPop(result);
    }

 private:
    // hostname, result;
    typedef string item;
    typedef list<item>::iterator itemItr;
    typedef hash_map<uint64_t, itemItr> index;
    typedef index::iterator indexItr;

    mutex m_mutex_;
    list<item> reply_list_;
    ThreadSafeQueue<reply> reply_queue_;
    index mp_;
};

#endif /* RESULT_COLLECTOR_HPP_ */
