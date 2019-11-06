/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#include <unordered_map>

#include "base/type.hpp"
#include "base/predicate.hpp"
#include "core/message.hpp"
#include "utils/config.hpp"

#pragma once

class IndexStore {
 public:
    const static double ratio = 0.2;

    IndexStore() {
        config_ = Config::GetInstance();
    }

    bool IsIndexEnabled(Element_T type, int pid, PredicateValue* pred = NULL, uint64_t* count = NULL) {
        if (config_->global_enable_indexing) {
            unordered_map<int, index_>* m;
            if (type == Element_T::VERTEX) {
                m = &vtx_index;
            } else {
                m = &edge_index;
            }

            thread_mutex_.lock();
            // check property key
            auto itr = m->find(pid);
            if (itr == m->end()) {
                thread_mutex_.unlock();
                return false;
            }
            thread_mutex_.unlock();

            index_ &idx = itr->second;
            if (idx.isEnabled) {
                if (pred != NULL) {
                    uint64_t threshold = idx.total * ratio;
                    *count = get_count_by_predicate(type, pid, *pred);
                    if (*count >= threshold) {
                        return false;
                    } else {
                        return true;
                    }
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }
        return false;
    }

    // type:             VERTEX / EDGE
    // property_key:     key
    // index_map:        alreay constructed index map
    // no_key_vec:       vector of elements that have no provided key
    bool SetIndexMap(Element_T type, int pid, map<value_t, vector<value_t>>& index_map, vector<value_t>& no_key_vec) {
        if (config_->global_enable_indexing) {
            unordered_map<int, index_>* m;
            if (type == Element_T::VERTEX) {
                m = &vtx_index;
            } else {
                m = &edge_index;
            }

            index_ &idx = (*m)[pid];

            uint64_t sum = 0;
            // sort each vector for better searching performance
            for (auto& item : index_map) {
                set<value_t> temp(std::make_move_iterator(item.second.begin()), std::make_move_iterator(item.second.end()));
                uint64_t count = temp.size();
                sum += count;
                auto itr = idx.index_map.insert(idx.index_map.end(), {move(item.first), move(temp)});
                idx.count_map[item.first] = count;
                idx.values.push_back(&(itr->first));
            }

            idx.no_key = set<value_t>(make_move_iterator(no_key_vec.begin()), make_move_iterator(no_key_vec.end()));
            sum += idx.no_key.size();
            idx.total = sum;
            return true;
        }
        return false;
    }

    bool SetIndexMapEnable(Element_T type, int pid, bool inverse = false) {
        if (config_->global_enable_indexing) {
            unordered_map<int, index_>* m;
            if (type == Element_T::VERTEX) {
                m = &vtx_index;
            } else {
                m = &edge_index;
            }

            index_ &idx = (*m)[pid];

            thread_mutex_.lock();
            if (inverse) {
                idx.isEnabled = !idx.isEnabled;
            } else {
                idx.isEnabled = true;
            }
            thread_mutex_.unlock();
            return idx.isEnabled;
        }
        return false;
    }

    void GetElements(Element_T type, vector<pair<int, PredicateValue>>& pred_chain, vector<value_t>& data) {
        bool is_first = true;
        bool need_sort = pred_chain.size() != 1;
        for (auto& pred_pair : pred_chain) {
            vector<value_t> vec;
            // get sorted vector of all elements satisfying current predicate
            get_elements_by_predicate(type, pred_pair.first, pred_pair.second, need_sort, vec);

            if (is_first) {
                data.swap(vec);
                is_first = false;
            } else {
                vector<value_t> temp;
                // do intersection with previous result
                // temp is sorted after intersection
                set_intersection(make_move_iterator(data.begin()), make_move_iterator(data.end()),
                                make_move_iterator(vec.begin()), make_move_iterator(vec.end()),
                                back_inserter(temp));
                data.swap(temp);
            }
        }
    }

    bool GetRandomValue(Element_T type, int pid, int rand_seed, string& value_str) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_index;
        } else {
            m = &edge_index;
        }

        auto itr = m->find(pid);
        if (itr == m->end()) {
            return false;
        }

        index_ &idx = itr->second;
        int size = idx.values.size();
        if (size == 0) {
            return false;
        }

        // get value string
        value_t v = *idx.values[rand_seed % size];
        value_str = Tool::DebugString(v);
        return true;
    }

 private:
    Config * config_;

    mutex thread_mutex_;

    struct index_ {
        bool isEnabled;
        uint64_t total;
        map<value_t, set<value_t>> index_map;
        set<value_t> no_key;
        map<value_t, uint64_t> count_map;
        vector<const value_t *> values;
    };

    unordered_map<int, index_> vtx_index;
    unordered_map<int, index_> edge_index;

    void get_elements_by_predicate(Element_T type, int pid, PredicateValue& pred, bool need_sort, vector<value_t>& vec) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_index;
        } else {
            m = &edge_index;
        }

        index_ &idx = (*m)[pid];

        auto &index_map = idx.index_map;
        map<value_t, set<value_t>>::iterator itr;
        int num_set = 0;

        switch (pred.pred_type) {
        case Predicate_T::ANY:
            // Search though whole index map
            for (auto& item : index_map) {
                vec.insert(vec.end(), item.second.begin(), item.second.end());
                num_set++;
            }
            break;
        case Predicate_T::NEQ:
        case Predicate_T::WITHOUT:
            // Search though whole index map to find matched values
            for (auto& item : index_map) {
                if (Evaluate(pred, &item.first)) {
                    vec.insert(vec.end(), item.second.begin(), item.second.end());
                    num_set++;
                }
            }
            break;
        case Predicate_T::EQ:
            // Get elements with single value
            itr = index_map.find(pred.values[0]);
            if (itr != index_map.end()) {
                vec.assign(itr->second.begin(), itr->second.end());
                num_set++;
            }
            break;
        case Predicate_T::WITHIN:
            // Get elements with given values
            for (auto& val : pred.values) {
                itr = index_map.find(val);
                if (itr != index_map.end()) {
                    vec.insert(vec.end(), itr->second.begin(), itr->second.end());
                    num_set++;
                }
            }
            break;
        case Predicate_T::NONE:
            // Get elements from no_key_store
            vec.assign(idx.no_key.begin(), idx.no_key.end());
            num_set++;
            break;

        case Predicate_T::OUTSIDE:
            // find less than
            pred.pred_type = Predicate_T::LT;
            build_range_elements(index_map, pred, vec, num_set);
            // find greater than
            pred.pred_type = Predicate_T::GT;
            swap(pred.values[0], pred.values[1]);
            build_range_elements(index_map, pred, vec, num_set);
            break;
        default:
            // LT, LTE, GT, GTE, BETWEEN, INSIDE
            build_range_elements(index_map, pred, vec, num_set);
            break;
        }

        if (need_sort && num_set > 1) {
            sort(vec.begin(), vec.end());
        }
    }

    uint64_t get_count_by_predicate(Element_T type, int pid, PredicateValue& pred) {
        unordered_map<int, index_>* m;
        if (type == Element_T::VERTEX) {
            m = &vtx_index;
        } else {
            m = &edge_index;
        }

        index_ &idx = (*m)[pid];
        auto& count_map = idx.count_map;
        uint64_t count = 0;

        map<value_t, uint64_t>::iterator itr;
        switch (pred.pred_type) {
        case Predicate_T::ANY:
            count = idx.total - idx.no_key.size();
            break;
        case Predicate_T::NEQ:
        case Predicate_T::WITHOUT:
            // Search though whole index map to find matched values
            for (auto& item : count_map) {
                if (Evaluate(pred, &item.first)) {
                    count += item.second;
                }
            }
            break;
        case Predicate_T::EQ:
            // Get elements with single value
            itr = count_map.find(pred.values[0]);
            if (itr != count_map.end()) {
                count += itr->second;
            }
            break;
        case Predicate_T::WITHIN:
            // Get elements with given values
            for (auto& val : pred.values) {
                itr = count_map.find(val);
                if (itr != count_map.end()) {
                    count += itr->second;
                }
            }
            break;
        case Predicate_T::NONE:
            // Get elements from no_key_store
            count += idx.no_key.size();
            break;

        case Predicate_T::OUTSIDE:
            // find less than
            pred.pred_type = Predicate_T::LT;
            build_range_count(count_map, pred, count);
            // find greater than
            pred.pred_type = Predicate_T::GT;
            swap(pred.values[0], pred.values[1]);
            build_range_count(count_map, pred, count);
            break;
        default:
            // LT, LTE, GT, GTE, BETWEEN, INSIDE
            build_range_count(count_map, pred, count);
            break;
        }
        return count;
    }

    void build_range_count(map<value_t, uint64_t>& m, PredicateValue& pred, uint64_t& count) {
        map<value_t, uint64_t>::iterator itr_low;
        map<value_t, uint64_t>::iterator itr_high;

        build_range(m, pred, itr_low, itr_high);

        for (auto itr = itr_low; itr != itr_high; itr ++) {
            count += itr->second;
        }
    }

    void build_range_elements(map<value_t, set<value_t>>& m, PredicateValue& pred, vector<value_t>& vec, int& num_set) {
        map<value_t, set<value_t>>::iterator itr_low;
        map<value_t, set<value_t>>::iterator itr_high;

        build_range(m, pred, itr_low, itr_high);

        for (auto itr = itr_low; itr != itr_high; itr ++) {
            vec.insert(vec.end(), itr->second.begin(), itr->second.end());
            num_set++;
        }
    }

    template <class Iterator, class T>
    void build_range(map<value_t, T>& m, PredicateValue& pred, Iterator& low, Iterator& high) {
        low = m.begin();
        high = m.end();

        // get lower bound
        switch (pred.pred_type) {
        case Predicate_T::GT:
        case Predicate_T::GTE:
        case Predicate_T::INSIDE:
        case Predicate_T::BETWEEN:
            low = m.lower_bound(pred.values[0]);
        }

        // remove "EQ"
        switch (pred.pred_type) {
        case Predicate_T::GT:
        case Predicate_T::INSIDE:
            if (low != m.end() && low->first == pred.values[0]) {
                low++;
            }
        }

        int param = 1;
        // get upper_bound
        switch (pred.pred_type) {
        case Predicate_T::LT:
        case Predicate_T::LTE:
            param = 0;
        case Predicate_T::INSIDE:
        case Predicate_T::BETWEEN:
            high = m.upper_bound(pred.values[param]);
        }

        // remove "EQ"
        switch (pred.pred_type) {
        case Predicate_T::LT:
        case Predicate_T::INSIDE:
            // exclude last one if match
            if (high != low) {
                high--;
                if (high->first != pred.values[param]) {
                    high++;
                }
            }
        }
    }
};
