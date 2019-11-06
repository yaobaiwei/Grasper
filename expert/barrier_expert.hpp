/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
         Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#pragma once
#include <limits.h>

#include "core/result_collector.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_cache.hpp"
#include "storage/data_store.hpp"
#include "utils/mkl_util.hpp"
#include "utils/tool.hpp"

#include <mkl_vsl.h>

namespace BarrierData {
struct barrier_data_base {
    map<string, int> path_counter;
};
}  // namespace BarrierData

// Base class for barrier experts
template<class T = BarrierData::barrier_data_base>
class BarrierExpertBase :  public AbstractExpert {
    static_assert(std::is_base_of<BarrierData::barrier_data_base, T>::value, "T must derive from barrier_data_base");
    using BarrierDataTable = tbb::concurrent_hash_map<mkey_t, T, MkeyHashCompare>;

 public:
    BarrierExpertBase(int id, DataStore* data_store, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity) {}

    void process(const vector<Expert_Object> & experts, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // get msg info
        mkey_t key;
        string end_path;
        GetMsgInfo(msg, key, end_path);

        typename BarrierDataTable::accessor ac;
        data_table_.insert(ac, key);

        bool isReady = IsReady(ac, msg.meta, end_path);

        do_work(tid, experts, msg, ac, isReady);

        if (isReady) {
            data_table_.erase(ac);

            // don't need to send out msg when next expert is still barrier expert
            if (is_next_barrier(experts, msg.meta.step)) {
                // move to next expert
                msg.meta.step = experts[msg.meta.step].next_expert;
                if (experts[msg.meta.step].expert_type == EXPERT_T::COUNT) {
                    for (auto& p : msg.data) {
                        value_t v;
                        Tool::str2int(to_string(p.second.size()), v);
                        p.second.clear();
                        p.second.push_back(move(v));
                    }
                }
            }
        }
    }

 protected:
    virtual void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, typename BarrierDataTable::accessor& ac, bool isReady) = 0;
    // get labelled branch key if in branch
    static int get_branch_key(Meta & m) {
        // check if count expert in branch
        // run locally if true
        int branch_depth = m.branch_infos.size();
        int key = - 1;
        if (branch_depth != 0) {
            key = m.branch_infos[branch_depth - 1].key;
        }
        return key;
    }

    // get branch value assigned by labelled branch expert from history
    static int get_branch_value(history_t& his, int branch_key, bool erase_his = true) {
        int branch_value = -1;
        if (branch_key >= 0) {
            // find key from history
            auto his_itr = std::find_if(his.begin(), his.end(),
                [&branch_key](const pair<int, value_t>& element){ return element.first == branch_key;});

            if (his_itr != his.end()) {
                // convert branch value to int
                branch_value = Tool::value_t2int(his_itr->second);
                // some barrier experts will remove hisotry after branch key
                if (erase_his) {
                    his.erase(his_itr + 1, his.end());
                }
            }
        }
        return branch_value;
    }

    static inline bool is_next_barrier(const vector<Expert_Object>& experts, int step) {
        int next = experts[step].next_expert;
        return next < experts.size() && experts[next].IsBarrier();
    }

    // projection functions from E/V to label or property
    static bool project_property_edge(int tid, value_t & val, int key, DataStore * datastore, ExpertCache* cache) {
        eid_t e_id;
        uint2eid_t(Tool::value_t2uint64_t(val), e_id);

        epid_t ep_id(e_id, key);

        val.content.clear();

        // Try cache
        if (datastore->EPKeyIsLocal(ep_id) || cache == NULL) {
            return datastore->GetPropertyForEdge(tid, ep_id, val);
        } else {
            bool found = true;
            if (!cache->get_property_from_cache(ep_id.value(), val)) {
                // not found in cache
                found = datastore->GetPropertyForEdge(tid, ep_id, val);
                if (found) {
                    cache->insert_properties(ep_id.value(), val);
                }
            }
            return found;
        }
    }

    static bool project_property_vertex(int tid, value_t & val, int key, DataStore * datastore, ExpertCache* cache) {
        vid_t v_id(Tool::value_t2int(val));
        vpid_t vp_id(v_id, key);

        val.content.clear();

        // Try cache
        if (datastore->VPKeyIsLocal(vp_id) || cache == NULL) {
            return datastore->GetPropertyForVertex(tid, vp_id, val);
        } else {
            bool found = true;
            if (!cache->get_property_from_cache(vp_id.value(), val)) {
                // not found in cache
                found = datastore->GetPropertyForVertex(tid, vp_id, val);
                if (found) {
                    cache->insert_properties(vp_id.value(), val);
                }
            }
            return found;
        }
    }

    static bool project_label_edge(int tid, value_t & val, int key, DataStore * datastore, ExpertCache* cache) {
        eid_t e_id;
        uint2eid_t(Tool::value_t2uint64_t(val), e_id);
        val.content.clear();

        label_t label;
        bool found;
        if (datastore->EPKeyIsLocal(epid_t(e_id, 0)) || cache == NULL) {
            found = datastore->GetLabelForEdge(tid, e_id, label);
        } else {
            found = true;
            if (!cache->get_label_from_cache(e_id.value(), label)) {
                found = datastore->GetLabelForEdge(tid, e_id, label);
                if (found) {
                    cache->insert_label(e_id.value(), label);
                }
            }
        }

        if (found) {
            string label_str;
            datastore->GetNameFromIndex(Index_T::E_LABEL, label, label_str);
            Tool::str2str(label_str, val);
        }
        return found;
    }

    static bool project_label_vertex(int tid, value_t & val, int key, DataStore * datastore, ExpertCache* cache) {
        vid_t v_id(Tool::value_t2int(val));

        val.content.clear();

        label_t label;
        bool found;
        if (datastore->VPKeyIsLocal(vpid_t(v_id, 0)) || cache == NULL) {
            found = datastore->GetLabelForVertex(tid, v_id, label);
        } else {
            found = true;
            if (!cache->get_label_from_cache(v_id.value(), label)) {
                found = datastore->GetLabelForVertex(tid, v_id, label);
                if (found) {
                    cache->insert_label(v_id.value(), label);
                }
            }
        }

        if (found) {
            string label_str;
            datastore->GetNameFromIndex(Index_T::V_LABEL, label, label_str);
            Tool::str2str(label_str, val);
        }
        return found;
    }

    static bool project_none(int tid, value_t & val, int key, DataStore * datastore, ExpertCache* cache) {
        return true;
    }

 private:
    // concurrent_hash_map, storing data for barrier processing
    BarrierDataTable data_table_;

    // Check if msg all collected
    static bool IsReady(typename BarrierDataTable::accessor& ac, Meta& m, string end_path) {
        map<string, int>& counter = ac->second.path_counter;
        string msg_path = m.msg_path;
        // check if all msg are collected
        while (msg_path != end_path) {
            int i = msg_path.find_last_of("\t");
            // "\t" should not be the the last char
            assert(i + 1 < msg_path.size());
            // get last number
            int num = atoi(msg_path.substr(i + 1).c_str());

            // check key
            if (counter.count(msg_path) != 1) {
                counter[msg_path] = 0;
            }

            // current branch is ready
            if ((++counter[msg_path]) == num) {
                // reset count to 0
                counter[msg_path] = 0;
                // remove last number
                msg_path = msg_path.substr(0, i == string::npos ? 0 : i);
            } else {
                return false;
            }
        }
        m.msg_path = end_path;
        return true;
    }

    // get msg info
    // key : mkey_t, identifier of msg
    // end_path: identifier of msg collection completed
    static void GetMsgInfo(Message& msg, mkey_t &key, string &end_path) {
        // init info
        uint64_t msg_id = 0;
        int index = 0;
        end_path = "";

        int branch_depth = msg.meta.branch_infos.size() - 1;
        if (branch_depth >= 0) {
            msg_id = msg.meta.branch_infos[branch_depth].msg_id;
            index = msg.meta.branch_infos[branch_depth].index;
            end_path = msg.meta.branch_infos[branch_depth].msg_path;
        }
        key = mkey_t(msg.meta.qid, msg_id, index);
    }
};

namespace BarrierData {
struct end_data : barrier_data_base {
    vector<value_t> result;
};
}  // namespace BarrierData

class EndExpert : public BarrierExpertBase<BarrierData::end_data> {
 public:
    EndExpert(int id, DataStore* data_store, int num_nodes, Result_Collector * rc, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::end_data>(id, data_store, core_affinity), num_nodes_(num_nodes), rc_(rc), mailbox_(mailbox) {}

 private:
    Result_Collector * rc_;
    AbstractMailbox * mailbox_;
    int num_nodes_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        #ifdef EXPERT_PROCESS_PRINT
        Node node = Node::StaticInstance();
        printf("%f, EXPERT = %s, %s, msg.meta.step = %d, node = %d, tid = %d\n", node.WtimeSinceStart(), "EndExpert::do_work", experts[msg.meta.step].DebugString().c_str(), msg.meta.step, node.get_local_rank(), tid);
        #endif

        auto& data = ac->second.result;

        // move msg data to data table
        for (auto& pair : msg.data) {
            data.insert(data.end(), std::make_move_iterator(pair.second.begin()), std::make_move_iterator(pair.second.end()));
        }

        // all msg are collected
        if (isReady) {
            // insert data to result collector
            rc_->InsertResult(msg.meta.qid, data);

            vector<Message> vec;
            msg.CreateExitMsg(num_nodes_, vec);
            for (auto& m : vec) {
                mailbox_->Send(tid, m);
            }
        }
    }
};

namespace BarrierData {
struct agg_data : barrier_data_base {
    vector<value_t> agg_data;
    vector<pair<history_t, vector<value_t>>> msg_data;
};
}  // namespace BarrierData

class AggregateExpert : public BarrierExpertBase<BarrierData::agg_data> {
 public:
    AggregateExpert(int id, DataStore* data_store, int num_nodes, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::agg_data>(id, data_store, core_affinity), num_nodes_(num_nodes), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_nodes_;
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& agg_data = ac->second.agg_data;
        auto& msg_data = ac->second.msg_data;

        // move msg data to data table
        for (auto& p : msg.data) {
            auto itr = find_if(msg_data.begin(), msg_data.end(),
                [&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
            if (itr == msg_data.end()) {
                itr = msg_data.insert(itr, make_pair(move(p.first), vector<value_t>()));
            }

            itr->second.insert(itr->second.end(), p.second.begin(), p.second.end());
            agg_data.insert(agg_data.end(), std::make_move_iterator(p.second.begin()), std::make_move_iterator(p.second.end()));
        }

        // all msg are collected
        if (isReady) {
            const Expert_Object& expert = experts[msg.meta.step];
            assert(expert.params.size() == 1);
            int key = Tool::value_t2int(expert.params[0]);

            // insert to current node's storage
            data_store_->InsertAggData(agg_t(msg.meta.qid, key), agg_data);

            vector<Message> v;
            // send aggregated data to other nodes
            msg.CreateFeedMsg(key, num_nodes_, agg_data, v);

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                // send input data and history to next expert
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
            }

            for (auto& m : v) {
                mailbox_->Send(tid, m);
            }
        }
    }
};

class CapExpert : public BarrierExpertBase<> {
 public:
    CapExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        // all msg are collected
        if (isReady) {
            const Expert_Object& expert = experts[msg.meta.step];
            vector<pair<history_t, vector<value_t>>> msg_data;
            msg_data.emplace_back(history_t(), vector<value_t>());

            // calculate max size of one value_t with empty history
            // max msg size - sizeof(data with one empty pair) - sizeof(empty value_t)
            size_t max_size = msg.max_data_size - MemSize(msg_data) - MemSize(value_t());

            assert(expert.params.size() % 2 == 0);

            // side-effect key list
            for (int i = 0; i < expert.params.size(); i+=2) {
                int se_key = Tool::value_t2int(expert.params.at(i));
                string se_string = Tool::value_t2string(expert.params.at(i+1));
                vector<value_t> vec;
                data_store_->GetAggData(agg_t(msg.meta.qid, se_key), vec);

                string temp = se_string + ":[";
                for (auto& val : vec) {
                    temp += Tool::DebugString(val) + ", ";
                }

                // remove trailing ", "
                if (vec.size() > 0) {
                    temp.pop_back();
                    temp.pop_back();
                }
                temp += "]";

                while (1) {
                    value_t v;
                    // each value_t should have at most max_size
                    Tool::str2str(temp.substr(0, max_size), v);
                    msg_data[0].second.push_back(move(v));
                    if (temp.size() > max_size) {
                        temp = temp.substr(max_size);
                    } else {
                        break;
                    }
                }
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct count_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //        history_t: histroy of data
    //        int:       record num of incoming data
    unordered_map<int, pair<history_t, int>> counter_map;
};
}  // namespace BarrierData

class CountExpert : public BarrierExpertBase<BarrierData::count_data> {
 public:
    CountExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::count_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& counter_map = ac->second.counter_map;
        int branch_key = get_branch_key(msg.meta);

        // process msg data
        for (auto& p : msg.data) {
            int count = 0;
            if (p.second.size() != 0) {
                count = Tool::value_t2int(p.second[0]);
            }
            int branch_value = get_branch_value(p.first, branch_key);

            // get <history_t, int> pair by branch_value
            auto itr_cp = counter_map.find(branch_value);
            if (itr_cp == counter_map.end()) {
                itr_cp = counter_map.insert(itr_cp, {branch_value, {move(p.first), 0}});
            }
            itr_cp->second.second += count;
        }

        // all msg are collected
        if (isReady) {
            vector<pair<history_t, vector<value_t>>> msg_data;
            for (auto& p : counter_map) {
                value_t v;
                Tool::str2int(to_string(p.second.second), v);
                msg_data.emplace_back(move(p.second.first), vector<value_t>{move(v)});
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct dedup_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // vec: filtered data
    unordered_map<int, vector<pair<history_t, vector<value_t>>>> data_map;
    unordered_map<int, unordered_set<history_t, HistoryTHash>> dedup_his_map;  // for dedup by history
    unordered_map<int, unordered_set<value_t, ValueTHash>> dedup_val_map;  // for dedup by value
};
}  // namespace BarrierData

class DedupExpert : public BarrierExpertBase<BarrierData::dedup_data> {
 public:
    DedupExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::dedup_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& data_map = ac->second.data_map;
        auto& dedup_his_map = ac->second.dedup_his_map;
        auto& dedup_val_map = ac->second.dedup_val_map;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];
        set<int> key_set;
        for (auto& param : expert.params) {
            key_set.insert(Tool::value_t2int(param));
        }

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key, false);

            // get data and history set under branch value
            auto& data_vec = data_map[branch_value];

            vector<pair<history_t, vector<value_t>>>::iterator itr_dp;
            if (data_vec.size() != 0) {
                if (data_vec[0].second.size() == 0) {
                    // clear useless history with empty data
                    data_vec.clear();
                    itr_dp = data_vec.end();
                } else {
                    // find if current history already added
                    itr_dp = find_if(data_vec.begin(), data_vec.end(),
                        [&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
                }
            } else {
                itr_dp = data_vec.end();
            }

            if (itr_dp == data_vec.end()) {
                itr_dp = data_vec.insert(itr_dp, {p.first, vector<value_t>()});
            }

            if (key_set.size() > 0 && p.second.size() != 0) {
                auto& dedup_set = dedup_his_map[branch_value];
                history_t his;
                // dedup history
                // construct history with given key
                for (auto& val : p.first) {
                    if (key_set.find(val.first) != key_set.end()) {
                        his.push_back(move(val));
                    }
                }
                // insert constructed history and check if exists
                if (dedup_set.insert(move(his)).second) {
                    itr_dp->second.push_back(move(p.second[0]));
                }
            } else {
                auto& dedup_set = dedup_val_map[branch_value];
                // dedup value, should check on all values
                for (auto& val : p.second) {
                    // insert value to set and check if exists
                    if (dedup_set.insert(val).second) {
                        itr_dp->second.push_back(move(val));
                    }
                }
            }
        }

        // all msg are collected
        if (isReady) {
            vector<pair<history_t, vector<value_t>>> msg_data;
            for (auto& p : data_map) {
                msg_data.insert(msg_data.end(), make_move_iterator(p.second.begin()), make_move_iterator(p.second.end()));
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct group_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //        history_t:                 histroy of data
    //        map<string,value_t>:    record key and values of grouped data
    unordered_map<int, pair<history_t, map<string, vector<value_t>>>> data_map;
};
}  // namespace BarrierData

class GroupExpert : public BarrierExpertBase<BarrierData::group_data> {
 public:
    GroupExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::group_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {
        config_ = Config::GetInstance();
    }

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    Config* config_;
    ExpertCache cache_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& data_map = ac->second.data_map;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];
        assert(expert.params.size() == 4);
        Element_T element_type = (Element_T)Tool::value_t2int(expert.params[1]);
        int keyProjection = Tool::value_t2int(expert.params[2]);
        int valueProjection = Tool::value_t2int(expert.params[3]);

        // get projection function by expert params
        bool(*kp)(int, value_t&, int, DataStore*, ExpertCache*) = project_none;
        bool(*vp)(int, value_t&, int, DataStore*, ExpertCache*) = project_none;
        if (keyProjection == 0) {
            kp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
        } else if (keyProjection > 0) {
            kp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
        }

        if (valueProjection == 0) {
            vp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
        } else if (valueProjection > 0) {
            vp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
        }

        ExpertCache * cache = NULL;
        if (config_->global_enable_caching) {
            cache = &cache_;
        }

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key);

            // get <history_t, map<string, vector<value_t>> pair by branch_value
            auto itr_data = data_map.find(branch_value);
            if (itr_data == data_map.end()) {
                itr_data = data_map.insert(itr_data, {branch_value, {move(p.first), map<string, vector<value_t>>()}});
            }
            auto& map_ = itr_data->second.second;

            for (auto& val : p.second) {
                value_t k = val;
                value_t v = val;
                if (!kp(tid, k, keyProjection, data_store_, cache)) {
                    continue;
                }
                if (!vp(tid, v, valueProjection, data_store_, cache)) {
                    continue;
                }
                string key = Tool::DebugString(k);
                map_[key].push_back(move(v));
            }
        }

        // all msg are collected
        if (isReady) {
            bool isCount = Tool::value_t2int(expert.params[0]);
            vector<pair<history_t, vector<value_t>>> msg_data;

            for (auto& p : data_map) {
                // calculate max size of one map_string with given history
                // max msg size - sizeof(data_vec) - sizeof(current history) - sizeof(empty value_t)
                size_t max_size = msg.max_data_size - MemSize(msg_data) - MemSize(p.second.first) - MemSize(value_t());

                vector<value_t> vec_val;
                for (auto& item : p.second.second) {
                    string map_string;
                    // construct string
                    if (isCount) {
                        map_string = item.first + ":" + to_string(item.second.size());
                    } else {
                        map_string = item.first + ":[";
                        for (auto& v : item.second) {
                            map_string += Tool::DebugString(v) + ", ";
                        }
                        // remove trailing ", "
                        if (item.second.size() > 0) {
                            map_string.pop_back();
                            map_string.pop_back();
                        }
                        map_string += "]";
                    }

                    while (1) {
                        value_t v;
                        // each value_t should have at most max_size
                        Tool::str2str(map_string.substr(0, max_size), v);
                        vec_val.push_back(move(v));
                        if (map_string.size() > max_size) {
                            map_string = map_string.substr(max_size);
                        } else {
                            break;
                        }
                    }
                }

                msg_data.emplace_back(move(p.second.first), move(vec_val));
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct order_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //        history_t:                             histroy of data
    //        map<value_t, multiset<value_t>>:
    //            value_t:                         key for ordering
    //            multiset<value_t>                store real data
    unordered_map<int, pair<history_t, map<value_t, multiset<value_t>>>> data_map;    // for order with mapping
    unordered_map<int, pair<history_t, multiset<value_t>>> data_set;                // for order without mapping
};
}  // namespace BarrierData

class OrderExpert : public BarrierExpertBase<BarrierData::order_data> {
 public:
    OrderExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::order_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {
        config_ = Config::GetInstance();
    }

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    ExpertCache cache_;
    Config* config_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& data_map = ac->second.data_map;
        auto& data_set = ac->second.data_set;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];
        assert(expert.params.size() == 3);
        Element_T element_type = (Element_T)Tool::value_t2int(expert.params[0]);
        int keyProjection = Tool::value_t2int(expert.params[1]);

        // get projection function by expert params
        bool(*kp)(int, value_t&, int, DataStore*, ExpertCache*) = project_none;
        if (keyProjection == 0) {
            kp = (element_type == Element_T::VERTEX) ? project_label_vertex : project_label_edge;
        } else if (keyProjection > 0) {
            kp = (element_type == Element_T::VERTEX) ? project_property_vertex : project_property_edge;
        }

        ExpertCache * cache = NULL;
        if (config_->global_enable_caching) {
            cache = &cache_;
        }

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key);

            if (keyProjection < 0) {
                // get <history_t, multiset<value_t>> pair by branch_value
                auto itr_data = data_set.find(branch_value);
                if (itr_data == data_set.end()) {
                    itr_data = data_set.insert(itr_data, {branch_value, {move(p.first), multiset<value_t>()}});
                }
                auto& set_ = itr_data->second.second;

                for (auto& val : p.second) {
                    set_.insert(move(val));
                }
            } else {
                // get <history_t, map<value_t, multiset<value_t>>> pair by branch_value
                auto itr_data = data_map.find(branch_value);
                if (itr_data == data_map.end()) {
                    itr_data = data_map.insert(itr_data, {branch_value, {move(p.first), map<value_t, multiset<value_t>>()}});
                }
                auto& map_ = itr_data->second.second;

                for (auto& val : p.second) {
                    value_t key = val;
                    if (!kp(tid, key, keyProjection, data_store_, cache)) {
                        continue;
                    }
                    map_[key].insert(move(val));
                }
            }
        }

        // all msg are collected
        if (isReady) {
            Order_T order = (Order_T)Tool::value_t2int(expert.params[2]);
            vector<pair<history_t, vector<value_t>>> msg_data;
            if (keyProjection < 0) {
                for (auto& p : data_set) {
                    vector<value_t> val_vec;
                    auto& set_ = p.second.second;
                    if (order == Order_T::INCR) {
                        val_vec.insert(val_vec.end(), make_move_iterator(set_.begin()), make_move_iterator(set_.end()));
                    } else {
                        val_vec.insert(val_vec.end(), make_move_iterator(set_.rbegin()), make_move_iterator(set_.rend()));
                    }
                    msg_data.emplace_back(move(p.second.first), move(val_vec));
                }
            } else {
                for (auto& p : data_map) {
                    vector<value_t> val_vec;
                    auto& m = p.second.second;
                    if (order == Order_T::INCR) {
                        for (auto itr = m.begin(); itr != m.end(); itr++) {
                            val_vec.insert(val_vec.end(), make_move_iterator(itr->second.begin()), make_move_iterator(itr->second.end()));
                        }
                    } else {
                        for (auto itr = m.rbegin(); itr != m.rend(); itr++) {
                            val_vec.insert(val_vec.end(), make_move_iterator(itr->second.rbegin()), make_move_iterator(itr->second.rend()));
                        }
                    }
                    msg_data.emplace_back(move(p.second.first), move(val_vec));
                }
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct range_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // pair:
    //        int: counter, record num of incoming data
    //        vec: record data in given range
    unordered_map<int, pair<int, vector<pair<history_t, vector<value_t>>>>> counter_map;
};
}  // namespace BarrierData

class RangeExpert : public BarrierExpertBase<BarrierData::range_data> {
 public:
    RangeExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::range_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& counter_map = ac->second.counter_map;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];
        assert(expert.params.size() == 2);
        int start = Tool::value_t2int(expert.params[0]);
        int end = Tool::value_t2int(expert.params[1]);
        if (end == -1) {
            end = INT_MAX;
        }

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key, false);

            // get <counter, vector<data_pair>> pair by branch_value
            auto itr_cp = counter_map.find(branch_value);
            if (itr_cp == counter_map.end()) {
                itr_cp = counter_map.insert(itr_cp, {branch_value, {0, vector<pair<history_t, vector<value_t>>>()}});
            }
            auto& counter_pair = itr_cp->second;

            // get vector<data_pair>
            vector<pair<history_t, vector<value_t>>>::iterator itr_vec;
            // check vector<data_pair>
            if (counter_pair.second.size() != 0) {
                // skip when exceed limit
                if (counter_pair.first > end)
                    continue;

                if (counter_pair.second[0].second.size() == 0) {
                    // clear useless history with empty data
                    counter_pair.second.clear();
                    itr_vec = counter_pair.second.end();
                } else {
                    // find if current history already added
                    itr_vec = find_if(counter_pair.second.begin(), counter_pair.second.end(),
                        [&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
                }
            } else {
                itr_vec = counter_pair.second.end();
            }

            // insert new history
            if (itr_vec == counter_pair.second.end()) {
                itr_vec = counter_pair.second.insert(itr_vec, {move(p.first), vector<value_t>()});
            }

            for (auto& val : p.second) {
                if (counter_pair.first > end) {
                    break;
                }
                // insert value when start <= count <= end
                if (counter_pair.first >= start) {
                    itr_vec->second.push_back(move(val));
                }
                (counter_pair.first)++;
            }
        }

        // all msg are collected
        if (isReady) {
            vector<pair<history_t, vector<value_t>>> msg_data;
            for (auto& p : counter_map) {
                msg_data.insert(msg_data.end(), make_move_iterator(p.second.second.begin()), make_move_iterator(p.second.second.end()));
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

class CoinExpert : public BarrierExpertBase<BarrierData::range_data> {
 public:
    CoinExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::range_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& counter_map = ac->second.counter_map;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];

        assert(expert.params.size() == 1);
        double rate = Tool::value_t2double(expert.params[0]);

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key, false);

            // get <counter, vector<data_pair>> pair by branch_value
            auto itr_cp = counter_map.find(branch_value);
            if (itr_cp == counter_map.end()) {
                itr_cp = counter_map.insert(itr_cp, {branch_value, {0, vector<pair<history_t, vector<value_t>>>()}});
            }
            auto& counter_pair = itr_cp->second;

            // get vector<data_pair>
            vector<pair<history_t, vector<value_t>>>::iterator itr_vec;
            // check vector<data_pair>
            if (counter_pair.second.size() != 0) {
                if (counter_pair.second[0].second.size() == 0) {
                    // clear useless history with empty data
                    counter_pair.second.clear();
                    itr_vec = counter_pair.second.end();
                } else {
                    // find if current history already added
                    itr_vec = find_if(counter_pair.second.begin(), counter_pair.second.end(),
                        [&p](const pair<history_t, vector<value_t>>& element){ return element.first == p.first;});
                }
            } else {
                itr_vec = counter_pair.second.end();
            }

            // insert new history
            if (itr_vec == counter_pair.second.end()) {
                itr_vec = counter_pair.second.insert(itr_vec, {move(p.first), vector<value_t>()});
            }

            int sz = p.second.size();

            if (sz > 0) {
                float* tmp_rand_arr = new float[sz];

                MKLUtil::GetInstance()->UniformRNGF4(tmp_rand_arr, sz, 0.0, 1.0);

                for (int i = 0; i < sz; i++) {
                    if (tmp_rand_arr[i] < rate)
                        itr_vec->second.push_back(move(p.second[i]));
                }

                delete[] tmp_rand_arr;
            }
        }

        // all msg are collected
        if (isReady) {
            vector<pair<history_t, vector<value_t>>> msg_data;
            for (auto& p : counter_map) {
                msg_data.insert(msg_data.end(), make_move_iterator(p.second.second.begin()), make_move_iterator(p.second.second.end()));
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }
};

namespace BarrierData {
struct math_meta_t {
    int count;
    value_t value;
    history_t history;
};

struct math_data : barrier_data_base {
    // int: assigned branch value by labelled branch step
    // math_data_t: meta data for math
    unordered_map<int, math_meta_t> data_map;
};
}  // namespace BarrierData

class MathExpert : public BarrierExpertBase<BarrierData::math_data> {
 public:
    MathExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : BarrierExpertBase<BarrierData::math_data>(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}

 private:
    int num_thread_;
    AbstractMailbox * mailbox_;

    void do_work(int tid, const vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& data_map = ac->second.data_map;
        int branch_key = get_branch_key(msg.meta);

        // get expert params
        const Expert_Object& expert = experts[msg.meta.step];
        assert(expert.params.size() == 1);
        Math_T math_type = (Math_T)Tool::value_t2int(expert.params[0]);
        void (*op)(BarrierData::math_meta_t&, value_t&);
        switch (math_type) {
            case Math_T::SUM:
            case Math_T::MEAN:   op = sum; break;
            case Math_T::MAX:    op = max; break;
            case Math_T::MIN:    op = min; break;
            default:             cout << "Unexpected math type in MathExpert" << endl;
        }

        // process msg data
        for (auto& p : msg.data) {
            int branch_value = get_branch_value(p.first, branch_key);

            // get math_data_t by branch_value
            auto itr_data = data_map.find(branch_value);
            if (itr_data == data_map.end()) {
                itr_data = data_map.insert(itr_data, {branch_value, BarrierData::math_meta_t()});
                itr_data->second.history = move(p.first);
                itr_data->second.count = 0;
            }

            for (auto& val : p.second) {
                op(itr_data->second, val);    // operate on new value
            }
        }

        // all msg are collected
        if (isReady) {
            bool isMean = math_type == Math_T::MEAN;
            vector<pair<history_t, vector<value_t>>> msg_data;
            for (auto& p : data_map) {
                BarrierData::math_meta_t& data = p.second;
                vector<value_t> val_vec;
                if (data.count > 0) {
                    // convert value to double
                    to_double(data, isMean);
                    val_vec.push_back(move(data.value));
                }
                msg_data.emplace_back(move(data.history), move(val_vec));
            }

            if (is_next_barrier(experts, msg.meta.step)) {
                msg.data = move(msg_data);
            } else {
                vector<Message> v;
                msg.CreateNextMsg(experts, msg_data, num_thread_, data_store_, core_affinity_, v);
                for (auto& m : v) {
                    mailbox_->Send(tid, m);
                }
            }
        }
    }

    static void sum(BarrierData::math_meta_t& data, value_t& v) {
        data.count++;
        if (data.count == 1) {
            data.value = move(v);
            return;
        }
        value_t temp = data.value;
        data.value.content.clear();
        switch (v.type) {
            case 1:
                Tool::str2int(to_string(Tool::value_t2int(temp) + Tool::value_t2int(v)), data.value);
                break;
            case 2:
                Tool::str2double(to_string(Tool::value_t2double(temp) + Tool::value_t2double(v)), data.value);
                break;
        }
    }

    static void max(BarrierData::math_meta_t& data, value_t& v) {
        if (data.count == 0 || data.value < v) {
            data.value = move(v);
        }
        data.count++;
    }

    static void min(BarrierData::math_meta_t& data, value_t& v) {
        if (data.count == 0 || data.value > v) {
            data.value = move(v);
        }
        data.count++;
    }

    static void to_double(BarrierData::math_meta_t& data, bool isMean) {
        value_t temp = data.value;

        // divide value by count if isMean
        int count = isMean ? data.count : 1;

        data.value.content.clear();
        switch (data.value.type) {
            case 1:
                Tool::str2double(to_string((double)Tool::value_t2int(temp) / count), data.value);
                break;
            case 2:
                Tool::str2double(to_string(Tool::value_t2double(temp) / count), data.value);
                break;
        }
    }
};
