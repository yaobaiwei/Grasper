/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include "expert/abstract_expert.hpp"

struct msg_id_alloc {
    uint64_t id;
    mutex id_mutex;
    msg_id_alloc() : id(1) {}
    void AssignId(uint64_t &msg_id) {
        id_mutex.lock();
        msg_id = id++;
        id_mutex.unlock();
    }
};

namespace BranchData {
struct branch_data_base {
    map<string, int> path_counter;
    pair<int, int> branch_counter;
};
}  // namespace BranchData

// Base class for labelled branch experts
// Process on a single traverser instead of entire incoming data flow
// Branched msg will aggregate back to labelled branch expert
template<typename T = BranchData::branch_data_base>
class LabelledBranchExpertBase :  public AbstractExpert {
    static_assert(std::is_base_of<BranchData::branch_data_base, T>::value, "T must derive from barrier_data_base");
    using BranchDataTable = tbb::concurrent_hash_map<mkey_t, T, MkeyHashCompare>;

 public:
    LabelledBranchExpertBase(int id, DataStore* data_store, int num_thread, AbstractMailbox* mailbox, CoreAffinity* core_affinity, msg_id_alloc* allocator): AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), id_allocator_(allocator) {}

    void process(const vector<Expert_Object> & experts,  Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            uint64_t msg_id;
            id_allocator_->AssignId(msg_id);
            // set up data for sub branch collection
            int index = 0;
            if (msg.meta.branch_infos.size() > 0) {
                // get index of parent branch if any
                index = msg.meta.branch_infos[msg.meta.branch_infos.size() - 1].index;
            }
            mkey_t key(msg.meta.qid, msg_id, index);

            typename BranchDataTable::accessor ac;
            data_table_.insert(ac, key);
            send_branch_msg(tid, experts, msg, msg_id);
            ac->second.branch_counter = make_pair(get_steps_count(experts[msg.meta.step]), 0);

            process_spawn(msg, ac);
        } else if (msg.meta.msg_type == MSG_T::BRANCH) {
            // get branch message key
            mkey_t key;
            string end_path;
            GetMsgInfo(msg, key, end_path);

            typename BranchDataTable::accessor ac;
            data_table_.find(ac, key);

            bool isReady = IsReady(ac, msg.meta, end_path);

            process_branch(tid, experts, msg, ac, isReady);

            if (isReady) {
                data_table_.erase(ac);
            }
        } else {
            cout << "Unexpected msg type in branch expert." << endl;
            exit(-1);
        }
    }

 protected:
    int num_thread_;
    AbstractMailbox* mailbox_;

    // Child class process message with type = SPAWN
    virtual void process_spawn(Message & msg, typename BranchDataTable::accessor& ac) = 0;

    // Child class process message with type = BRANCH
    virtual void process_branch(int tid, const vector<Expert_Object> & experts, Message & msg, typename BranchDataTable::accessor& ac, bool isReady) = 0;

    // get sub steps of branch expert
    virtual void get_steps(const Expert_Object & expert, vector<int>& steps) = 0;
    virtual int get_steps_count(const Expert_Object & expert) = 0;

 private:
    // assign unique msg id
    msg_id_alloc* id_allocator_;

    BranchDataTable data_table_;

    // send out msg with history label to indicate each input traverser
    void send_branch_msg(int tid, const vector<Expert_Object> & experts, Message & msg, uint64_t msg_id) {
        vector<int> step_vec;
        get_steps(experts[msg.meta.step], step_vec);

        vector<Message> msg_vec;
        msg.CreateBranchedMsgWithHisLabel(experts, step_vec, msg_id, num_thread_, data_store_, core_affinity_, msg_vec);
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    // check if all branched steps are collected
    static bool IsReady(typename BranchDataTable::accessor& ac, Meta& m, string end_path) {
        map<string, int>& counter = ac->second.path_counter;
        pair<int, int>& branch_counter = ac->second.branch_counter;
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

        branch_counter.second++;
        if (branch_counter.first == branch_counter.second) {
            m.msg_path = end_path;
            return true;
        }
        return false;
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
            // msg info given by current expert
            msg_id = msg.meta.branch_infos[branch_depth].msg_id;
            end_path = msg.meta.branch_infos[branch_depth].msg_path;
        }

        if (branch_depth >= 1) {
            // msg info given by parent expert if any
            index = msg.meta.branch_infos[branch_depth - 1].index;
        }
        key = mkey_t(msg.meta.qid, msg_id, index);
    }
};

namespace BranchData {
struct branch_filter_data : branch_data_base {
    // count num of successful branches for each data
    map<int, uint32_t> counter;
    // store input data of current step
    vector<pair<history_t, vector<value_t>>> data;
};
}  // namespace BranchData

class BranchFilterExpert : public LabelledBranchExpertBase<BranchData::branch_filter_data> {
 public:
    BranchFilterExpert(int id, DataStore* data_store_, int num_thread, AbstractMailbox* mailbox, CoreAffinity* core_affinity, msg_id_alloc* allocator): LabelledBranchExpertBase<BranchData::branch_filter_data>(id, data_store_, num_thread, mailbox, core_affinity, allocator) {}

 private:
    void process_spawn(Message & msg, BranchDataTable::accessor& ac) {
        ac->second.data = move(msg.data);
    }

    void process_branch(int tid, const vector<Expert_Object> & experts, Message & msg, BranchDataTable::accessor& ac, bool isReady) {
        auto &counter =  ac->second.counter;

        // get branch infos
        int info_size = msg.meta.branch_infos.size();
        assert(info_size > 0);
        int branch_index = msg.meta.branch_infos[info_size - 1].index;
        int his_key = msg.meta.branch_infos[info_size - 1].key;

        for (auto& p : msg.data) {
            // empty data, not suceess
            if (p.second.size() == 0) {
                continue;
            }

            // find history with given key
            auto his_itr = std::find_if(p.first.begin(), p.first.end(),
                [&his_key](const pair<int, value_t>& element){ return element.first == his_key; });

            if (his_itr == p.first.end()) {
                continue;
            }

            // get index of data
            int data_index = Tool::value_t2int(his_itr->second);
            // update counter
            update_counter(counter[data_index], branch_index);
        }

        if (isReady) {
            // get expert info
            const Expert_Object& expert = experts[msg.meta.step];
            int num_of_branch = ac->second.branch_counter.first;
            Filter_T filter_type = (Filter_T)Tool::value_t2int(expert.params[0]);

            // get filter function according to filter type
            bool (*pass)(uint32_t, int);
            switch (filter_type) {
                case Filter_T::AND:     pass = all_success;  break;
                case Filter_T::OR:      pass = any_success;  break;
                case Filter_T::NOT:     pass = none_success; break;
            }

            vector<pair<history_t, vector<value_t>>> &data = ac->second.data;
            int i = 0;
            auto checkFunction = [&](value_t & value) {
                return !pass(counter[i++], num_of_branch);
            };

            // filter
            for (auto& pair : data) {
                pair.second.erase(remove_if(pair.second.begin(), pair.second.end(), checkFunction), pair.second.end());
            }

            // remove last branch info
            msg.meta.branch_infos.pop_back();
            vector<Message> v;
            msg.CreateNextMsg(experts, data, num_thread_, data_store_, core_affinity_, v);
            for (auto& m : v) {
                mailbox_->Send(tid, m);
            }
        }
    }

    void get_steps(const Expert_Object & expert, vector<int>& steps) {
        vector<value_t> params = expert.params;
        assert(params.size() > 1);
        for (int i = 1; i < params.size(); i++) {
            steps.push_back(Tool::value_t2int(params[i]));
        }
    }

    int get_steps_count(const Expert_Object & expert) {
        return expert.params.size() - 1;
    }

    // each bit of counter indicates one branch index
    static inline void update_counter(uint32_t & counter, int branch_index) {
        // set corresponding bit to 1
        counter |= (1 << (branch_index - 1));
    }

    static bool all_success(uint32_t counter, int num_of_branch) {
        // check if all required bits are 1
        return counter == ((1 << num_of_branch) - 1);
    }

    static bool none_success(uint32_t counter, int num_of_branch) {
        // check if all bits are 0
        return counter == 0;
    }

    static bool any_success(uint32_t counter, int num_of_branch) {
        // check if any bit is 1
        return counter >= 1;
    }
};
