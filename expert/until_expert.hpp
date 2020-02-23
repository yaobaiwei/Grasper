//
// Created by Oruqimaru on 20/2/2020.
//

#ifndef UNTIL_EXPERT_HPP_
#define UNTIL_EXPERT_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"


struct agg_data {
    vector<value_t> agg_data;
    vector<pair<history_t, vector<value_t>>> msg_data;
};

// Bowen: we need to use a new message type instead of SPAWN to avoid confusion
class UntilExpert : public AbstractExpert {
using BarrierDataTable = tbb::concurrent_hash_map<mkey_t, agg_data, MkeyHashCompare>;

public:
    UntilExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::UNTIL) {}

    // [pred_T , pred_params]...
    // Bowen: cannot let the expert_objs be a constant reference
    void process(vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            process_spawn(const vector<Expert_Object> & expert_objs, Message & msg);
        } else if (msg.meta.msg_type == MSG_T::BRANCH) {
            // get branch message key
            mkey_t key;
            string end_path;
            GetMsgInfo(msg, key, end_path);

            typename BarrierDataTable::accessor ac;
            data_table_.find(ac, key);

            bool isReady = IsReady(ac, msg.meta, end_path);

            process_barrier(tid, experts, msg, ac, isReady);

            if (isReady) {
                data_table_.erase(ac);
            }
        } else {
            cout << "Unexpected msg type in until expert." << endl;
            exit(-1);
        }
    }

private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    BarrierDataTable data_table_; // Bowen


//    void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateValue> & pred_chain) {
//        auto checkFunction = [&](value_t & value) {
//            int counter = pred_chain.size();
//            for (auto & pred : pred_chain) {
//                if (Evaluate(pred, &value)) {
//                    counter--;
//                }
//            }
//
//            // Not match all pred
//            if (counter != 0) {
//                return true;
//            }
//            return false;
//        };
//
//        for (auto & data_pair : data) {
//            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
//        }
//    }

    // Bowen: This is imcomplete
    // 1. The new history record is not created. Lables are not handled. Refer to as_expert.hpp
    void process_spawn(const vector<Expert_Object> & expert_objs, Message & msg){
        int tid = TidMapper::GetInstance()->GetTid();

        Meta &m = msg.meta;
        Expert_Object expert_object = expert_objs[m.step];

        vector<pair<history_t, vector<value_t>>> &data = msg.data;

        auto data_end = data.end();

        for (auto& pair: data) {
            for (auto& data_point : pair.second){
                data.push_back(std::make_pair(pair.first, data_point));
            }
        }

        data.erase(data.begin(), data_end);

        vector<int> step_vec;
        vector<Message> msg_vec;

        step_vec.push_back(expert_objs[msg.meta.step].next_expert);

        msg.CreateBranchedMsg(expert_objs, step_vec, num_thread_, data_store_, core_affinity_, msg_vec);

        for (auto& m : msg_vec) {
            mailbox_->Send(tid, m);
        }
    }

    void process_barrier(int tid, vector<Expert_Object> & experts, Message & msg, BarrierDataTable::accessor& ac, bool isReady) {
        auto& agg_data = ac->second.agg_data;
        auto& msg_data = ac->second.msg_data;
        int his_index = msg.his_index; // index of the relevant history record

        // move msg data to data table
        for (auto& p : msg.data) {
            auto itr = find_if(msg_data.begin(), msg_data.end(),
                [&p](const pair<history_t, vector<value_t>>& element){ return element.first.back() == p.first[his_index];});
            if (itr == msg_data.end()) {
                // erase the history written by Tu
                p.first.erase(p.first.begin()+his_index+1, p.first.end());
                itr = msg_data.insert(itr, make_pair(move(p.first), vector<value_t>()));
            }

            itr->second.insert(itr->second.end(), p.second.begin(), p.second.end());
            agg_data.insert(agg_data.end(), std::make_move_iterator(p.second.begin()), std::make_move_iterator(p.second.end()));
        }

        // all msg are collected
        if (isReady) {
            const Expert_Object& expert = experts[msg.meta.step];
            assert(expert.params.size() == 1);
            int repeat_entrance = Tool::value_t2int(expert.params[0]);

            // check if the traverser pass the condition
            vector<pair<history_t, vector<value_t>>> pass_msg_data;
            vector<pair<history_t, vector<value_t>>> fail_msg_data;
            for (auto& p : msg_data) {
                pair<history_t, vector<value_t>> tmp;
                tmp = make_pair(move(p.first), vector<value_t>())
                tmp.second.insert(tmp.second.end(), tmp.first.back())
                tmp.first.pop_back();

                if(p.second.size() == 0) {
                    fail_msg_data.insert(fail_msg_data.end(), move(tmp));
                } else {
                    pass_msg_data.insert(pass_msg_data.end(), move(tmp));
                }
            }

            vector<Message> v_fail;
            int current_next_expert = expert_object.next_expert;
            expert_object.next_expert = Tool::value_t2int(expert_object.params.at(0));
            
            // send input data and history back to repeat
            msg.CreateNextMsg(experts, fail_msg_data, num_thread_, data_store_, core_affinity_, v_fail);

            for (auto& m : v_fail) {
                mailbox_->Send(tid, m);
            }

            vector<Message> v_pass;
            expert_object.next_expert = current_next_expert;

            // send input data and history to next expert
            msg.CreateNextMsg(experts, pass_msg_data, num_thread_, data_store_, core_affinity_, v_pass);

            for (auto& m : v_pass) {
                mailbox_->Send(tid, m);
            }
        }
    }

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



#endif //GRASPER_UNTIL_EXPERT_HPP
