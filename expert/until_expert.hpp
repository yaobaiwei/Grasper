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

class UntilExpert : public AbstractExpert {
public:
    UntilExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::UNTIL) {}

    // [pred_T , pred_params]...
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        if (msg.meta.msg_type == MSG_T::SPAWN)
            processSpawnMsg(const vector<Expert_Object> & expert_objs, Message & msg);

    }

private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

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

    void processSpawnMsg(const vector<Expert_Object> & expert_objs, Message & msg){
        int tid = TidMapper::GetInstance()->GetTid();

        Meta &m = msg.meta;
        Expert_Object expert_object = expert_objs[m.step];

        vector<pair<history_t, vector<value_t>>> &data = msg.data;

        auto data_end = data.end();

        for (auto& pair: data){
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
};



#endif //GRASPER_UNTIL_EXPERT_HPP
