/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef IS_EXPERT_HPP_
#define IS_EXPERT_HPP_

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

class IsExpert : public AbstractExpert {
 public:
    IsExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::IS) {}

    // [pred_T , pred_params]...
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get Params
        vector<PredicateValue> pred_chain;

        assert(expert_obj.params.size() > 0 && (expert_obj.params.size() % 2) == 0);
        int numParamsGroup = expert_obj.params.size() / 2;

        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 2;
            // Get predicate params
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos));
            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 1), pred_params);

            pred_chain.emplace_back(pred_type, pred_params);
        }

        // Evaluate
        EvaluateData(msg.data, pred_chain);

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(expert_objs, msg.data, num_thread_, data_store_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
     }

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateValue> & pred_chain) {
        auto checkFunction = [&](value_t & value) {
            int counter = pred_chain.size();
            for (auto & pred : pred_chain) {
                if (Evaluate(pred, &value)) {
                    counter--;
                }
            }

            // Not match all pred
            if (counter != 0) {
                return true;
            }
            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }
};

#endif /* IS_EXPERT_HPP_ */
