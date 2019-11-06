/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef WHERE_EXPERT_HPP_
#define WHERE_EXPERT_HPP_

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

class WhereExpert : public AbstractExpert {
 public:
    WhereExpert(int id, DataStore * data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity * core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::WHERE) {}

    // Where:
    // [    label_step_key:  int
    //         pred: Predicate_T
    //         pred_param: value_t]...
    //
    //     e.g. g.V().as('a'),,,.where(neq('a'))
    //           g.V().as('a'),,,.as('b').,,,.where('a', neq('b'))
    //
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // store all predicate
        vector<PredicateHistory> pred_chain;

        // Get Params
        assert(expert_obj.params.size() > 0 && (expert_obj.params.size() % 3) == 0);
        int numParamsGroup = expert_obj.params.size() / 3;  // number of groups of params

        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3;

            // Create pred chain
            vector<int> his_labels;
            // For aggregate stored data
            map<value_t, int> agg_datas;

            his_labels.push_back(Tool::value_t2int(expert_obj.params.at(pos)));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos + 1));

            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 2), pred_params);

            if (pred_type == Predicate_T::WITHIN || pred_type == Predicate_T::WITHOUT) {
                for (auto & param : pred_params) {
                    vector<value_t> tmp_agg_data;
                    int his_label = Tool::value_t2int(param);

                    if (!HasAggregateData(agg_t(m.qid, his_label), tmp_agg_data)) {
                        msg.meta.recver_tid = msg.meta.parent_tid;
                        mailbox_->Send(tid, msg);
                        return;
                    }

                    for (auto & sgl_data : tmp_agg_data) {
                        if (agg_datas.find(sgl_data) == agg_datas.end()) {
                            agg_datas.insert(pair<value_t, int>(sgl_data, 1));
                        }
                    }
                }

                EvaluateForAggregate(msg.data, agg_datas, Tool::value_t2int(expert_obj.params.at(pos)), pred_type);
                continue;
            }

            for (auto & param : pred_params) {
                his_labels.push_back(Tool::value_t2int(param));
            }
            pred_chain.emplace_back(pred_type, his_labels);
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

    void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateHistory> & pred_chain) {
        for (auto & pred : pred_chain) {
            Predicate_T pred_type = pred.pred_type;
            vector<int> step_labels = pred.history_step_labels;

            if (step_labels.at(0) == -1) {
                // Find the value of history_label
                for (auto & data_pair : data) {
                    vector<value_t> his_val;
                    if (!GetHistoryValue(data_pair.first, vector<int>(step_labels.begin() + 1, step_labels.end()), his_val)) {
                        data_pair.second.clear();
                        continue;
                    }

                    PredicateValue single_pred(pred_type, his_val);
                    auto checkSinglePred = [&](value_t & value) {
                        return !Evaluate(single_pred, &value);
                    };
                    data_pair.second.erase(remove_if(data_pair.second.begin(),
                                    data_pair.second.end(),
                                    checkSinglePred),
                                    data_pair.second.end());
                }
            } else {
                for (auto & data_pair : data) {
                    vector<value_t> his_val;
                    if (!GetHistoryValue(data_pair.first, step_labels, his_val)) {
                        data_pair.second.clear();
                        continue;
                    }

                    if (step_labels.size() > 2) {
                        PredicateValue single_pred(pred_type, vector<value_t>(his_val.begin() + 1, his_val.end()));
                        if (!Evaluate(single_pred, &his_val.at(0))) {
                            data_pair.second.clear();
                        }
                    } else {
                        if (!Evaluate(pred_type, his_val.at(0), his_val.at(1))) {
                            data_pair.second.clear();
                        }
                    }
                }
            }
        }
    }

    void EvaluateForAggregate(vector<pair<history_t, vector<value_t>>> & data, map<value_t, int> & agg_data, int his_label, Predicate_T pred_type) {
        auto checkFunction = [&](value_t & value) {
            if (pred_type == Predicate_T::WITHIN) {
                if (agg_data.find(value) == agg_data.end()) {
                    return true;
                }
                return false;
            } else {  // WITHOUT
                if (agg_data.find(value) != agg_data.end()) {
                    return true;
                }
                return false;
            }
        };

        if (his_label == -1) {
            for (auto & data_pair : data) {
                data_pair.second.erase(remove_if(data_pair.second.begin(),
                                data_pair.second.end(),
                                checkFunction),
                                data_pair.second.end());
            }
        } else {
            for (auto & data_pair : data) {
                for (auto his_itr = data_pair.first.begin(); his_itr != data_pair.first.end(); ) {
                    if ((*his_itr).first == his_label) {
                        if (pred_type == Predicate_T::WITHIN) {
                            if (agg_data.find((*his_itr).second) == agg_data.end()) {
                                data_pair.second.clear();
                            }
                        } else {  // WIHTOUT
                            if (agg_data.find((*his_itr).second) != agg_data.end()) {
                                data_pair.second.clear();
                            }
                        }
                    }
                    his_itr++;
                }
            }
        }
    }

    bool HasAggregateData(agg_t key, vector<value_t> & agg_data) {
        data_store_->GetAggData(key, agg_data);

        if (agg_data.size() == 0) {
            return false;
        }
        return true;
    }

    bool GetHistoryValue(history_t & data_his, vector<int> step_labels, vector<value_t> & his_val) {
        int counter = step_labels.size();
        for (auto & step_label : step_labels) {
            history_t::iterator his_itr = data_his.begin();
            do {
                if ((*his_itr).first == step_label) {
                    his_val.push_back((*his_itr).second);
                    counter--;
                    break;
                }
                his_itr++;
            } while (his_itr != data_his.end());
        }

        // IF not found all
        if (counter != 0) {
            return false;
        }
        return true;
    }
};

#endif /* WHERE_EXPERT_HPP_ */
