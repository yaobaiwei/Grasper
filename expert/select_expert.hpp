/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef SELECT_EXPERT_HPP_
#define SELECT_EXPERT_HPP_

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

class SelectExpert : public AbstractExpert {
 public:
    SelectExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity * core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::SELECT) {}

    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        assert(expert_obj.params.size() % 2 == 0);
        // Get Params
        vector<pair<int, string>> label_step_list;
        for (int i = 0; i < expert_obj.params.size(); i+=2) {
            label_step_list.emplace_back(Tool::value_t2int(expert_obj.params.at(i)), Tool::value_t2string(expert_obj.params.at(i + 1)));
        }

        // sort label_step_list for quick search
        sort(label_step_list.begin(), label_step_list.end(),
            [](const pair<int, string>& l, const pair<int, string>& r){ return l.first < r.first;});

        //  Grab history_t
        if (label_step_list.size() != 1) {
            GrabHistory(label_step_list, msg.data);
        } else {
            GrabHistory(label_step_list[0].first, msg.data);
        }

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

    void GrabHistory(vector<pair<int, string>> label_step_list, vector<pair<history_t, vector<value_t>>> & data) {
        vector<value_t> result;

        for (auto & data_pair : data) {
            int value_size = data_pair.second.size();
            string res = "[";
            bool isResultEmpty = true;

            auto l_itr = label_step_list.begin();

            if (!data_pair.first.empty()) {
                vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();

                // once there is one list ends, end search
                do {
                    // cout << "label_step_list_item & history_list_item : " << *l_itr << " & " << (*p_itr).first << endl;
                    if (l_itr->first == p_itr->first) {
                        res += l_itr->second + ":" + Tool::DebugString(p_itr->second) + ", ";
                        isResultEmpty = false;

                        l_itr++;
                        p_itr++;
                    } else if (l_itr->first < p_itr->first) {
                        l_itr++;
                    } else if (l_itr->first > p_itr->first) {
                        p_itr++;
                    }
                } while (l_itr != label_step_list.end() && p_itr != data_pair.first.end());
            }

            if (!isResultEmpty) {
                res.pop_back();
                res.pop_back();
            }
            res += "]";

            // cout << "Copy " << data_pair.second.size() << " times for " << res << endl;
            if (!data_pair.first.empty() && !isResultEmpty) {
                for (int i = 0; i < data_pair.second.size(); i++) {
                    value_t val;
                    Tool::str2str(res, val);
                    result.push_back(val);
                }
            }

            data_pair.second.swap(result);
            result.clear();
        }
    }

    void GrabHistory(int label_step, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> result;
            if (!data_pair.first.empty()) {
                vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();
                do {
                    if (label_step == (*p_itr).first) {
                        for (int i = 0; i < data_pair.second.size(); i++) {
                            result.push_back((*p_itr).second);
                        }
                        break;
                    }
                    p_itr++;
                } while (p_itr != data_pair.first.end());
            }

            data_pair.second.swap(result);
        }
    }
};

#endif /* SELECT_EXPERT_HPP_ */
