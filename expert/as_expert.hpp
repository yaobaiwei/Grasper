/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef AS_EXPERT_HPP_
#define AS_EXPERT_HPP_

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

class AsExpert : public AbstractExpert {
 public:
    AsExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::AS) {}

    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get Params
        int label_step_key = Tool::value_t2int(expert_obj.params.at(0));

        // record history_t
        RecordHistory(label_step_key, msg.data);

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

    // write into side effect
    void RecordHistory(int label_step_key, vector<pair<history_t, vector<value_t>>> & data) {
        vector<pair<history_t, vector<value_t>>> newData;
        map<value_t, int> value_pos;
        int cnt;

        for (auto & data_pair : data) {
            value_pos.clear();
            cnt = 0;
            for (auto & elem : data_pair.second) {
                history_t his;
                vector<value_t> newValue;

                // Check whether elem is already in newData
                map<value_t, int>::iterator itr = value_pos.find(elem);
                if (itr == value_pos.end()) {
                    // move previous history to new one
                    his = data_pair.first;

                    // push back current label key
                    his.emplace_back(label_step_key, elem);

                    newValue.push_back(elem);
                    newData.emplace_back(his, newValue);

                    // Insert into map
                    value_pos.insert(pair<value_t, int>(elem, cnt++));
                } else {
                    // append elem to certain history
                    int pos = value_pos.at(elem);

                    if (pos >= value_pos.size()) {
                        cout << "Position larger than size : " << pos << " & " << value_pos.size() << endl;
                    } else {
                        newData.at(pos).second.push_back(elem);
                    }
                }
            }
        }

        data.swap(newData);
    }
};

#endif /* AS_EXPERT_HPP_ */
