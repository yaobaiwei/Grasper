/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef LABEL_EXPERT_HPP_
#define LABEL_EXPERT_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_cache.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

class LabelExpert : public AbstractExpert {
 public:
    LabelExpert(int id, DataStore* data_store, int machine_id, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), machine_id_(machine_id), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::LABEL) {
        config_ = Config::GetInstance();
    }
    // Label:
    //         Output all labels of input
    // Parmas:
    //         inType
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));

        switch (inType) {
            case Element_T::VERTEX:
                VertexLabel(tid, msg.data);
                break;
            case Element_T::EDGE:
                EdgeLabel(tid, msg.data);
                break;
            default:
                cout << "Wrong in type"  << endl;
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
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Cache
    ExpertCache cache;
    Config* config_;

    void VertexLabel(int tid, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                label_t label;
                if (data_store_->VPKeyIsLocal(vpid_t(v_id, 0)) || !config_->global_enable_caching) {
                    data_store_->GetLabelForVertex(tid, v_id, label);
                } else {
                    if (!cache.get_label_from_cache(v_id.value(), label)) {
                        data_store_->GetLabelForVertex(tid, v_id, label);
                        cache.insert_label(v_id.value(), label);
                    }
                }

                string keyStr;
                data_store_->GetNameFromIndex(Index_T::V_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }

            data_pair.second.swap(newData);
        }
    }

    void EdgeLabel(int tid, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                label_t label;
                if (data_store_->EPKeyIsLocal(epid_t(e_id, 0)) || !config_->global_enable_caching) {
                    data_store_->GetLabelForEdge(tid, e_id, label);
                } else {
                    if (!cache.get_label_from_cache(e_id.value(), label)) {
                        data_store_->GetLabelForEdge(tid, e_id, label);
                        cache.insert_label(e_id.value(), label);
                    }
                }

                string keyStr;
                data_store_->GetNameFromIndex(Index_T::E_LABEL, label, keyStr);

                value_t val;
                Tool::str2str(keyStr, val);
                newData.push_back(val);
            }

            data_pair.second.swap(newData);
        }
    }
};

#endif /* LABEL_EXPERT_HPP_ */
