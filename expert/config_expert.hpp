/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/
#ifndef CONFIG_EXPERT_HPP_
#define CONFIG_EXPERT_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "expert/abstract_expert.hpp"
#include "storage/data_store.hpp"
#include "utils/config.hpp"
#include "utils/tool.hpp"

class ConfigExpert : public AbstractExpert {
 public:
    ConfigExpert(int id, DataStore * data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::CONFIG) {
        config_ = Config::GetInstance();
    }

    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get Params
        assert(expert_obj.params.size() == 2);  // make sure input format
        string config_name = Tool::value_t2string(expert_obj.params[0]);
        bool enable = Tool::value_t2int(expert_obj.params[1]);

        string s = "Set config done";
        if (config_name == "caching") {
            config_->global_enable_caching = enable;
        } else if (config_name == "core_bind") {
            config_->global_enable_core_binding = enable;
        } else if (config_name == "expert_division") {
            if (config_->global_enable_workstealing) {
                config_->global_enable_expert_division = enable;
            }
        } else if (config_name == "step_reorder") {
            config_->global_enable_step_reorder = enable;
        } else if (config_name == "indexing") {
            config_->global_enable_indexing = enable;
        } else if (config_name == "stealing") {
            config_->global_enable_workstealing = enable;
            if (!enable) {
                config_->global_enable_expert_division = enable;
            }
        } else if (config_name == "data_size") {
            int i = Tool::value_t2int(expert_obj.params[1]);
            cout << i << endl;
            config_->max_data_size = i;
        } else {
            s = "Config name should be: ";
            s += "1. caching\n";
            s += "2. core_bind\n";
            s += "3. expert_division\n";
            s += "4. step_reorder\n";
            s += "5. indexing\n";
            s += "6. stealing\n";
            s += "7. data_size\n";
        }

        s += "\n";
        s += "Caching : " + string(config_->global_enable_caching ? "True" : "False") + "\n";
        s += "CoreBind : " + string(config_->global_enable_core_binding ? "True" : "False") + "\n";
        s += "ExpertDivision : " + string(config_->global_enable_expert_division ? "True" : "False") + "\n";
        s += "StepReorder : " + string(config_->global_enable_step_reorder ? "True" : "False") + "\n";
        s += "Indexing : " + string(config_->global_enable_indexing ? "True" : "False") + "\n";
        s += "Stealing : " + string(config_->global_enable_workstealing ? "True" : "False") + "\n";
        s += "Max Data Size: " + to_string(config_->max_data_size) + "\n";
        if (m.recver_nid == m.parent_nid) {
            value_t v;
            Tool::str2str(s, v);
            msg.data.emplace_back(history_t(), vector<value_t>{v});
        } else {
            msg.data.emplace_back(history_t(), vector<value_t>());
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

    // Config
    Config * config_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
};

#endif /* CONFIG_EXPERT_HPP_ */
