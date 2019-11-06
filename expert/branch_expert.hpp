/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include "expert/abstract_expert.hpp"

// Branch Expert
// Copy incoming data to sub branches
class BranchExpert : public AbstractExpert {
 public:
    BranchExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox* mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox) {}
    void process(const vector<Expert_Object> & experts,  Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (msg.meta.msg_type == MSG_T::SPAWN) {
            vector<int> step_vec;
            get_steps(experts[msg.meta.step], step_vec);

            vector<Message> msg_vec;
            msg.CreateBranchedMsg(experts, step_vec, num_thread_, data_store_, core_affinity_, msg_vec);

            for (auto& m : msg_vec) {
                mailbox_->Send(tid, m);
            }
        } else {
            cout << "Unexpected msg type in branch expert." << endl;
            exit(-1);
        }
    }

 private:
    int num_thread_;
    AbstractMailbox* mailbox_;

    void get_steps(const Expert_Object & expert, vector<int>& steps) {
        vector<value_t> params = expert.params;
        assert(params.size() >= 1);
        for (int i = 0; i < params.size(); i++) {
            steps.push_back(Tool::value_t2int(params[i]));
        }
    }
};
