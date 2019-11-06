/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
         Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef INIT_EXPERT_HPP_
#define INIT_EXPERT_HPP_

#include <iostream>
#include <string>
#include "glog/logging.h"

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "core/index_store.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"


#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

using namespace std;

class InitExpert : public AbstractExpert {
 public:
    InitExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity, IndexStore * index_store, int num_nodes) : AbstractExpert(id, data_store, core_affinity), index_store_(index_store), num_thread_(num_thread), mailbox_(mailbox), num_nodes_(num_nodes), type_(EXPERT_T::INIT), is_ready_(false) {
        config_ = Config::GetInstance();

        // read snapshot here
        // write snapshot @ InitData

        MPISnapshot* snapshot = MPISnapshot::GetInstance();

        int snapshot_read_cnt = 0;

        snapshot_read_cnt += ((snapshot->ReadData("init_expert_vtx_msgs", vtx_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt += ((snapshot->ReadData("init_expert_edge_msgs", edge_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt += ((snapshot->ReadData("init_expert_vtx_count_msgs", vtx_count_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        snapshot_read_cnt += ((snapshot->ReadData("init_expert_edge_count_msgs", edge_count_msgs, ReadMailboxMsgImpl)) ? 1 : 0);
        if (snapshot_read_cnt == 4) {
            is_ready_ = true;
        } else {
            // atomic, all fail
            vtx_msgs.resize(0);
            edge_msgs.resize(0);
            vtx_count_msgs.resize(0);
            edge_count_msgs.resize(0);
        }
    }

    virtual ~InitExpert() {}

    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        if (expert_objs[msg.meta.step].params.size() == 1) {
            InitWithoutIndex(tid, expert_objs, msg);
        } else {
            InitWithIndex(tid, expert_objs, msg);
        }
    }

 private:
    // Number of threads
    int num_thread_;
    int num_nodes_;
    bool is_ready_;

    Config * config_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Pointer of index store
    IndexStore * index_store_;

    // Ensure only one thread ever runs the expert
    std::mutex thread_mutex_;

    // Cached data
    vector<Message> vtx_msgs;
    vector<Message> edge_msgs;

    // msg for count expert
    vector<Message> vtx_count_msgs;
    vector<Message> edge_count_msgs;

    void InitData() {
        if (is_ready_) {
            return;
        }
        MPISnapshot* snapshot = MPISnapshot::GetInstance();

        // convert id to msg
        Meta m;
        m.step = 1;
        m.msg_path = to_string(num_nodes_);

        uint64_t start_t = timer::get_usec();

        InitVtxData(m);
        snapshot->WriteData("init_expert_vtx_msgs", vtx_msgs, WriteMailboxMsgImpl);
        snapshot->WriteData("init_expert_vtx_count_msgs", vtx_count_msgs, WriteMailboxMsgImpl);
        uint64_t end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initV_Msg in init_expert" << endl;

        start_t = timer::get_usec();
        InitEdgeData(m);
        snapshot->WriteData("init_expert_edge_msgs", edge_msgs, WriteMailboxMsgImpl);
        snapshot->WriteData("init_expert_edge_count_msgs", edge_count_msgs, WriteMailboxMsgImpl);
        end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - start_t) / 1000 << " ms for initE_Msg in init_expert" << endl;
    }

    void InitVtxData(Meta& m) {
        vector<vid_t> vid_list;
        data_store_->GetAllVertices(vid_list);
        uint64_t count = vid_list.size();

        vector<pair<history_t, vector<value_t>>> data;
        data.emplace_back(history_t(), vector<value_t>());
        data[0].second.reserve(count);
        for (auto& vid : vid_list) {
            value_t v;
            Tool::str2int(to_string(vid.value()), v);
            data[0].second.push_back(v);
        }
        vector<vid_t>().swap(vid_list);

        vtx_msgs.clear();
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            vtx_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(vtx_msgs.size());
        for (auto & msg_ : vtx_msgs) {
            msg_.meta.msg_path += num;
        }

        Message vtx_count_msg(m);
        vtx_count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        vtx_count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        vtx_count_msgs.push_back(move(vtx_count_msg));
    }

    void InitEdgeData(Meta& m) {
        vector<eid_t> eid_list;
        data_store_->GetAllEdges(eid_list);
        uint64_t count = eid_list.size();

        vector<pair<history_t, vector<value_t>>> data;
        data.emplace_back(history_t(), vector<value_t>());
        data[0].second.reserve(count);
        for (auto& eid : eid_list) {
            value_t v;
            Tool::str2uint64_t(to_string(eid.value()), v);
            data[0].second.push_back(v);
        }
        vector<eid_t>().swap(eid_list);

        edge_msgs.clear();
        do {
            Message msg(m);
            msg.max_data_size = config_->max_data_size;
            msg.InsertData(data);
            edge_msgs.push_back(move(msg));
        } while ((data.size() != 0));

        string num = "\t" + to_string(edge_msgs.size());
        for (auto & msg_ : edge_msgs) {
            msg_.meta.msg_path += num;
        }

        Message edge_count_msg(m);
        edge_count_msg.max_data_size = config_->max_data_size;
        value_t v;
        Tool::str2int(to_string(count), v);
        edge_count_msg.data.emplace_back(history_t(), vector<value_t>{v});
        edge_count_msgs.push_back(move(edge_count_msg));
    }

    void InitWithIndex(int tid, const vector<Expert_Object> & expert_objs, Message & msg) {
        Meta m = msg.meta;
        const Expert_Object& expert_obj = expert_objs[m.step];

        // store all predicate
        vector<pair<int, PredicateValue>> pred_chain;

        // Get Params
        assert(expert_obj.params.size() > 1 && (expert_obj.params.size() - 1) % 3 == 0);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        int numParamsGroup = (expert_obj.params.size() - 1) / 3;  // number of groups of params

        // Create predicate chain for this query
        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3 + 1;
            // Get predicate params
            int pid = Tool::value_t2int(expert_obj.params.at(pos));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos + 1));
            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 2), pred_params);
            pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        }

        msg.max_data_size = config_->max_data_size;
        msg.data.clear();
        msg.data.emplace_back(history_t(), vector<value_t>());
        index_store_->GetElements(inType, pred_chain, msg.data[0].second);

        vector<Message> vec;
        msg.CreateNextMsg(expert_objs, msg.data, num_thread_, data_store_, core_affinity_, vec);

        // Send Message
        for (auto& msg_ : vec) {
            mailbox_->Send(tid, msg_);
        }
    }

    void InitWithoutIndex(int tid, const vector<Expert_Object> & expert_objs, Message & msg) {
        if (!is_ready_) {
            if (thread_mutex_.try_lock()) {
                InitData();
                is_ready_ = true;
                thread_mutex_.unlock();
            } else {
                // wait until InitMsg finished
                while (!thread_mutex_.try_lock()) {}
                thread_mutex_.unlock();
            }
        }
        Meta m = msg.meta;
        const Expert_Object& expert_obj = expert_objs[m.step];

        // Get init element type
        Element_T inType = (Element_T)Tool::value_t2int(expert_obj.params.at(0));
        vector<Message>* msg_vec;

        if (expert_objs[m.step + 1].expert_type == EXPERT_T::COUNT) {
            if (inType == Element_T::VERTEX) {
                msg_vec = &vtx_count_msgs;
            } else if (inType == Element_T::EDGE) {
                msg_vec = &edge_count_msgs;
            }
        } else {
            if (inType == Element_T::VERTEX) {
                msg_vec = &vtx_msgs;
            } else if (inType == Element_T::EDGE) {
                msg_vec = &edge_msgs;
            }
        }


        // update meta
        m.step++;
        m.msg_type = MSG_T::SPAWN;
        if (expert_objs[m.step].IsBarrier()) {
            m.msg_type = MSG_T::BARRIER;
            m.recver_nid = m.parent_nid;
        }

        thread_mutex_.lock();
        // Send Message
        for (auto& msg : *msg_vec) {
            msg.meta.qid = m.qid;
            msg.meta.msg_type = m.msg_type;
            msg.meta.recver_nid = m.recver_nid;
            msg.meta.recver_tid = core_affinity_->GetThreadIdForExpert(expert_objs[m.step].expert_type);
            msg.meta.parent_nid = m.parent_nid;
            msg.meta.parent_tid = m.parent_tid;
            mailbox_->Send(tid, msg);
        }
        thread_mutex_.unlock();
    }
};

#endif /* INIT_EXPERT_HPP_ */
