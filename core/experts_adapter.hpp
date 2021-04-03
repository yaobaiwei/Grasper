/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Nick Fang (jcfang6@cse.cuhk.edu.hk)
         Changji Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef EXPERTS_ADAPTER_HPP_
#define EXPERTS_ADAPTER_HPP_

#include <map>
#include <vector>
#include <atomic>
#include <thread>
#include <tbb/concurrent_hash_map.h>

#include "expert/abstract_expert.hpp"
#include "expert/as_expert.hpp"
#include "expert/barrier_expert.hpp"
#include "expert/branch_expert.hpp"
#include "expert/config_expert.hpp"
#include "expert/has_expert.hpp"
#include "expert/has_label_expert.hpp"
#include "expert/init_expert.hpp"
#include "expert/index_expert.hpp"
#include "expert/is_expert.hpp"
#include "expert/key_expert.hpp"
#include "expert/label_expert.hpp"
#include "expert/labelled_branch_expert.hpp"
#include "expert/properties_expert.hpp"
#include "expert/select_expert.hpp"
#include "expert/traversal_expert.hpp"
#include "expert/values_expert.hpp"
#include "expert/where_expert.hpp"
#include "expert/repeat_expert.hpp"

#include "base/node.hpp"
#include "base/type.hpp"
#include "base/core_affinity.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "core/index_store.hpp"
#include "storage/data_store.hpp"
#include "utils/config.hpp"
#include "utils/timer.hpp"

#include <omp.h>

#include "utils/tid_mapper.hpp"

using namespace std;

class ExpertAdapter {
 public:
    ExpertAdapter(Node & node, Result_Collector * rc, AbstractMailbox * mailbox, DataStore* data_store, CoreAffinity* core_affinity, IndexStore * index_store) : node_(node), rc_(rc), mailbox_(mailbox), data_store_(data_store), core_affinity_(core_affinity), index_store_(index_store) {
        config_ = Config::GetInstance();
        num_thread_ = config_->global_num_threads;
        times_.resize(num_thread_, 0);
    }

    void Init() {
        int id = 0;
        experts_[EXPERT_T::AGGREGATE] = unique_ptr<AbstractExpert>(new AggregateExpert(id ++, data_store_, node_.get_local_size(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::AS] = unique_ptr<AbstractExpert>(new AsExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::BRANCH] = unique_ptr<AbstractExpert>(new BranchExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::BRANCHFILTER] = unique_ptr<AbstractExpert>(new BranchFilterExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_, &id_allocator_));
        experts_[EXPERT_T::CAP] = unique_ptr<AbstractExpert>(new CapExpert(id ++, data_store_ , num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::CONFIG] = unique_ptr<AbstractExpert>(new ConfigExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::COUNT] = unique_ptr<AbstractExpert>(new CountExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::DEDUP] = unique_ptr<AbstractExpert>(new DedupExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::END] = unique_ptr<AbstractExpert>(new EndExpert(id ++, data_store_, node_.get_local_size(), rc_, mailbox_, core_affinity_));
        experts_[EXPERT_T::GROUP] = unique_ptr<AbstractExpert>(new GroupExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::HAS] = unique_ptr<AbstractExpert>(new HasExpert(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::HASLABEL] = unique_ptr<AbstractExpert>(new HasLabelExpert(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::INIT] = unique_ptr<AbstractExpert>(new InitExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_, index_store_, node_.get_local_size()));
        experts_[EXPERT_T::INDEX] = unique_ptr<AbstractExpert>(new IndexExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_, index_store_));
        experts_[EXPERT_T::IS] = unique_ptr<AbstractExpert>(new IsExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::KEY] = unique_ptr<AbstractExpert>(new KeyExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::LABEL] = unique_ptr<AbstractExpert>(new LabelExpert(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::MATH] = unique_ptr<AbstractExpert>(new MathExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::ORDER] = unique_ptr<AbstractExpert>(new OrderExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::PROPERTY] = unique_ptr<AbstractExpert>(new PropertiesExpert(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::RANGE] = unique_ptr<AbstractExpert>(new RangeExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::COIN] = unique_ptr<AbstractExpert>(new CoinExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::REPEAT] = unique_ptr<AbstractExpert>(new RepeatExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::SELECT] = unique_ptr<AbstractExpert>(new SelectExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::TRAVERSAL] = unique_ptr<AbstractExpert>(new TraversalExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::VALUES] = unique_ptr<AbstractExpert>(new ValuesExpert(id ++, data_store_, node_.get_local_rank(), num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::WHERE] = unique_ptr<AbstractExpert>(new WhereExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        experts_[EXPERT_T::UNTIL] = unique_ptr<AbstractExpert>(new UntilExpert(id ++, data_store_, num_thread_, mailbox_, core_affinity_));
        // TODO(future) add more

        timer::init_timers((experts_.size() + timer_offset) * num_thread_);
    }

    void Start() {
        Init();
        TidMapper* tmp_tid_mapper_ptr = TidMapper::GetInstance();  // in case of initial in parallel region

        for (int i = 0; i < num_thread_; ++i)
            thread_pool_.emplace_back(&ExpertAdapter::ThreadExecutor, this, i);
    }

    void Stop() {
        for (auto &thread : thread_pool_)
            thread.join();
    }

    void execute(int tid, Message & msg) {
        Meta & m = msg.meta;
        if (m.msg_type == MSG_T::INIT) {
            // acquire write lock for insert
            accessor ac;
            msg_logic_table_.insert(ac, m.qid);
            ac->second = move(m.experts);
        } else if (m.msg_type == MSG_T::FEED) {
            assert(msg.data.size() == 1);
            agg_t agg_key(m.qid, m.step);
            data_store_->InsertAggData(agg_key, msg.data[0].second);

            return;
        } else if (m.msg_type == MSG_T::EXIT) {
            const_accessor ac;
            msg_logic_table_.find(ac, m.qid);

            // erase aggregate result
            int i = 0;
            for (auto& act : ac->second) {
                if (act.expert_type == EXPERT_T::AGGREGATE) {
                    agg_t agg_key(m.qid, i);
                    data_store_->DeleteAggData(agg_key);
                }
                i++;
            }

            // earse only after query with qid is done
            msg_logic_table_.erase(ac);

            return;
        }

        const_accessor ac;
        // qid not found
        if (!msg_logic_table_.find(ac, m.qid)) {
            // throw msg to the same thread as init msg
            msg.meta.recver_tid = msg.meta.parent_tid;
            mailbox_->Send(tid, msg);
            return;
        }

        int current_step;
        do {
            current_step = msg.meta.step;
            EXPERT_T next_expert = ac->second[current_step].expert_type;
            // int offset = (experts_[next_expert]->GetExpertId() + timer_offset) * num_thread_;

            // timer::start_timer(tid + offset);
            experts_[next_expert]->process(ac->second, msg);
            // timer::stop_timer(tid + offset);
        } while (current_step != msg.meta.step);    // process next expert directly if step is modified
    }

    void ThreadExecutor(int tid) {
        TidMapper::GetInstance()->Register(tid);
        // bind thread to core
        if (config_->global_enable_core_binding) {
            core_affinity_->BindToCore(tid);
        }

        vector<int> steal_list;
        core_affinity_->GetStealList(tid, steal_list);

        while (true) {
            // timer::start_timer(tid + 2 * num_thread_);
            mailbox_->Sweep(tid);

            Message recv_msg;
            // timer::start_timer(tid + 3 * num_thread_);
            bool success = mailbox_->TryRecv(tid, recv_msg);
            times_[tid] = timer::get_usec();
            if (success) {
                // timer::stop_timer(tid + 3 * num_thread_);
                execute(tid, recv_msg);
                times_[tid] = timer::get_usec();
                // timer::stop_timer(tid + 2 * num_thread_);
            } else {
                if (!config_->global_enable_workstealing)
                    continue;

                if (steal_list.size() == 0) {  // num_thread_ < 6
                    success = mailbox_->TryRecv((tid + 1) % num_thread_, recv_msg);
                    if (success) {
                        // timer::stop_timer(tid + 3 * num_thread_);
                        execute(tid, recv_msg);
                        // timer::stop_timer(tid + 2 * num_thread_);
                    }
                } else {  // num_thread_ >= 6
                    for (auto itr = steal_list.begin(); itr != steal_list.end(); itr++) {
                        if (times_[tid] < times_[*itr] + STEALTIMEOUT)
                                continue;

                        // timer::start_timer(tid + 2 * num_thread_);
                        // timer::start_timer(tid + 3 * num_thread_);
                        success = mailbox_->TryRecv(*itr, recv_msg);
                        if (success) {
                            // timer::stop_timer(tid + 3 * num_thread_);
                            execute(tid, recv_msg);
                            // timer::stop_timer(tid + 2 * num_thread_);
                            break;
                        }
                    }
                }
                times_[tid] = timer::get_usec();
            }
        }
    }

 private:
    AbstractMailbox * mailbox_;
    Result_Collector * rc_;
    DataStore * data_store_;
    IndexStore * index_store_;
    Config * config_;
    CoreAffinity * core_affinity_;
    msg_id_alloc id_allocator_;
    Node node_;

    // Experts pool <expert_type, [experts]>
    map<EXPERT_T, unique_ptr<AbstractExpert>> experts_;

    // global map to record the vec<expert_obj> of query
    // avoid repeatedly transfer vec<expert_obj> for message
    tbb::concurrent_hash_map<uint64_t, vector<Expert_Object>> msg_logic_table_;
    typedef tbb::concurrent_hash_map<uint64_t, vector<Expert_Object>>::accessor accessor;
    typedef tbb::concurrent_hash_map<uint64_t, vector<Expert_Object>>::const_accessor const_accessor;
    // Thread pool
    vector<thread> thread_pool_;

    // clocks
    vector<uint64_t> times_;
    int num_thread_;

    // 5 more timers for total, recv , send, serialization, create msg
    const static int timer_offset = 5;
    const static uint64_t STEALTIMEOUT = 1000;
};


#endif /* EXPERTS_ADAPTER_HPP_ */
