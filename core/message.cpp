/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/


#include "core/message.hpp"

ibinstream& operator<<(ibinstream& m, const Branch_Info& info) {
    m << info.node_id;
    m << info.thread_id;
    m << info.index;
    m << info.key;
    m << info.msg_id;
    m << info.msg_path;
    return m;
}

obinstream& operator>>(obinstream& m, Branch_Info& info) {
    m >> info.node_id;
    m >> info.thread_id;
    m >> info.index;
    m >> info.key;
    m >> info.msg_id;
    m >> info.msg_path;
    return m;
}

ibinstream& operator<<(ibinstream& m, const Meta& meta) {
    m << meta.qid;
    m << meta.step;
    m << meta.msg_type;
    m << meta.recver_nid;
    m << meta.recver_tid;
    m << meta.parent_nid;
    m << meta.parent_tid;
    m << meta.msg_path;
    m << meta.branch_infos;
    m << meta.experts;
    return m;
}

obinstream& operator>>(obinstream& m, Meta& meta) {
    m >> meta.qid;
    m >> meta.step;
    m >> meta.msg_type;
    m >> meta.recver_nid;
    m >> meta.recver_tid;
    m >> meta.parent_nid;
    m >> meta.parent_tid;
    m >> meta.msg_path;
    m >> meta.branch_infos;
    m >> meta.experts;
    return m;
}

std::string Meta::DebugString() const {
    std::stringstream ss;
    ss << "Meta: {";
    ss << "  qid: " << qid;
    ss << ", step: " << step;
    ss << ", recver node: " << recver_nid << ":" << recver_tid;
    ss << ", msg type: " << MsgType[static_cast<int>(msg_type)];
    ss << ", msg path: " << msg_path;
    ss << ", parent node: " << parent_nid;
    ss << ", paraent thread: " << parent_tid;
    if (msg_type == MSG_T::INIT) {
        ss << ", experts [";
        for (auto &expert : experts) {
            ss  << ExpertType[static_cast<int>(expert.expert_type)];
        }
        ss << "]";
    }
    ss << "}";
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Message& msg) {
    m << msg.meta;
    m << msg.data;
    m << msg.max_data_size;
    m << msg.data_size;
    return m;
}

obinstream& operator>>(obinstream& m, Message& msg) {
    m >> msg.meta;
    m >> msg.data;
    m >> msg.max_data_size;
    m >> msg.data_size;
    return m;
}

void Message::CreateInitMsg(uint64_t qid, int parent_node, int nodes_num, int recv_tid, const vector<Expert_Object>& experts, vector<Message>& vec) {
    // assign receiver thread id
    Meta m;
    m.qid = qid;
    m.step = 0;
    m.recver_nid = -1;                    // assigned in for loop
    m.recver_tid = recv_tid;
    m.parent_nid = parent_node;
    m.parent_tid = recv_tid;
    m.msg_type = MSG_T::INIT;
    m.msg_path = to_string(nodes_num);
    m.experts = experts;

    for (int i = 0; i < nodes_num; i++) {
        Message msg;
        msg.meta = m;
        msg.meta.recver_nid = i;
        vec.push_back(move(msg));
    }
}

void Message::CreateExitMsg(int nodes_num, vector<Message>& vec) {
    Meta m;
    m.qid = this->meta.qid;
    m.msg_type = MSG_T::EXIT;
    m.recver_tid = this->meta.parent_tid;

    for (int i = 0; i < nodes_num; i ++) {
        m.recver_nid = i;
        Message msg(m);
        vec.push_back(move(msg));
    }
}


void Message::CreateNextMsg(const vector<Expert_Object>& experts, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, CoreAffinity* core_affinity, vector<Message>& vec) {
    // timer::start_timer(meta.recver_tid + 4 * num_thread);
    Meta m = this->meta;
    m.step = experts[this->meta.step].next_expert;

    int count = vec.size();
    dispatch_data(m, experts, data, num_thread, data_store, core_affinity, vec);

    // set disptching path
    string num = to_string(vec.size() - count);
    if (meta.msg_path != "") {
        num = "\t" + num;
    }

    for (int i = count; i < vec.size(); i++) {
        vec[i].meta.msg_path += num;
    }
    // timer::stop_timer(meta.recver_tid + 4 * num_thread);
}

void Message::CreateBranchedMsg(const vector<Expert_Object>& experts, vector<int>& steps, int num_thread, DataStore* data_store, CoreAffinity * core_affinity, vector<Message>& vec) {
    // timer::start_timer(meta.recver_tid + 4 * num_thread);
    Meta m = this->meta;

    // append steps num into msg path
    int step_count = steps.size();

    // update branch info
    Branch_Info info;

    // parent msg end path
    string parent_path = "";

    int branch_depth = m.branch_infos.size() - 1;
    if (branch_depth >= 0) {
        // use parent branch's route
        info = m.branch_infos[branch_depth];
        parent_path = info.msg_path;

        // update current branch's end path with steps num
        info.msg_path += "\t" + to_string(step_count);
    } else {
        info.node_id = m.parent_nid;
        info.thread_id = m.parent_tid;
        info.key = -1;
        info.msg_path = to_string(step_count);
        info.msg_id = 0;
    }

    string tail = m.msg_path.substr(parent_path.size());
    // append "\t" to tail when first char is not "\t"
    if (tail != "" && tail.find("\t") != 0) {
        tail = "\t" + tail;
    }

    //    insert step_count into current msg path
    //     e.g.:
    //  "3\t2\t4" with parent path "3\t2", steps num 5
    //  info.msg_path => "3\t2\t5"
    //  tail = "4"
    //  msg_path = "3\t2\t5\t4"
    m.msg_path = info.msg_path + tail;

    // copy labeled data to each step
    for (int i = 0; i < steps.size(); i ++) {
        Meta step_meta = m;

        int step = steps[i];
        info.index = i + 1;
        step_meta.branch_infos.push_back(info);
        step_meta.step = step;

        auto temp = data;
        // dispatch data to msg vec
        int count = vec.size();
        dispatch_data(step_meta, experts, temp, num_thread, data_store, core_affinity, vec);

        // set msg_path for each branch
        for (int j = count; j < vec.size(); j++) {
            vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
        }
    }
    // timer::stop_timer(meta.recver_tid + 4 * num_thread);
}

void Message::CreateBranchedMsgWithHisLabel(const vector<Expert_Object>& experts, vector<int>& steps, uint64_t msg_id, int num_thread, DataStore* data_store, CoreAffinity * core_affinity, vector<Message>& vec) {
    // timer::start_timer(meta.recver_tid + 4 * num_thread);
    Meta m = this->meta;

    // update branch info
    Branch_Info info;
    info.node_id = m.recver_nid;
    info.thread_id = m.recver_tid;
    info.key = m.step;
    info.msg_path = m.msg_path;
    info.msg_id = msg_id;

    // label each data with unique id
    vector<pair<history_t, vector<value_t>>> labeled_data;
    int count = 0;
    for (auto pair : data) {
        if (pair.second.size() == 0) {
            labeled_data.push_back(move(pair));
        }
        for (auto& value : pair.second) {
            value_t v;
            Tool::str2int(to_string(count ++), v);
            history_t his = pair.first;
            his.emplace_back(m.step, move(v));
            vector<value_t> val_vec;
            val_vec.push_back(move(value));
            labeled_data.emplace_back(move(his), move(val_vec));
        }
    }
    // copy labeled data to each step
    for (int i = 0; i < steps.size(); i ++) {
        int step = steps[i];
        Meta step_meta = m;
        info.index = i + 1;
        step_meta.branch_infos.push_back(info);
        step_meta.step = step;

        vector<pair<history_t, vector<value_t>>> temp;
        if (i == steps.size() - 1) {
            temp = move(labeled_data);
        } else {
            temp = labeled_data;
        }

        // dispatch data to msg vec
        int count = vec.size();
        dispatch_data(step_meta, experts, temp, num_thread, data_store, core_affinity, vec);

        // set msg_path for each branch
        for (int j = count; j < vec.size(); j++) {
            vec[j].meta.msg_path += "\t" + to_string(vec.size() - count);
        }
    }
    // timer::stop_timer(meta.recver_tid + 4 * num_thread);
}

void Message::CreateFeedMsg(int key, int nodes_num, vector<value_t>& data, vector<Message>& vec) {
    Meta m;
    m.qid = this->meta.qid;
    m.recver_tid = this->meta.parent_tid;
    m.step = key;
    m.msg_type = MSG_T::FEED;

    auto temp_data = make_pair(history_t(), data);

    for (int i = 0; i < nodes_num; i++) {
        // skip parent node
        if (i == meta.parent_nid) {
            continue;
        }
        Message msg(m);
        msg.meta.recver_nid = i;
        msg.data.push_back(temp_data);
        vec.push_back(move(msg));
    }
}

void Message::dispatch_data(Meta& m, const vector<Expert_Object>& experts, vector<pair<history_t, vector<value_t>>>& data, int num_thread, DataStore* data_store, CoreAffinity * core_affinity, vector<Message>& vec) {
    Meta cm = m;
    bool route_assigned = update_route(m, experts);
    bool empty_to_barrier = update_collection_route(cm, experts);
    // <node id, data>
    map<int, vector<pair<history_t, vector<value_t>>>> id2data;
    // store history with empty data
    vector<pair<history_t, vector<value_t>>> empty_his;

    bool is_count = experts[m.step].expert_type == EXPERT_T::COUNT;

    // enable route mapping
    if (!route_assigned && experts[this->meta.step].send_remote) {
        for (auto& p : data) {
            map<int, vector<value_t>> id2value_t;
            if (p.second.size() == 0) {
                if (empty_to_barrier)
                    empty_his.push_back(move(p));
                continue;
            }

            // get node id
            for (auto& v : p.second) {
                int node = get_node_id(v, data_store);
                id2value_t[node].push_back(move(v));
            }

            // insert his/value pair to corresponding node
            for (auto& item : id2value_t) {
                id2data[item.first].emplace_back(p.first, move(item.second));
            }
        }

        // no data is added to next expert
        if (id2data.size() == 0 && empty_his.size() == 0) {
            empty_his.emplace_back(history_t(), vector<value_t>());
        }
    } else {
        for (auto& p : data) {
            if (p.second.size() == 0) {
                if (empty_to_barrier)
                    empty_his.push_back(move(p));
                continue;
            }

            if (is_count) {
                value_t v;
                Tool::str2int(to_string(p.second.size()), v);
                id2data[m.recver_nid].emplace_back(move(p.first), vector<value_t>{move(v)});
            } else {
                id2data[m.recver_nid].push_back(move(p));
            }
        }

        // no data is added to next expert
        if (id2data[m.recver_nid].size() == 0 && empty_his.size() == 0) {
            empty_his.emplace_back(history_t(), vector<value_t>());
        }
    }

    //++ added by DSY
    vector<pair<int, vector<pair<history_t, vector<value_t>>>>> id2data_vec;
    if(experts[this->meta.step].expert_type ==  EXPERT_T::UNTIL && this->meta.msg_type == MSG_T::SPAWN){
        for(auto& item: id2data){
            for(auto& data_pair: item.second){
                vector<pair<history_t, vector<value_t>>> point_vec;
                point_vec.push_back(move(data_pair));
                id2data_vec.push_back(move(make_pair(item.first, move(point_vec))));
            }
        }
    } else {
        for (auto& item: id2data){
            id2data_vec.push_back(move(make_pair(item.first, move(item.second))));
        }
    }
    //-- end DSY

    for (auto& item : id2data_vec) {    //+this line modified by DSY
        // insert data to msg
        do {
            Message msg(m);
            msg.max_data_size = this->max_data_size;
            msg.meta.recver_nid = item.first;
            // if(! route_assigned){
            msg.meta.recver_tid = core_affinity->GetThreadIdForExpert(experts[m.step].expert_type);
            // }

            //++added by DSY
            if (experts[this->meta.step].expert_type ==  EXPERT_T::UNTIL && this->meta.msg_type == MSG_T::SPAWN)
                msg.meta.his_index = item.second[0].first.size() - 1;   //save the index of shadow history and the last one is the data until insert
            //--end DSY

            msg.InsertData(item.second);
            vec.push_back(move(msg));
        } while ((item.second.size() != 0));    // Data no consumed
    }

    // send history with empty data
    if (empty_his.size() != 0) {
        // insert history to msg
        do {
            Message msg(cm);
            msg.max_data_size = this->max_data_size;
            msg.meta.recver_tid = core_affinity->GetThreadIdForExpert(experts[cm.step].expert_type);
            msg.InsertData(empty_his);
            vec.push_back(move(msg));
        } while ((empty_his.size() != 0));    // Data no consumed
    }
}

bool Message::update_route(Meta& m, const vector<Expert_Object>& experts) {
    int branch_depth = m.branch_infos.size() - 1;
    // update recver route & msg_type
    if (experts[m.step].IsBarrier()) {
        if (branch_depth >= 0) {
            // barrier expert in branch
            m.recver_nid = m.branch_infos[branch_depth].node_id;
            // m.recver_tid = m.branch_infos[branch_depth].thread_id;
        } else {
            // barrier expert in main query
            m.recver_nid = m.parent_nid;
            // m.recver_tid = m.parent_tid;
        }
        m.msg_type = MSG_T::BARRIER;
        return true;
    } else if (m.step <= this->meta.step) {
        // to branch parent
        assert(branch_depth >= 0);

        if (experts[m.step].expert_type == EXPERT_T::BRANCH || experts[m.step].expert_type == EXPERT_T::REPEAT) {
            // don't need to send msg back to parent branch step
            // go to next expert of parent
            m.step = experts[m.step].next_expert;
            m.branch_infos.pop_back();

            return update_route(m, experts);
        }
        //++start DSY
        else if (experts[this->meta.step].expert_type == EXPERT_T::UNTIL){
            m.msg_type = MSG_T::SPAWN;
            return false;
        }
        //--end DSY
        else {
            // aggregate labelled branch experts to parent machine
            m.recver_nid = m.branch_infos[branch_depth].node_id;
            m.recver_tid = m.branch_infos[branch_depth].thread_id;
            m.msg_type = MSG_T::BRANCH;
            return true;
        }
    } else {
        // normal expert, recver = sender
        m.msg_type = MSG_T::SPAWN;
        return false;
    }
}

bool Message::update_collection_route(Meta& m, const vector<Expert_Object>& experts) {
    bool to_barrier = false;
    // empty data should be send to:
    // 1. barrier expert, msg_type = BARRIER
    // 2. branch expert: which will broadcast empty data to barriers inside each branches for msg collection, msg_type = SPAWN
    // 3. labelled branch parent: which will collect branched msg with label, msg_type = BRANCH
    while (m.step < experts.size()) {
        if (experts[m.step].IsBarrier()) {
            // to barrier
            to_barrier = true;
            break;
        } else if (experts[m.step].expert_type == EXPERT_T::BRANCH || experts[m.step].expert_type == EXPERT_T::REPEAT) {
            if (m.step <= this->meta.step) {
                // to branch parent, pop back one branch info
                // as barrier expert is not founded in sub branch, continue to search
                m.branch_infos.pop_back();
            } else {
                // to branch expert for the first time, should broadcast empty data to each branches
                break;
            }
        } else if (m.step <= this->meta.step) {
            // to labelled branch parent
            break;
        }

        m.step = experts[m.step].next_expert;
    }

    update_route(m, experts);

    return to_barrier;
}

int Message::get_node_id(value_t & v, DataStore* data_store) {
    int type = v.type;
    if (type == 1) {
        vid_t v_id(Tool::value_t2int(v));
        return data_store->GetMachineIdForVertex(v_id);
    } else if (type == 5) {
        eid_t e_id;
        uint2eid_t(Tool::value_t2uint64_t(v), e_id);
        return data_store->GetMachineIdForEdge(e_id);
    } else {
        cout << "Wrong Type when getting node id" << type << endl;
        return -1;
    }
}

bool Message::InsertData(pair<history_t, vector<value_t>>& pair) {
    size_t space = max_data_size - data_size;
    size_t his_size = MemSize(pair.first) + sizeof(size_t);

    if (pair.second.size() == 0) {
        data.push_back(pair);
        data_size += his_size;
        return true;
    }

    // check if able to add history
    // no space for history
    if (his_size >= space) {
        return false;
    }

    size_t in_size = his_size;
    auto itr = pair.second.begin();
    for (; itr != pair.second.end(); itr ++) {
        size_t s = MemSize(*itr);
        if (s + in_size <= space) {
            in_size += s;
        } else {
            break;
        }
    }

    // move data
    if (in_size != his_size) {
        vector<value_t> temp;
        std::move(pair.second.begin(), itr, std::back_inserter(temp));
        pair.second.erase(pair.second.begin(), itr);
        data.emplace_back(pair.first, move(temp));
        data_size += in_size;
    }

    return pair.second.size() == 0;
}

void Message::InsertData(vector<pair<history_t, vector<value_t>>>& vec) {
    auto itr = vec.begin();
    for (; itr != vec.end(); itr++) {
        if (!InsertData(*itr)) {
            break;
        }
    }
    vec.erase(vec.begin(), itr);
}

std::string Message::DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data)
          ss << " data_size=" << d.second.size();
    }
    return ss.str();
}

bool operator == (const history_t& l, const history_t& r) {
    if (l.size() != r.size()) {
        return false;
    }

    // history keys are in ascending order
    // so simply match kv pair one by one
    for (int i = 0; i < l.size(); i++) {
        if (l[i] != r[i]) {
            return false;
        }
    }
    return true;
}

size_t MemSize(const int& i) {
    return sizeof(int);
}

size_t MemSize(const char& c) {
    return sizeof(char);
}

size_t MemSize(const value_t& data) {
    size_t s = sizeof(uint8_t);
    s += MemSize(data.content);
    return s;
}
