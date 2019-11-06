/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#ifndef WORKER_HPP_
#define WORKER_HPP_

#include "third_party/zmq.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/throughput_monitor.hpp"

#include "core/message.hpp"
#include "core/parser.hpp"
#include "core/buffer.hpp"
#include "core/id_mapper.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/experts_adapter.hpp"
#include "core/index_store.hpp"
#include "core/progress_monitor.hpp"
#include "core/result_collector.hpp"
#include "core/logical_plan.hpp"

#include "storage/data_store.hpp"
#include "storage/mpi_snapshot.hpp"

class Worker {
 public:
    Worker(Node & my_node, vector<Node> & workers): my_node_(my_node), workers_(workers) {
        config_ = Config::GetInstance();
        num_query = 0;
        is_emu_mode_ = false;

        index_store_ = NULL;
        parser_ = NULL;
        receiver_ = NULL;
        worker_listener_ = NULL;
        thpt_monitor_ = NULL;
        rc_ = NULL;
    }

    ~Worker() {
        for (int i = 0; i < senders_.size(); i++) {
            delete senders_[i];
        }

        delete receiver_;
        delete worker_listener_;
        delete parser_;
        delete index_store_;
        delete rc_;
    }

    void Init() {
        // init the necessary components
        index_store_ = new IndexStore();
        parser_ = new Parser(index_store_);
        receiver_ = new zmq::socket_t(context_, ZMQ_PULL);
        worker_listener_ = new zmq::socket_t(context_, ZMQ_REP);
        thpt_monitor_ = new Throughput_Monitor();
        rc_ = new Result_Collector;

        char addr[64];
        char w_addr[64];
        sprintf(addr, "tcp://*:%d", my_node_.tcp_port);
        sprintf(w_addr, "tcp://*:%d", my_node_.tcp_port + config_->global_num_threads + 1);

        receiver_->bind(addr);
        worker_listener_->bind(w_addr);

        // connect to other workers for commun
        for (int i = 0; i < my_node_.get_local_size(); i++) {
            if (i != my_node_.get_local_rank()) {
                zmq::socket_t * sender = new zmq::socket_t(context_, ZMQ_PUSH);
                sprintf(addr, "tcp://%s:%d", workers_[i].hostname.c_str(), workers_[i].tcp_port);
                sender->connect(addr);
                senders_.push_back(sender);
            }
        }
    }

    // evaluate the throughput
    void RunEMU(string& cmd, string& client_host) {
        string emu_host = "EMUHOST";
        qid_t qid;
        bool is_main_worker = false;

        // in here, the client send emu request to one specific worker for throughput test,
        // this worker will enter into the following branch for:
        // notify other workers for throughput test with hostname:EMUHOST
        if (client_host != emu_host) {
            is_main_worker = true;
            ibinstream in;
            in << emu_host;
            in << cmd;

            for (int i = 0; i < senders_.size(); i++) {
                zmq::message_t msg(in.size());
                memcpy((void *)msg.data(), in.get_buf(), in.size());
                senders_[i]->send(msg);
            }

            qid = qid_t(my_node_.get_local_rank(), ++num_query);
            rc_->Register(qid.value(), client_host);
        }

        cmd = cmd.substr(3);
        string file_name = Tool::trim(cmd, " ");
        ifstream ifs(file_name);
        if (!ifs.good()) {
            cout << "file not found: " << file_name << endl;
            return;
        }
        uint64_t test_time, parrellfexpert, ratio;
        ifs >> test_time >> parrellfexpert;

        // transfer sec to usec
        test_time *= 1000000;
        int n_type = 0;
        ifs >> n_type;
        assert(n_type > 0);

        vector<string> queries;
        vector<pair<Element_T, int>> query_infos;
        vector<int> ratios;
        for (int i = 0; i < n_type; i++) {
            string query;
            string property_key;
            int ratio;
            ifs >> query >> property_key >> ratio;
            ratios.push_back(ratio);
            Element_T e_type;
            if (query.find("g.V()") == 0) {
                e_type = Element_T::VERTEX;
            } else {
                e_type = Element_T::EDGE;
            }

            int pid = parser_->GetPid(e_type, property_key);
            if (pid == -1) {
                if (is_main_worker) {
                    value_t v;
                    Tool::str2str("Emu Mode Error", v);
                    vector<value_t> result = {v};
                    thpt_monitor_->RecordStart(qid.value());
                    rc_->InsertResult(qid.value(), result);
                }
                return;
            }

            queries.push_back(query);
            query_infos.emplace_back(e_type, pid);
        }

        // wait until all previous query done
        while (thpt_monitor_->WorksRemaining() != 0) {}

        is_emu_mode_ = true;
        srand(time(NULL));
        regex match("\\$RAND");

        vector<string> commited_queries;

        // suppose one query will be generated within 10 us
        commited_queries.reserve(test_time / 10);
        // wait for all nodes
        worker_barrier(my_node_);

        thpt_monitor_->StartEmu();
        uint64_t start = timer::get_usec();
        while (timer::get_usec() - start < test_time) {
            if (thpt_monitor_->WorksRemaining() > parrellfexpert) {
                continue;
            }
            // pick random query type
            int query_type = mymath::get_distribution(rand(), ratios);

            // get query info
            string query_temp = queries[query_type];
            Element_T element_type = query_infos[query_type].first;
            int pid = query_infos[query_type].second;

            // generate random value
            string rand_value;
            if (!index_store_->GetRandomValue(element_type, pid, rand(), rand_value)) {
                cout << "Not values for property " << pid << " stored in node " << my_node_.get_local_rank() << endl;
                break;
            }

            query_temp = regex_replace(query_temp, match, rand_value);
            // run query
            ParseAndSendQuery(query_temp, emu_host, query_type);
            commited_queries.push_back(move(query_temp));
            if (is_main_worker) {
                thpt_monitor_->PrintThroughput();
            }
        }
        thpt_monitor_->StopEmu();

        while (thpt_monitor_->WorksRemaining() != 0) {
            cout << "Node " << my_node_.get_local_rank() << " still has " << thpt_monitor_->WorksRemaining() << "queries" << endl;
            usleep(500000);
        }

        double thpt = thpt_monitor_->GetThroughput();
        map<int, vector<uint64_t>> latency_map;
        thpt_monitor_->GetLatencyMap(latency_map);

        if (my_node_.get_local_rank() == 0) {
            vector<double> thpt_list;
            vector<map<int, vector<uint64_t>>> map_list;
            thpt_list.resize(my_node_.get_local_size());
            map_list.resize(my_node_.get_local_size());
            master_gather(my_node_, false, thpt_list);
            master_gather(my_node_, false, map_list);


            cout << "#################################" << endl;
            cout << "Emulator result with " << n_type << " classes and parrell fexpert: " << parrellfexpert << endl;
            cout << "Throughput of node 0: " << thpt << " K queries/sec" << endl;
            for (int i = 1; i < my_node_.get_local_size(); i++) {
                thpt += thpt_list[i];
                cout << "Throughput of node " << i << ": " << thpt_list[i] << " K queries/sec" << endl;
            }
            cout << "Total Throughput : " << thpt << " K queries/sec" << endl;
            cout << "#################################" << endl;

            map_list[0] = move(latency_map);
            thpt_monitor_->PrintCDF(map_list);
        } else {
            slave_gather(my_node_, false, thpt);
            slave_gather(my_node_, false, latency_map);
        }

        // output all commited_queries to file
        string ofname = "Thpt_Queries_" + to_string(my_node_.get_local_rank()) + ".txt";
        ofstream ofs(ofname, ofstream::out);
        ofs << commited_queries.size() << endl;
        for (auto& query : commited_queries) {
            ofs << query << endl;
        }

        is_emu_mode_ = false;

        // send reply to client
        if (is_main_worker) {
            value_t v;
            Tool::str2str("Run Emu Mode Done", v);
            vector<value_t> result = {v};
            thpt_monitor_->SetEmuStartTime(qid.value());
            rc_->InsertResult(qid.value(), result);
        }
    }

    // parse the query string to vector<expert_obj> as the logical query plan
    void ParseAndSendQuery(string query, string client_host, int query_type = -1) {
        qid_t qid(my_node_.get_local_rank(), ++num_query);
        thpt_monitor_->RecordStart(qid.value(), query_type);

        rc_->Register(qid.value(), client_host);

        vector<Expert_Object> experts;
        string error_msg;
        bool success = parser_->Parse(query, experts, error_msg);

        if (success) {
            LogicPlan plan(qid);
            plan.Feed(experts);
            queue_.Push(plan);
        } else {
            value_t v;
            Tool::str2str(error_msg, v);
            vector<value_t> vec = {v};
            rc_->InsertResult(qid.value(), vec);
        }
    }

    // a listener thread, to listen requests from other workers when RDMA is disabled (using TCP instead)
    // then, generate the reply by locally accessing the objects(V/E) in datastore
    void TCPListener(DataStore * datastore) {
        while (1) {
            zmq::message_t request;
            worker_listener_->recv(&request);

            char* buf = new char[request.size()];
            memcpy(buf, (char *)request.data(), request.size());
            obinstream um(buf, request.size());
            ibinstream m;

            uint64_t id;
            int elem_type;
            value_t val;

            um >> id;
            um >> elem_type;

            switch (elem_type) {
                case Element_T::VERTEX:
                    datastore->AccessVProperty(id, val);
                    break;
                case Element_T::EDGE:
                    datastore->AccessEProperty(id, val);
                    break;
                default:
                    cout << my_node_.hostname <<"-[ERROR]: Wrong Request Type!" << endl;
                    exit(1);
            }

            m << val;
            zmq::message_t msg(m.size());
            memcpy((void *)msg.data(), m.get_buf(), m.size());
            worker_listener_->send(msg);
        }
    }

    // a listener thread, receive request from clients
    void RecvRequest() {
        while (1) {
            zmq::message_t request;
            receiver_->recv(&request);

            char* buf = new char[request.size()];
            memcpy(buf, (char *)request.data(), request.size());
            obinstream um(buf, request.size());

            string client_host;
            string query;

            um >> client_host;  // get the client hostname for returning results.
            um >> query;
            cout << "worker_node" << my_node_.get_local_rank() << " gets one QUERY: \"" << query <<"\" from host " << client_host << endl;

            if (query.find("emu") == 0) {
                RunEMU(query, client_host);
            } else {
                ParseAndSendQuery(query, client_host);
            }
        }
    }

    void SendQueryMsg(AbstractMailbox * mailbox, CoreAffinity * core_affinity) {
        while (1) {
            LogicPlan plan;
            queue_.WaitAndPop(plan);

            vector<Message> msgs;
            Message::CreateInitMsg(plan.qid.value(), my_node_.get_local_rank(), my_node_.get_local_size(), core_affinity->GetThreadIdForExpert(EXPERT_T::INIT), plan.experts, msgs);
            for (int i = 0 ; i < my_node_.get_local_size(); i++) {
                mailbox->Send(config_->global_num_threads, msgs[i]);
            }
            mailbox->Sweep(config_->global_num_threads);
        }
    }

    void Start() {
        // initial MPIUniqueNamer
        MPIUniqueNamer* p = MPIUniqueNamer::GetInstance(my_node_.local_comm);
        p->AppendHash(config_->HDFS_INDEX_PATH +
                      config_->HDFS_VTX_SUBFOLDER +
                      config_->HDFS_VP_SUBFOLDER +
                      config_->HDFS_EP_SUBFOLDER +
                      p->ultos(config_->key_value_ratio_in_rdma) +
                      p->ultos(config_->global_vertex_property_kv_sz_gb) +
                      p->ultos(config_->global_edge_property_kv_sz_gb));

        // initial MPISnapshot
        MPISnapshot* snapshot = MPISnapshot::GetInstance(config_->SNAPSHOT_PATH);

        // you can use this if you want to overwrite snapshot
        // snapshot->DisableRead();
        // you can use this if you are testing on a tiny dataset to avoid write snapshot
        // snapshot->DisableWrite();

        // ===================prepare stage=================
        NaiveIdMapper * id_mapper = new NaiveIdMapper(my_node_);

        // init core affinity
        CoreAffinity * core_affinity = new CoreAffinity();
        core_affinity->Init();
        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> Init Core Affinity" << endl;

        // set the in-memory layout for RDMA buf
        Buffer * buf = new Buffer(my_node_);
        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> Register RDMA MEM, SIZE = " << buf->GetBufSize() << endl;

        AbstractMailbox * mailbox;
        if (config_->global_use_rdma)
            mailbox = new RdmaMailbox(my_node_, buf);
        else
            mailbox = new TCPMailbox(my_node_);
        mailbox->Init(workers_);

        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> Mailbox->Init()" << endl;

        DataStore * datastore = new DataStore(my_node_, id_mapper, buf);
        DataStore::StaticInstanceP(datastore);
        datastore->Init(workers_);

        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> DataStore->Init()" << endl;

        // read snapshot area
        datastore->ReadSnapshot();

        datastore->LoadDataFromHDFS();
        worker_barrier(my_node_);

        // =======data shuffle==========
        datastore->Shuffle();
        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> DataStore->Shuffle()" << endl;
        // =======data shuffle==========

        datastore->DataConverter();
        worker_barrier(my_node_);

        cout << "Worker" << my_node_.get_local_rank()  << ": DONE -> Datastore->DataConverter()" << endl;

        parser_->LoadMapping(datastore);
        cout << "Worker" << my_node_.get_local_rank()  << ": DONE -> Parser_->LoadMapping()" << endl;

        // write snapshot area
        datastore->WriteSnapshot();

        thread recvreq(&Worker::RecvRequest, this);
        thread sendmsg(&Worker::SendQueryMsg, this, mailbox, core_affinity);

        // for TCP use
        thread listener;
        if (!config_->global_use_rdma)
            listener = thread(&Worker::TCPListener, this, datastore);

        Monitor * monitor = new Monitor(my_node_);
        monitor->Start();
        worker_barrier(my_node_);

        // expert driver starts
        ExpertAdapter * expert_adapter = new ExpertAdapter(my_node_, rc_, mailbox, datastore, core_affinity, index_store_);
        expert_adapter->Start();
        cout << "Worker" << my_node_.get_local_rank() << ": DONE -> expert_adapter->Start()" << endl;
        worker_barrier(my_node_);


        fflush(stdout);
        worker_barrier(my_node_);
        if (my_node_.get_world_rank() == MASTER_RANK) cout << "Grasper Servers Are All Ready ..." << endl;


        // pop out the query result from collector
        // TODO(future) add SIG to break and return
        while (1) {
            reply re;
            rc_->Pop(re);

            // Node::SingleTrap("rc_->Pop(re);");

            uint64_t time_ = thpt_monitor_->RecordEnd(re.qid);

            if (!is_emu_mode_) {
                ibinstream m;
                m << re.hostname;   // client hostname
                m << re.results;    // query results
                m << time_;         // execution time

                zmq::message_t msg(m.size());
                memcpy((void *)msg.data(), m.get_buf(), m.size());

                zmq::socket_t sender(context_, ZMQ_PUSH);
                char addr[64];
                // port calculation is based on our self-defined protocol
                sprintf(addr, "tcp://%s:%d", re.hostname.c_str(), workers_[my_node_.get_local_rank()].tcp_port + my_node_.get_world_rank());
                sender.connect(addr);
                cout << "worker_node" << my_node_.get_local_rank() << " sends the results to Client " << re.hostname << endl;
                sender.send(msg);

                monitor->IncreaseCounter(1);
            }
        }

        expert_adapter->Stop();
        monitor->Stop();

        recvreq.join();
        sendmsg.join();
        if (!config_->global_use_rdma)
            listener.join();
    }

 private:
    Node & my_node_;
    vector<Node> & workers_;
    Config * config_;
    Parser* parser_;
    IndexStore* index_store_;
    ThreadSafeQueue<LogicPlan> queue_;
    Result_Collector * rc_;
    uint32_t num_query;

    bool is_emu_mode_;
    Throughput_Monitor * thpt_monitor_;

    zmq::context_t context_;
    zmq::socket_t * receiver_;
    zmq::socket_t * worker_listener_;

    vector<zmq::socket_t *> senders_;
};

#endif /* WORKER_HPP_ */
