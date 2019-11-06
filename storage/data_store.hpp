/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <mutex>
#include <string>
#include <stdlib.h>
#include <ext/hash_map>
#include <ext/hash_set>
#include <hdfs.h>
#include "glog/logging.h"

#include "base/type.hpp"
#include "base/node_util.hpp"
#include "base/communication.hpp"
#include "core/id_mapper.hpp"
#include "core/buffer.hpp"
#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataStore {
 public:
    DataStore(Node & node, AbstractIdMapper * id_mapper, Buffer * buf);

    ~DataStore();

    void Init(vector<Node> & nodes);

    // index format
    // string \t index [int]
    /*
     *    unordered_map<string, label_t> str2el; //map to edge_label
     *    unordered_map<label_t, string> el2str;
     *    unordered_map<string, label_t> str2epk; //map to edge's property key
     *    unordered_map<label_t, string> epk2str;
     *    unordered_map<string, label_t> str2vl; //map to vtx_label
     *    unordered_map<label_t, string> vl2str;
     *    unordered_map<string, label_t> str2vpk; //map to vtx's property key
     *    unordered_map<label_t, string> vpk2str;
     */

    void LoadDataFromHDFS();
    void Shuffle();
    void DataConverter();

    void ReadSnapshot();
    void WriteSnapshot();

    Vertex* GetVertex(vid_t v_id);
    Edge* GetEdge(eid_t e_id);

    void GetAllVertices(vector<vid_t> & vid_list);
    void GetAllEdges(vector<eid_t> & eid_list);

    bool VPKeyIsLocal(vpid_t vp_id);
    bool EPKeyIsLocal(epid_t ep_id);

    bool GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val);
    bool GetPropertyForEdge(int tid, epid_t ep_id, value_t & val);

    bool GetLabelForVertex(int tid, vid_t vid, label_t & label);
    bool GetLabelForEdge(int tid, eid_t eid, label_t & label);

    int GetMachineIdForVertex(vid_t v_id);
    int GetMachineIdForEdge(eid_t e_id);

    void GetNameFromIndex(Index_T type, label_t label, string & str);

    void InsertAggData(agg_t key, vector<value_t> & data);
    void GetAggData(agg_t key, vector<value_t> & data);
    void DeleteAggData(agg_t key);

    // local access val for TCP request
    void AccessVProperty(uint64_t vp_id_v, value_t & val);
    void AccessEProperty(uint64_t ep_id_v, value_t & val);

    // single ptr instance
    // diff from Node
    static DataStore* StaticInstanceP(DataStore* p = NULL) {
        static DataStore* static_instance_p_ = NULL;
        if (p) {
            // if(static_instance_p_)
            //     delete static_instance_p_;
            // static_instance_p_ = new DataStore;
            static_instance_p_ = p;
        }

        assert(static_instance_p_ != NULL);
        return static_instance_p_;
    }

    // load the index and data from HDFS
    string_index indexes;  // index is global, no need to shuffle

 private:
    Buffer * buffer_;
    AbstractIdMapper* id_mapper_;
    Config* config_;
    Node & node_;

    hash_map<vid_t, Vertex*> v_table;
    hash_map<eid_t, Edge*> e_table;

    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;

    VKVStore * vpstore_;
    EKVStore * epstore_;

    // =========tmp usage=========
    // will not be used after data loading
    vector<Vertex*> vertices;
    vector<Edge*> edges;
    vector<VProperty*> vplist;
    vector<EProperty*> eplist;
    vector<vp_list*> vp_buf;

    // ==========tmp usage=========
    void get_string_indexes();
    void get_vertices();
    void load_vertices(const char* inpath);
    Vertex* to_vertex(char* line);

    void get_vplist();
    void load_vplist(const char* inpath);
    void to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf);

    void get_eplist();
    void load_eplist(const char* inpath);
    void to_ep(char* line, vector<EProperty*> & eplist);
};
