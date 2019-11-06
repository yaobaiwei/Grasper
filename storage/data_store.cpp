/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "storage/data_store.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

DataStore::DataStore(Node & node, AbstractIdMapper * id_mapper, Buffer * buf): node_(node), id_mapper_(id_mapper), buffer_(buf) {
    config_ = Config::GetInstance();
    vpstore_ = NULL;
    epstore_ = NULL;
}

DataStore::~DataStore() {
    delete vpstore_;
    delete epstore_;
}

void DataStore::Init(vector<Node> & nodes) {
    vpstore_ = new VKVStore(buffer_);
    epstore_ = new EKVStore(buffer_);
    vpstore_->init(nodes);
    epstore_->init(nodes);
}

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

void DataStore::LoadDataFromHDFS() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    get_string_indexes();
    cout << "Node " << node_.get_local_rank() << " get_string_indexes() DONE !" << endl;

    get_vertices();
    cout << "Node " << node_.get_local_rank() << " Get_vertices() DONE !" << endl;

    if (!snapshot->TestRead("vkvstore")) {
        get_vplist();
    }
    cout << "Node " << node_.get_local_rank() << " Get_vplist() DONE !" << endl;

    if (!snapshot->TestRead("ekvstore")) {
        get_eplist();
    }
    cout << "Node " << node_.get_local_rank() << " Get_eplist() DONE !" << endl;
}

void DataStore::ReadSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    snapshot->ReadData("datastore_v_table", v_table, ReadHashMapSerImpl);
    snapshot->ReadData("datastore_e_table", e_table, ReadHashMapSerImpl);

    vpstore_->ReadSnapshot();
    epstore_->ReadSnapshot();
}

void DataStore::WriteSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    snapshot->WriteData("datastore_v_table", v_table, WriteHashMapSerImpl);
    snapshot->WriteData("datastore_e_table", e_table, WriteHashMapSerImpl);

    vpstore_->WriteSnapshot();
    epstore_->WriteSnapshot();
}

void DataStore::Shuffle() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    if (!snapshot->TestRead("datastore_v_table")) {
        // vertices
        vector<vector<Vertex*>> vtx_parts;
        vtx_parts.resize(node_.get_local_size());
        for (int i = 0; i < vertices.size(); i++) {
            Vertex* v = vertices[i];
            vtx_parts[id_mapper_->GetMachineIdForVertex(v->id)].push_back(v);
        }
        all_to_all(node_, false, vtx_parts);
        vertices.clear();
        if (node_.get_local_rank() == 0) {
            cout << "Shuffle vertex done" << endl;
        }

        for (int i = 0; i < node_.get_local_size(); i++) {
            vertices.insert(vertices.end(), vtx_parts[i].begin(), vtx_parts[i].end());
        }
        vtx_parts.clear();
        vector<vector<Vertex*>>().swap(vtx_parts);
    }

    // edges
    if (!snapshot->TestRead("datastore_e_table")) {
        vector<vector<Edge*>> edges_parts;
        edges_parts.resize(node_.get_local_size());
        for (int i = 0; i < edges.size(); i++) {
            Edge* e = edges[i];
            edges_parts[id_mapper_->GetMachineIdForEdge(e->id)].push_back(e);
        }
        all_to_all(node_, false, edges_parts);
        if (node_.get_local_rank() == 0) {
            cout << "Shuffle edge done" << endl;
        }

        edges.clear();
        for (int i = 0; i < node_.get_local_size(); i++) {
            edges.insert(edges.end(), edges_parts[i].begin(), edges_parts[i].end());
        }
        edges_parts.clear();
        vector<vector<Edge*>>().swap(edges_parts);
    }

    if (!snapshot->TestRead("vkvstore")) {
        // VProperty
        vector<vector<VProperty*>> vp_parts;
        vp_parts.resize(node_.get_local_size());
        for (int i = 0; i < vplist.size(); i++) {
            VProperty* vp = vplist[i];
            map<int, vector<V_KVpair>> node_map;
            for (auto& kv_pair : vp->plist)
                node_map[id_mapper_->GetMachineIdForVProperty(kv_pair.key)].push_back(kv_pair);

            for (auto& item : node_map) {
                VProperty* vp_ = new VProperty();
                vp_->id = vp->id;
                vp_->plist = move(item.second);
                vp_parts[item.first].push_back(vp_);
            }
            delete vp;
        }
        all_to_all(node_, false, vp_parts);

        if (node_.get_local_rank() == 0) {
            cout << "Shuffle vp done" << endl;
        }

        vplist.clear();

        for (int i = 0; i < node_.get_local_size(); i++) {
            vplist.insert(vplist.end(), vp_parts[i].begin(), vp_parts[i].end());
        }
        vp_parts.clear();
        vector<vector<VProperty*>>().swap(vp_parts);

        // vp_lists
        vector<vector<vp_list*>> vpl_parts;
        vpl_parts.resize(node_.get_local_size());
        for (int i = 0; i < vp_buf.size(); i++) {
            vp_list* vp = vp_buf[i];
            vpl_parts[id_mapper_->GetMachineIdForVertex(vp->vid)].push_back(vp);
        }
        all_to_all(node_, false, vpl_parts);
        if (node_.get_local_rank() == 0) {
            cout << "Shuffle vp list done" << endl;
        }

        vp_buf.clear();
        for (int i = 0; i < node_.get_local_size(); i++) {
            vp_buf.insert(vp_buf.end(), vpl_parts[i].begin(), vpl_parts[i].end());
        }
        vpl_parts.clear();
        vector<vector<vp_list*>>().swap(vpl_parts);
    } else {
        if (node_.get_local_rank() == MASTER_RANK)
            printf("Shuffle snapshot->TestRead('vkvstore')\n");
    }

    if (!snapshot->TestRead("ekvstore")) {
        // EProperty
        vector<vector<EProperty*>> ep_parts;
        ep_parts.resize(node_.get_local_size());
        for (int i = 0; i < eplist.size(); i++) {
            EProperty* ep = eplist[i];
            map<int, vector<E_KVpair>> node_map;
            for (auto& kv_pair : ep->plist)
                node_map[id_mapper_->GetMachineIdForEProperty(kv_pair.key)].push_back(kv_pair);
            for (auto& item : node_map) {
                EProperty* ep_ = new EProperty();
                ep_->id = ep->id;
                ep_->plist = move(item.second);
                ep_parts[item.first].push_back(ep_);
            }
            delete ep;
        }

        all_to_all(node_, false, ep_parts);

        if (node_.get_local_rank() == 0) {
            cout << "Shuffle ep done" << endl;
        }

        eplist.clear();
        for (int i = 0; i < node_.get_local_size(); i++) {
            eplist.insert(eplist.end(), ep_parts[i].begin(), ep_parts[i].end());
        }
        ep_parts.clear();
        vector<vector<EProperty*>>().swap(ep_parts);
    } else {
        if (node_.get_local_rank() == MASTER_RANK)
            printf("Shuffle snapshot->TestRead('ekvstore')\n");
    }
}

void DataStore::DataConverter() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();
    if (!snapshot->TestRead("datastore_v_table")) {
        for (int i = 0 ; i < vertices.size(); i++) {
            v_table[vertices[i]->id] = vertices[i];
        }

        for (int i = 0 ; i < vp_buf.size(); i++) {
            hash_map<vid_t, Vertex*>::iterator vIter = v_table.find(vp_buf[i]->vid);
            if (vIter == v_table.end()) {
                cout << "ERROR: FAILED TO MATCH ONE ELEMENT in vp_buf" << endl;
                exit(-1);
            }
            Vertex * v = vIter->second;
            v->vp_list.insert(v->vp_list.end(), vp_buf[i]->pkeys.begin(), vp_buf[i]->pkeys.end());
        }
        // clean the vp_buf
        for (int i = 0 ; i < vp_buf.size(); i++) delete vp_buf[i];
        vector<vp_list*>().swap(vp_buf);
        vector<Vertex*>().swap(vertices);
    } else {
        if (node_.get_local_rank() == MASTER_RANK)
            printf("DataConverter snapshot->TestRead('datastore_v_table')\n");
    }

    if (!snapshot->TestRead("datastore_e_table")) {
        for (int i = 0 ; i < edges.size(); i++) {
            e_table[edges[i]->id] = edges[i];
        }
    }
    vector<Edge*>().swap(edges);

    if (!snapshot->TestRead("vkvstore")) {
        vpstore_->insert_vertex_properties(vplist);
        // clean the vp_list
        for (int i = 0 ; i < vplist.size(); i++) {
            // cout << vplist[i]->DebugString();  // TEST
            delete vplist[i];
        }
        vector<VProperty*>().swap(vplist);
    }

    if (!snapshot->TestRead("ekvstore")) {
        epstore_->insert_edge_properties(eplist);
        // clean the ep_list
        for (int i = 0 ; i < eplist.size(); i++) {
            // cout << eplist[i]->DebugString();  // TEST
            delete eplist[i];
        }
        vector<EProperty*>().swap(eplist);
    }
}

Vertex* DataStore::GetVertex(vid_t v_id) {
    CHECK(id_mapper_->IsVertexLocal(v_id));
    // return v_table[v_id];

    hash_map<vid_t, Vertex*>::iterator vt_itr = v_table.find(v_id);
    if (vt_itr != v_table.end()) {
        return vt_itr->second;
    }
    return NULL;
}

Edge* DataStore::GetEdge(eid_t e_id) {
    CHECK(id_mapper_->IsEdgeLocal(e_id));
    // return e_table[e_id];

    hash_map <eid_t, Edge*> ::iterator et_itr = e_table.find(e_id);
    if (et_itr != e_table.end()) {
        return et_itr->second;
    }
    return NULL;
}

void DataStore::GetAllVertices(vector<vid_t> & vid_list) {
    hash_map<vid_t, Vertex*>::iterator vt_itr;

    for (vt_itr = v_table.begin(); vt_itr != v_table.end(); vt_itr++) {
       vid_list.push_back(vt_itr->first);
    }
}

void DataStore::GetAllEdges(vector<eid_t> & eid_list) {
    hash_map<eid_t, Edge*>::iterator et_itr;

    for (et_itr = e_table.begin(); et_itr != e_table.end(); et_itr++) {
       eid_list.push_back(et_itr->first);
    }
}

bool DataStore::VPKeyIsLocal(vpid_t vp_id) {
    if (id_mapper_->IsVPropertyLocal(vp_id)) {
        return true;
    } else {
        return false;
    }
}

bool DataStore::EPKeyIsLocal(epid_t ep_id) {
    if (id_mapper_->IsEPropertyLocal(ep_id)) {
        return true;
    } else {
        return false;
    }
}

bool DataStore::GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val) {
    if (id_mapper_->IsVPropertyLocal(vp_id)) {      // locally
        vpstore_->get_property_local(vp_id.value(), val);
    } else {                                        // remotely
        vpstore_->get_property_remote(tid, id_mapper_->GetMachineIdForVProperty(vp_id), vp_id.value(), val);
    }

    if (val.content.size())
        return true;
    return false;
}

bool DataStore::GetPropertyForEdge(int tid, epid_t ep_id, value_t & val) {
    if (id_mapper_->IsEPropertyLocal(ep_id)) {      // locally
        epstore_->get_property_local(ep_id.value(), val);
    } else {                                        // remotely
        epstore_->get_property_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), val);
    }

    if (val.content.size())
        return true;
    return false;
}

bool DataStore::GetLabelForVertex(int tid, vid_t vid, label_t & label) {
    label = 0;
    vpid_t vp_id(vid, 0);
    if (id_mapper_->IsVPropertyLocal(vp_id)) {      // locally
        vpstore_->get_label_local(vp_id.value(), label);
    } else {                                        // remotely
        vpstore_->get_label_remote(tid, id_mapper_->GetMachineIdForVProperty(vp_id), vp_id.value(), label);
    }
    return label;
}

bool DataStore::GetLabelForEdge(int tid, eid_t eid, label_t & label) {
    label = 0;
    epid_t ep_id(eid, 0);
    if (id_mapper_->IsEPropertyLocal(ep_id)) {      // locally
        epstore_->get_label_local(ep_id.value(), label);
    } else {                                        // remotely
        epstore_->get_label_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), label);
    }
    return label;
}

int DataStore::GetMachineIdForVertex(vid_t v_id) {
    return id_mapper_->GetMachineIdForVertex(v_id);
}

int DataStore::GetMachineIdForEdge(eid_t e_id) {
    return id_mapper_->GetMachineIdForEdge(e_id);
}

void DataStore::GetNameFromIndex(Index_T type, label_t id, string & str) {
    unordered_map<label_t, string>::const_iterator itr;

    switch (type) {
        case Index_T::E_LABEL:
            itr = indexes.el2str.find(id);
            if (itr == indexes.el2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::E_PROPERTY:
            itr = indexes.epk2str.find(id);
            if (itr == indexes.epk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_LABEL:
            itr = indexes.vl2str.find(id);
            if (itr == indexes.vl2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_PROPERTY:
            itr = indexes.vpk2str.find(id);
            if (itr == indexes.vpk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        default:
            return;
    }
}

void DataStore::InsertAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr == agg_data_table.end()) {
        // Not Found, insert
        agg_data_table.insert(pair<agg_t, vector<value_t>>(key, data));
    } else {
        agg_data_table.at(key).insert(agg_data_table.at(key).end(), data.begin(), data.end());
    }
}

void DataStore::GetAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        data = itr->second;
    }
}

void DataStore::DeleteAggData(agg_t key) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        agg_data_table.erase(itr);
    }
}

void DataStore::AccessVProperty(uint64_t vp_id_v, value_t & val) {
    vpstore_->get_property_local(vp_id_v, val);
}

void DataStore::AccessEProperty(uint64_t ep_id_v, value_t & val) {
    epstore_->get_property_local(ep_id_v, val);
}

void DataStore::get_string_indexes() {
    hdfsFS fs = get_hdfs_fs();

    string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
    hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
    LineReader el_reader(fs, el_file);
    while (true) {
        el_reader.read_line();
        if (!el_reader.eof()) {
            char * line = el_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(NULL, "\t");
            label_t id = atoi(pch);

            // both string and ID are unique
            assert(indexes.str2el.find(key) == indexes.str2el.end());
            assert(indexes.el2str.find(id) == indexes.el2str.end());

            indexes.str2el[key] = id;
            indexes.el2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, el_file);

    string epk_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
    hdfsFile epk_file = get_r_handle(epk_path.c_str(), fs);
    LineReader epk_reader(fs, epk_file);
    while (true) {
        epk_reader.read_line();
        if (!epk_reader.eof()) {
            char * line = epk_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(NULL, "\t");
            label_t id = atoi(pch);
            pch = strtok(NULL, "\t");
            indexes.str2eptype[to_string(id)] = atoi(pch);


            // both string and ID are unique
            assert(indexes.str2epk.find(key) == indexes.str2epk.end());
            assert(indexes.epk2str.find(id) == indexes.epk2str.end());

            indexes.str2epk[key] = id;
            indexes.epk2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, epk_file);

    string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
    hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
    LineReader vl_reader(fs, vl_file);
    while (true) {
        vl_reader.read_line();
        if (!vl_reader.eof()) {
            char * line = vl_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(NULL, "\t");
            label_t id = atoi(pch);

            // both string and ID are unique
            assert(indexes.str2vl.find(key) == indexes.str2vl.end());
            assert(indexes.vl2str.find(id) == indexes.vl2str.end());

            indexes.str2vl[key] = id;
            indexes.vl2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vl_file);

    string vpk_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
    hdfsFile vpk_file = get_r_handle(vpk_path.c_str(), fs);
    LineReader vpk_reader(fs, vpk_file);
    while (true) {
        vpk_reader.read_line();
        if (!vpk_reader.eof()) {
            char * line = vpk_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(NULL, "\t");
            label_t id = atoi(pch);
            pch = strtok(NULL, "\t");
            indexes.str2vptype[to_string(id)] = atoi(pch);

            // both string and ID are unique
            assert(indexes.str2vpk.find(key) == indexes.str2vpk.end());
            assert(indexes.vpk2str.find(id) == indexes.vpk2str.end());

            indexes.str2vpk[key] = id;
            indexes.vpk2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vpk_file);
    hdfsDisconnect(fs);
}

void DataStore::get_vertices() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // break if the vtxs has already been finished.
    if (snapshot->TestRead("datastore_v_table")) {
        if (node_.get_local_rank() == MASTER_RANK)
            printf("get_vertices snapshot->TestRead('datastore_v_table')\n");
        return;
    }

    // check path + arrangement
    const char * indir = config_->HDFS_VTX_SUBFOLDER.c_str();

    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    }
}

void DataStore::load_vertices(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            Vertex * v = to_vertex(reader.get_line());
            vertices.push_back(v);
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] #in_nbs [\t] nb1 [space] nb2 [space] ... #out_nbs [\t] nb1 [space] nb2 [space] ...
Vertex* DataStore::to_vertex(char* line) {
    Vertex * v = new Vertex;

    char * pch;
    pch = strtok(line, "\t");

    vid_t vid(atoi(pch));
    v->id = vid;

    pch = strtok(NULL, "\t");
    int num_in_nbs = atoi(pch);
    for (int i = 0 ; i < num_in_nbs; i++) {
        pch = strtok(NULL, " ");
        v->in_nbs.push_back(atoi(pch));
    }
    pch = strtok(NULL, "\t");
    int num_out_nbs = atoi(pch);
    for (int i = 0 ; i < num_out_nbs; i++) {
        pch = strtok(NULL, " ");
        v->out_nbs.push_back(atoi(pch));
    }
    return v;
}

void DataStore::get_vplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_VP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vplist(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vplist(it->c_str());
    }
}

void DataStore::load_vplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_vp(reader.get_line(), vplist, vp_buf);
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf) {
    VProperty * vp = new VProperty;
    vp_list * vpl = new vp_list;

    char * pch;
    pch = strtok(line, "\t");
    vid_t vid(atoi(pch));
    vp->id = vid;
    vpl->vid = vid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);

    // insert label to VProperty
    V_KVpair v_pair;
    v_pair.key = vpid_t(vid, 0);
    Tool::str2int(to_string(label), v_pair.value);
    // push to property_list of v
    vp->plist.push_back(v_pair);

    pch = strtok(NULL, "");
    string s(pch);

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);
    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2vptype[kvpairs[i]], p);
        V_KVpair v_pair;
        v_pair.key = vpid_t(vid, p.key);
        v_pair.value = p.value;

        // push to property_list of v
        vp->plist.push_back(v_pair);

        // for property index on v
        vpl->pkeys.push_back((label_t)p.key);
    }

    // sort p_list in vertex
    sort(vpl->pkeys.begin(), vpl->pkeys.end());
    vplist.push_back(vp);
    vp_buf.push_back(vpl);

    // cout << "####### " << vp->DebugString(); //DEBUG
}

void DataStore::get_eplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_EP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_eplist(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_eplist(it->c_str());
    }
}

void DataStore::load_eplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_ep(reader.get_line(), eplist);
         } else {
            break;
         }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// out-v[\t] in-v[\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_ep(char* line, vector<EProperty*> & eplist) {
    Edge * e = new Edge;
    EProperty * ep = new EProperty;

    uint64_t atoi_time = timer::get_usec();
    char * pch;
    pch = strtok(line, "\t");
    int out_v = atoi(pch);
    pch = strtok(NULL, "\t");
    int in_v = atoi(pch);

    eid_t eid(in_v, out_v);
    e->id = eid;
    ep->id = eid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);
    // insert label to EProperty
    E_KVpair e_pair;
    e_pair.key = epid_t(in_v, out_v, 0);
    Tool::str2int(to_string(label), e_pair.value);
    // push to property_list of v
    ep->plist.push_back(e_pair);

    pch = strtok(NULL, "");
    string s(pch);
    vector<label_t> pkeys;

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);

    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2eptype[kvpairs[i]], p);

        E_KVpair e_pair;
        e_pair.key = epid_t(in_v, out_v, p.key);
        e_pair.value = p.value;

        ep->plist.push_back(e_pair);
        pkeys.push_back((label_t)p.key);
    }

    sort(pkeys.begin(), pkeys.end());
    e->ep_list.insert(e->ep_list.end(), pkeys.begin(), pkeys.end());
    edges.push_back(e);
    eplist.push_back(ep);
}
