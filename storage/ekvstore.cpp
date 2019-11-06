/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <assert.h>
#include "storage/ekvstore.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

using namespace std;

void EKVStore::ReadSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    auto snapshot_tmp_tuple = make_tuple(last_entry, mem_sz, mem);

    if (snapshot->ReadData("ekvstore", snapshot_tmp_tuple, ReadKVStoreImpl)) {
        last_entry = get<0>(snapshot_tmp_tuple);
    }
}

void EKVStore::WriteSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    auto snapshot_tmp_tuple = make_tuple(last_entry, mem_sz, mem);
    snapshot->WriteData("ekvstore", snapshot_tmp_tuple, WriteKVStoreImpl);
}

// ==================EKVStore=======================
uint64_t EKVStore::insert_id(uint64_t _pid) {
    // pid is already hashed
    uint64_t bucket_id = _pid % num_buckets;
    uint64_t slot_id = bucket_id * ASSOCIATIVITY;
    uint64_t lock_id = bucket_id % NUM_LOCKS;

    bool found = false;
    pthread_spin_lock(&bucket_locks[lock_id]);
    while (slot_id < num_slots) {
        // the last slot of each bucket is reserved for pointer to indirect header
        // key.pid is used to store the bucket_id of indirect header
        for (int i = 0; i < ASSOCIATIVITY - 1; i++, slot_id++) {
            // assert(vertices[slot_id].key != key);  // no duplicate key
            if (keys[slot_id].pid == _pid) {
                // Cannot get the original pid
                cout << "EKVStore ERROR: conflict at slot["
                     << slot_id << "] of bucket["
                     << bucket_id << "]" << endl;
                assert(false);
            }

            // insert to an empty slot
            if (keys[slot_id].pid == 0) {
                keys[slot_id].pid = _pid;
                goto done;
            }
        }

        // whether the bucket_ext (indirect-header region) is used
        if (!keys[slot_id].is_empty()) {
            slot_id = keys[slot_id].pid * ASSOCIATIVITY;
            continue;  // continue and jump to next bucket
        }


        // allocate and link a new indirect header
        pthread_spin_lock(&bucket_ext_lock);
        if (last_ext >= num_buckets_ext) {
            cout << "EKVStore ERROR: out of indirect-header region." << endl;
            assert(last_ext < num_buckets_ext);
        }
        keys[slot_id].pid = num_buckets + (last_ext++);
        pthread_spin_unlock(&bucket_ext_lock);

        slot_id = keys[slot_id].pid * ASSOCIATIVITY;  // move to a new bucket_ext
        keys[slot_id].pid = _pid;  // insert to the first slot
        goto done;
    }

done:
    pthread_spin_unlock(&bucket_locks[lock_id]);
    assert(slot_id < num_slots);
    assert(keys[slot_id].pid == _pid);
    return slot_id;
}

// Insert all properties for one vertex
void EKVStore::insert_single_edge_property(EProperty* ep) {
    // Every <vpid_t, value_t>
    for (int i = 0; i < ep->plist.size(); i++) {
        E_KVpair e_kv = ep->plist[i];
        // insert key and get slot_id
        int slot_id = insert_id(e_kv.key.value());

        // get length of centent
        uint64_t length = e_kv.value.content.size();

        // allocate for values in entry_region
        uint64_t off = sync_fetch_and_alloc_values(length + 1);

        // insert ptr
        ptr_t ptr = ptr_t(length + 1, off);
        keys[slot_id].ptr = ptr;

        // insert type of value first
        values[off++] = (char)e_kv.value.type;

        // insert value
        memcpy(&values[off], &e_kv.value.content[0], length);
    }
}

// Get current available memory location and reserve enough space for current kv
uint64_t EKVStore::sync_fetch_and_alloc_values(uint64_t n) {
    uint64_t orig;
    pthread_spin_lock(&entry_lock);
    orig = last_entry;
    last_entry += n;
    if (last_entry >= num_entries) {
        cout << "EKVStore ERROR: out of entry region." << endl;
        assert(last_entry < num_entries);
    }
    pthread_spin_unlock(&entry_lock);
    return orig;
}

// Get ikey_t by vpid or epid
void EKVStore::get_key_local(uint64_t pid, ikey_t & key) {
    uint64_t bucket_id = pid % num_buckets;
    while (true) {
        for (int i = 0; i < ASSOCIATIVITY; i++) {
            uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
            if (i < ASSOCIATIVITY - 1) {
                // data part
                if (keys[slot_id].pid == pid) {
                    // found it
                    key = keys[slot_id];
                    return;
                }
            } else {
                if (keys[slot_id].is_empty())
                    return;

                bucket_id = keys[slot_id].pid;  // move to next bucket
                break;  // break for-loop
            }
        }
    }
}

// Get key by key remotely
void EKVStore::get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key) {
    assert(config_->global_use_rdma);

    uint64_t bucket_id = pid % num_buckets;

    while (true) {
        char * buffer = buf_->GetSendBuf(tid);
        uint64_t off = offset + bucket_id * ASSOCIATIVITY * sizeof(ikey_t);
        uint64_t sz = ASSOCIATIVITY * sizeof(ikey_t);

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        rdma.dev->RdmaRead(tid, dst_nid, buffer, sz, off);
        // timer::stop_timer(tid);
        ikey_t *keys = (ikey_t *)buffer;
        for (int i = 0; i < ASSOCIATIVITY; i++) {
            if (i < ASSOCIATIVITY - 1) {
                if (keys[i].pid == pid) {
                    key = keys[i];
                    return;
                }
            } else {
                if (keys[i].is_empty())
                    return;  // not found

                bucket_id = keys[i].pid;  // move to next bucket
                break;  // break for-loop
            }
        }
    }
}

EKVStore::EKVStore(Buffer * buf) : buf_(buf) {
    config_ = Config::GetInstance();
    mem = config_->kvstore + GiB2B(config_->global_vertex_property_kv_sz_gb);
    mem_sz = GiB2B(config_->global_edge_property_kv_sz_gb);
    offset = config_->kvstore_offset + GiB2B(config_->global_vertex_property_kv_sz_gb);
    HD_RATIO = config_->key_value_ratio_in_rdma;

    // size for header and entry
    uint64_t header_sz = mem_sz * HD_RATIO / 100;
    uint64_t entry_sz = mem_sz - header_sz;

    // header region
    num_slots = header_sz / sizeof(ikey_t);
    num_buckets = mymath::hash_prime_u64((num_slots / ASSOCIATIVITY) * MHD_RATIO / 100);
    num_buckets_ext = (num_slots / ASSOCIATIVITY) - num_buckets;
    last_ext = 0;

    // entry region
    num_entries = entry_sz;
    last_entry = 0;

    cout << "INFO: ekvstore = " << header_sz + entry_sz << " bytes " << std::endl
         << "      header region: " << num_slots << " slots"
         << " (main = " << num_buckets << ", indirect = " << num_buckets_ext << ")" << std::endl
         << "      entry region: " << num_entries << " entries" << std::endl;

    // Header
    keys = (ikey_t *)(mem);
    // Entry
    values = (char *)(mem + num_slots * sizeof(ikey_t));

    pthread_spin_init(&entry_lock, 0);
    pthread_spin_init(&bucket_ext_lock, 0);
    for (int i = 0; i < NUM_LOCKS; i++)
        pthread_spin_init(&bucket_locks[i], 0);
}

void EKVStore::init(vector<Node> & nodes) {
    // initiate keys to 0 which means empty key
    for (uint64_t i = 0; i < num_slots; i++) {
        keys[i] = ikey_t();
    }

    if (!config_->global_use_rdma) {
        requesters.resize(config_->global_num_workers);
        for (int nid = 0; nid < config_->global_num_workers; nid++) {
            Node & r_node = GetNodeById(nodes, nid + 1);
            string ibname = r_node.ibname;

            requesters[nid] = new zmq::socket_t(context, ZMQ_REQ);
            char addr[64] = "";
            sprintf(addr, "tcp://%s:%d", ibname.c_str(), r_node.tcp_port + 1 + config_->global_num_threads);
            requesters[nid]->connect(addr);
        }
        pthread_spin_init(&req_lock, 0);
    }
}

// Insert a list of Edge properties
void EKVStore::insert_edge_properties(vector<EProperty*> & eplist) {
    for (int i = 0; i < eplist.size(); i++) {
        insert_single_edge_property(eplist.at(i));
    }
}

// Get property by key locally
void EKVStore::get_property_local(uint64_t pid, value_t & val) {
    ikey_t key;
    get_key_local(pid, key);

    if (key.is_empty()) {
        val.content.resize(0);
        return;
    }

    uint64_t off = key.ptr.off;
    uint64_t size = key.ptr.size - 1;

    // type : char to uint8_t
    val.type = values[off++];
    val.content.resize(size);

    char * ctt = &(values[off]);
    std::copy(ctt, ctt+size, val.content.begin());
}

// Get properties by key remotely
void EKVStore::get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val) {
    if (config_->global_use_rdma) {
        ikey_t key;
        get_key_remote(tid, dst_nid, pid, key);
        if (key.is_empty()) {
            val.content.resize(0);
            return;
        }

        char * buffer = buf_->GetSendBuf(tid);
        uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
        uint64_t r_sz = key.ptr.size;

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        rdma.dev->RdmaRead(tid, dst_nid, buffer, r_sz, r_off);
        // timer::stop_timer(tid);

        // type : char to uint8_t
        val.type = buffer[0];
        val.content.resize(r_sz-1);

        char * ctt = &(buffer[1]);
        std::copy(ctt, ctt + r_sz-1, val.content.begin());
    } else {
        ibinstream im;
        obinstream um;

        im << pid;
        im << Element_T::EDGE;
        pthread_spin_lock(&req_lock);
        SendReq(dst_nid, im);

        RecvRep(dst_nid, um);
        pthread_spin_unlock(&req_lock);
        um >> val;
    }
}

void EKVStore::get_label_local(uint64_t pid, label_t & label) {
    value_t val;
    get_property_local(pid, val);
    label = (label_t)Tool::value_t2int(val);
}

void EKVStore::get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label) {
    value_t val;
    get_property_remote(tid, dst_nid, pid, val);
    label = (label_t)Tool::value_t2int(val);
}

// analysis
void EKVStore::print_mem_usage() {
    uint64_t used_slots = 0;
    for (uint64_t x = 0; x < num_buckets; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "EKVStore main header: " << B2MiB(num_buckets * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets * ASSOCIATIVITY << " slots)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;
    cout << "\tchain: " << 100.0 * num_buckets / (num_buckets * ASSOCIATIVITY)
         << " % (" << num_buckets << " slots)" << endl;

    used_slots = 0;
    for (uint64_t x = num_buckets; x < num_buckets + last_ext; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "indirect header: " << B2MiB(num_buckets_ext * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets_ext * ASSOCIATIVITY << " slots)" << endl;
    cout << "\talloced: " << 100.0 * last_ext / num_buckets_ext
         << " % (" << last_ext << " buckets)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets_ext * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;

    cout << "entry: " << B2MiB(num_entries * sizeof(char))
         << " MB (" << num_entries << " entries)" << endl;
    cout << "\tused: " << 100.0 * last_entry / num_entries
         << " % (" << last_entry << " entries)" << endl;
}

void EKVStore::SendReq(int dst_nid, ibinstream & m) {
    zmq::message_t msg(m.size());
    memcpy((void *)msg.data(), m.get_buf(), m.size());
    requesters[dst_nid]->send(msg);
}

bool EKVStore::RecvRep(int nid, obinstream & um) {
    zmq::message_t msg;

    if (requesters[nid]->recv(&msg) < 0) {
        return false;
    }

    char* buf = new char[msg.size()];
    memcpy(buf, msg.data(), msg.size());
    um.assign(buf, msg.size(), 0);
    return true;
}
