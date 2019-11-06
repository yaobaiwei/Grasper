/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

// this file is designed to be isolated from MPISnapshot
// the function below is to be used as a parameter of MPISnapShot::WriteData and MPISnapShot::ReadData

#include <ext/hash_map>

#include "base/serialization.hpp"
#include "core/abstract_mailbox.hpp"

using __gnu_cxx::hash_map;

template<typename T>
static inline bool WriteSerImpl(string fn, T& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    ibinstream m;
    m << data;

    uint64_t buf_sz = m.size();
    out_f.write((char*)&buf_sz, sizeof(uint64_t));
    out_f.write(m.get_buf(), m.size());
    out_f.close();

    return true;
}

template<typename T>
static inline bool ReadSerImpl(string fn, T& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    uint64_t sz;
    in_f.read((char*)&sz, sizeof(uint64_t));

    char* tmp_buf = new char[sz];
    in_f.read(tmp_buf, sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, sz, 0);
    m >> data;

    return true;
}

template<typename T1, typename T2>
static inline bool WriteHashMapSerImpl(string fn, hash_map<T1, T2*>& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    // write data to the instream
    ibinstream m;

    for (auto kv : data) {
        // notice that kv.second is a pointer
        // and the content of the pointer need to be written
        m << kv.first;  // the key
        m << *kv.second;  // the value is a pointer
    }

    uint64_t data_sz = data.size(), buf_sz = m.size();

    // out_f << data_sz;
    // out_f << buf_sz;
    out_f.write((char*)&data_sz, sizeof(uint64_t));
    out_f.write((char*)&buf_sz, sizeof(uint64_t));
    out_f.write(m.get_buf(), m.size());

    out_f.close();

    return true;
}

template<typename T1, typename T2>
static inline bool ReadHashMapSerImpl(string fn, hash_map<T1, T2*>& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    uint64_t buf_sz, data_sz;
    // in_f >> data_sz;
    // in_f >> buf_sz;
    in_f.read((char*)&data_sz, sizeof(uint64_t));
    in_f.read((char*)&buf_sz, sizeof(uint64_t));

    char* tmp_buf = new char[buf_sz];
    in_f.read(tmp_buf, buf_sz);
    in_f.close();

    obinstream m;
    m.assign(tmp_buf, buf_sz, 0);
    // do not free

    for (uint64_t i = 0; i < data_sz; i++) {
        T1 key;
        T2* value = new T2;
        m >> key >> *value;
        data[key] = value;
    }

    return true;
}

static inline bool WriteKVStoreImpl(string fn, tuple<uint64_t, uint64_t, char*>& data) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    uint64_t last_entry = get<0>(data), mem_sz = get<1>(data);
    char* mem = get<2>(data);

    out_f.write((char*)&last_entry, sizeof(uint64_t));
    out_f.write((char*)&mem_sz, sizeof(uint64_t));
    out_f.write(mem, mem_sz);

    out_f.close();

    return true;
}

static inline bool ReadKVStoreImpl(string fn, tuple<uint64_t, uint64_t, char*>& data) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    char* mem = get<2>(data);

    uint64_t last_entry, mem_sz;

    in_f.read((char*)&last_entry, sizeof(uint64_t));
    in_f.read((char*)&mem_sz, sizeof(uint64_t));
    in_f.read(mem, mem_sz);
    in_f.close();

    data = make_tuple(last_entry, mem_sz, mem);

    return true;
}

static inline bool WriteMailboxMsgImpl(string fn, vector<Message>& msgs) {
    ofstream out_f(fn, ios::binary);

    if (!out_f.is_open()) {
        return false;
    }

    // write data to the instream
    ibinstream in;
    in << msgs;

    uint64_t len = in.size();
    out_f.write(reinterpret_cast<char*>(&len), sizeof(uint64_t));
    out_f.write(in.get_buf(), len);
    out_f.close();

    return true;
}

static inline bool ReadMailboxMsgImpl(string fn, vector<Message>& msgs) {
    ifstream in_f(fn, ios::binary);

    if (!in_f.is_open()) {
        return false;
    }

    obinstream out;
    uint64_t len;
    in_f.read(reinterpret_cast<char*>(&len), sizeof(size_t));

    char* tmp_buf = new char[len];
    in_f.read(tmp_buf, len);
    out.assign(tmp_buf, len, 0);
    out >> msgs;

    in_f.close();

    return true;
}
