/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include "utils/mpi_unique_namer.hpp"
#include <unistd.h>
#include <fstream>
#include "base/serialization.hpp"

// make sure that the snapshot path exists
// when this instance is called, make sure that MPIUniqueNamer is initialed.

namespace std {

class MPISnapshot {
 public:
    template<typename T>
    bool WriteData(string key, T& data, bool(WriteFunc)(string, T&)) {
        if (!write_enabled_)
            return false;

        if (TestRead(key))
            return true;

        // hash the key
        string fn = path_ + "/" + unique_namer_->ultos(unique_namer_->GetHash(key));
        WriteFunc(fn, data);
        write_map_[key] = true;

        return true;
    }

    template<typename T>
    bool ReadData(string key, T& data, bool(ReadFunc)(string, T&)) {
        if (!read_enabled_)
            return false;

        // hash the key
        string fn = path_ + "/" + unique_namer_->ultos(unique_namer_->GetHash(key));

        read_map_[key] = ReadFunc(fn, data);

        return read_map_[key];
    }

    // after read data of a specific key, this function will return true (for global usage)
    bool TestRead(string key) {
        if (read_map_.count(key) == 0)
            return false;  // not found
        if (read_map_[key])
            return true;
        return false;
    }

    bool TestWrite(string key) {
        if (write_map_.count(key) == 0)
            return false;  // not found
        if (write_map_[key])
            return true;
        return false;
    }

    // to overwrite snapshot (e.g., the file is damaged.)
    bool DisableRead() { read_enabled_ = false; }
    // to avoid write snapshot
    bool DisableWrite() { write_enabled_ = false; }

 private:
    MPIUniqueNamer* unique_namer_;
    string path_;

    map<string, bool> read_map_;
    map<string, bool> write_map_;

    // by default, it is enabled
    bool read_enabled_ = true;
    bool write_enabled_ = true;

    MPISnapshot(string path);

 public:
    static MPISnapshot* GetInstance(string path = "") {
        static MPISnapshot* snapshot_single_instance = NULL;

        if (snapshot_single_instance == NULL) {
            snapshot_single_instance = new MPISnapshot(path);
        }

        return snapshot_single_instance;
    }
};

};  // namespace std
