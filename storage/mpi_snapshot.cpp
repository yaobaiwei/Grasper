/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "mpi_snapshot.hpp"

using namespace std;

MPISnapshot::MPISnapshot(string path) {
    unique_namer_ = MPIUniqueNamer::GetInstance();

    // get the snapshot path
    path_ = path + "/" + unique_namer_->ExtractHash();

    // mkdir
    string cmd = "mkdir -p " + path_;
    system(cmd.c_str());
}
