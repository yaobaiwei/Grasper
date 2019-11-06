/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "mpi_unique_namer.hpp"

#include <cstdio>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>
#include <fstream>
#include <functional>

using namespace std;

void MPIUniqueNamer::GetHostsStr() {
    char tmp_hn[1000];
    int hn_len;
    MPI_Get_processor_name(tmp_hn, &hn_len);

    string hn(tmp_hn);

    int* hn_lens = new int[comm_sz_];
    int* hn_displs = new int[comm_sz_];

    MPI_Allgather(&hn_len, 1, MPI_INT, hn_lens, 1, MPI_INT, comm_);

    // after gathered this, call MPI_Allgatherv to merge hostname
    int total_len = hn_lens[0];
    hn_displs[0] = 0;

    for (int i = 1; i < comm_sz_; i++) {
        hn_displs[i] = total_len;
        total_len += hn_lens[i];
    }

    char* tmp_hn_cat = new char[total_len + 1];
    MPI_Allgatherv(tmp_hn, hn_len, MPI_CHAR, tmp_hn_cat, hn_lens, hn_displs, MPI_CHAR, comm_);

    tmp_hn_cat[total_len] = 0;
    string hn_cat(tmp_hn_cat);
    string rank_str = ultos(my_rank_);

    // make sure that different host has different dir name, which enables debug on NFS
    hn_cat_ = rank_str + hn_cat;

    delete hn_lens;
    delete hn_displs;
    delete tmp_hn_cat;
}

unsigned long MPIUniqueNamer::GetHash(string s) {
    hash<string> str_hash;
    return str_hash(s);
}

string MPIUniqueNamer::ultos(unsigned long ul) {
    char c[50];
    sprintf(c, "%lu", ul);
    return string(c);
}

void MPIUniqueNamer::AppendHash(string to_append) {
    if (hashed_str_.size() != 0)
        hashed_str_ = hashed_str_ + "_";

    hashed_str_ += ultos(GetHash(to_append));
}

string MPIUniqueNamer::ExtractHash() {
    return hashed_str_;
}
