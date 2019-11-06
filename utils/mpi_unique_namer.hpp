/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <string>
#include <mpi.h>

/* MPIUniqueNamer{}
 * Usage:
 * to generate a unique hash-based path for MPI program.
 * the "unique path" design is related to the specific application conf
 **/
class MPIUniqueNamer {
 private:
    // the concat of hostnames
    void GetHostsStr();

    MPIUniqueNamer(MPI_Comm comm) {
        comm_ = comm;
        MPI_Comm_rank(comm, &my_rank_);
        MPI_Comm_size(comm, &comm_sz_);

        GetHostsStr();

        AppendHash(hn_cat_);
    }

    MPI_Comm comm_;
    int my_rank_;
    int comm_sz_;

    std::string hn_cat_;

    std::string hashed_str_;

 public:
    static MPIUniqueNamer* GetInstance(MPI_Comm comm = MPI_COMM_WORLD) {
        static MPIUniqueNamer* config_namer_single_instance = NULL;

        if (config_namer_single_instance == NULL) {
            config_namer_single_instance = new MPIUniqueNamer(comm);
        }

        return config_namer_single_instance;
    }

    MPI_Comm GetComm() const {return comm_;}
    int GetCommRank() const {return my_rank_;}
    int GetCommSize() const {return comm_sz_;}

    void AppendHash(std::string to_append);  // extend the file name
    unsigned long GetHash(std::string s);
    std::string ultos(unsigned long ul);
    std::string ExtractHash();
};
