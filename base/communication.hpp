/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef COMMUNICATION_HPP_
#define COMMUNICATION_HPP_

#include <mpi.h>
#include <vector>

#include "base/node.hpp"
#include "base/serialization.hpp"
#include "utils/global.hpp"
#include "utils/unit.hpp"

using namespace std;
// ============================================

int all_sum(int my_copy, MPI_Comm world = MPI_COMM_WORLD);

long long master_sum_LL(long long my_copy, MPI_Comm world = MPI_COMM_WORLD);

long long all_sum_LL(long long my_copy, MPI_Comm world = MPI_COMM_WORLD);

char all_bor(char my_copy, MPI_Comm world = MPI_COMM_WORLD);

bool all_lor(bool my_copy, MPI_Comm world = MPI_COMM_WORLD);

bool all_land(bool my_copy, MPI_Comm world = MPI_COMM_WORLD);

// ============================================

void send(void* buf, int size, int dst, MPI_Comm world, int tag = COMMUN_CHANNEL);

int recv(void* buf, int size, int src, MPI_Comm world, int tag = COMMUN_CHANNEL);  // return the actual source, since "src" can be MPI_ANY_SOURCE

// ============================================

void send_ibinstream(ibinstream& m, int dst, MPI_Comm world, int tag = COMMUN_CHANNEL);

void recv_obinstream(obinstream& m, int src, MPI_Comm world, int tag = COMMUN_CHANNEL);

// ============================================
// obj-level send/recv
template <class T>
void send_data(Node & node, const T& data, int dst, bool is_global, int tag = COMMUN_CHANNEL);

template <class T>
T recv_data(Node & node, int src, bool is_global, int tag = COMMUN_CHANNEL);

// ============================================
// all-to-all
template <class T>
void all_to_all(Node & node, bool is_global, std::vector<T>& to_exchange);

template <class T>
void all_to_all(Node & node, bool is_global, vector<vector<T*>> & to_exchange);

template <class T, class T1>
void all_to_all(Node & node, bool is_global, vector<T>& to_send, vector<T1>& to_get);

template <class T, class T1>
void all_to_all_cat(Node & node, bool is_global, std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2);

template <class T, class T1, class T2>
void all_to_all_cat(Node & node, bool is_global, std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, std::vector<T2>& to_exchange3);

// ============================================
// scatter
template <class T>
void master_scatter(Node & node, bool is_global, vector<T>& to_send);

template <class T>
void slave_scatter(Node & node, bool is_global, T& to_get);

// ================================================================
// gather
template <class T>
void master_gather(Node & node, bool is_global, vector<T>& to_get);

template <class T>
void slave_gather(Node & node, bool is_global, T& to_send);

// ================================================================
// bcast
template <class T>
void master_bcast(Node & node, bool is_global, T& to_send);

template <class T>
void slave_bcast(Node & node, bool is_global, T& to_get);

#include "communication.tpp"

#endif /* COMMUNICATION_HPP_ */
