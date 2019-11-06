/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

template <class T>
void send_data(Node & node, const T& data, int dst, bool is_global, int tag) {
    ibinstream m;
    m << data;
    if (is_global)
        send_ibinstream(m, dst, MPI_COMM_WORLD, tag);
    else
        send_ibinstream(m, dst, node.local_comm, tag);
}

template <class T>
T recv_data(Node & node, int src, bool is_global, int tag) {
    MPI_Comm world;
    if (is_global)
        world = MPI_COMM_WORLD;
    else
        world = node.local_comm;
    obinstream um;
    recv_obinstream(um, src, world, tag);
    T data;
    um >> data;
    return data;
}

// ============================================
// all-to-all
template <class T>
void all_to_all(Node & node, bool is_global, std::vector<T>& to_exchange) {
    // for each to_exchange[i]
    // send out *to_exchange[i] to i
    // save received data in *to_exchange[i]
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                // send
                ibinstream m;
                m << to_exchange[partner];
                send_ibinstream(m, partner, world);

                // receive
                obinstream um;
                recv_obinstream(um, partner, world);
                um >> to_exchange[partner];
            } else {
                // receive
                obinstream um;
                recv_obinstream(um, partner, world);
                T received;
                um >> received;

                // send
                ibinstream m;
                m << to_exchange[partner];
                send_ibinstream(m, partner, world);
                to_exchange[partner] = received;
            }
        }
    }
}

template <class T>
void all_to_all(Node & node, bool is_global, vector<vector<T*> > & to_exchange) {
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                // send
                ibinstream * m = new ibinstream;
                *m << to_exchange[partner];
                for (int k = 0; k < to_exchange[partner].size(); k++)
                    delete to_exchange[partner][k];
                vector<T*>().swap(to_exchange[partner]);

                send_ibinstream(*m, partner, world);
                delete m;

                // receive
                obinstream um;
                recv_obinstream(um, partner, world);
                um >> to_exchange[partner];
            } else {
                // receive
                obinstream um;
                recv_obinstream(um, partner, world);

                // send
                ibinstream * m = new ibinstream;
                *m << to_exchange[partner];
                for (int k = 0; k < to_exchange[partner].size(); k++)
                    delete to_exchange[partner][k];
                vector<T*>().swap(to_exchange[partner]);

                send_ibinstream(*m, partner, world);
                delete m;

                um >> to_exchange[partner];
            }
        }
    }
}

template <class T, class T1>
void all_to_all(Node & node, bool is_global, vector<T>& to_send, vector<T1>& to_get) {
    // for each to_exchange[i]
    // send out *to_exchange[i] to i
    // save received data in *to_exchange[i]
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                // send
                ibinstream m;
                m << to_send[partner];
                send_ibinstream(m, partner, world);

                // receive
                obinstream um;
                recv_obinstream(um, partner, world);
                um >> to_get[partner];
            } else {
                // receive
                obinstream um;
                recv_obinstream(um, partner, world);
                T1 received;
                um >> received;

                // send
                ibinstream m;
                m << to_send[partner];
                send_ibinstream(m, partner, world);
                to_get[partner] = received;
            }
        }
    }
}

template <class T, class T1>
void all_to_all_cat(Node & node, bool is_global, std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2) {
    // for each to_exchange[i]
    // send out *to_exchange[i] to i
    // save received data in *to_exchange[i]
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                // send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];

                send_ibinstream(m, partner, world);

                // receive
                obinstream um;
                recv_obinstream(um, partner, world);

                um >> to_exchange1[partner];
                um >> to_exchange2[partner];
            } else {
                // receive
                obinstream um;
                recv_obinstream(um, partner, world);

                T received1;
                T1 received2;
                um >> received1;
                um >> received2;
                // send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];

                send_ibinstream(m, partner, world);

                to_exchange1[partner] = received1;
                to_exchange2[partner] = received2;
            }
        }
    }
}

template <class T, class T1, class T2>
void all_to_all_cat(Node & node, bool is_global,
                    std::vector<T>& to_exchange1,
                    std::vector<T1>& to_exchange2,
                    std::vector<T2>& to_exchange3) {
    // for each to_exchange[i]
    // send out *to_exchange[i] to i
    // save received data in *to_exchange[i]
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                // send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                m << to_exchange3[partner];

                send_ibinstream(m, partner, world);

                // receive
                obinstream um;
                recv_obinstream(um, partner, world);

                um >> to_exchange1[partner];
                um >> to_exchange2[partner];
                um >> to_exchange3[partner];
            } else {
                // receive
                obinstream um;
                recv_obinstream(um, partner, world);

                T received1;
                T1 received2;
                T2 received3;
                um >> received1;
                um >> received2;
                um >> received3;
                // send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                m << to_exchange3[partner];

                send_ibinstream(m, partner, world);

                to_exchange1[partner] = received1;
                to_exchange2[partner] = received2;
                to_exchange3[partner] = received3;
            }
        }
    }
}

// ============================================
// scatter
template <class T>
void master_scatter(Node & node, bool is_global, vector<T>& to_send) {
    // scatter
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    int* sendcounts = new int[np];
    int recvcount;
    int* sendoffset = new int[np];

    ibinstream m;

    int size = 0;
    for (int i = 0; i < np; i++) {
        if (i == me) {
            sendcounts[i] = 0;
        } else {
            m << to_send[i];
            sendcounts[i] = m.size() - size;
            size = m.size();
        }
    }

    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, world);

    for (int i = 0; i < np; i++) {
        sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
    }
    char* sendbuf = m.get_buf();  // ibinstream will delete it
    char* recvbuf;

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, world);

    delete[] sendcounts;
    delete[] sendoffset;
}

template <class T>
void slave_scatter(Node & node, bool is_global, T& to_get) {
    // scatter
    MPI_Comm world;
    if (is_global) {
        world = MPI_COMM_WORLD;
    } else {
        world = node.local_comm;
    }

    int* sendcounts;
    int recvcount;
    int* sendoffset;

    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, world);

    char* sendbuf;
    char* recvbuf = new char[recvcount];  // obinstream will delete it

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, world);

    obinstream um(recvbuf, recvcount);
    um >> to_get;
}

// ================================================================
// gather
template <class T>
void master_gather(Node & node, bool is_global, vector<T>& to_get) {
    // gather
    int np;
    int me;
    MPI_Comm world;
    if (is_global) {
        np = node.get_world_size();
        me = node.get_world_rank();
        world = MPI_COMM_WORLD;
    } else {
        np = node.get_local_size();
        me = node.get_local_rank();
        world = node.local_comm;
    }

    int sendcount = 0;
    int* recvcounts = new int[np];
    int* recvoffset = new int[np];

    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, world);

    for (int i = 0; i < np; i++) {
        recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
    }

    char* sendbuf;
    int recv_tot = recvoffset[np - 1] + recvcounts[np - 1];
    char* recvbuf = new char[recv_tot];  // obinstream will delete it

    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, world);

    obinstream um(recvbuf, recv_tot);
    for (int i = 0; i < np; i++) {
        if (i == me)
            continue;
        um >> to_get[i];
    }

    delete[] recvcounts;
    delete[] recvoffset;
}

template <class T>
void slave_gather(Node & node, bool is_global, T& to_send) {
    // gather
    // scatter
    MPI_Comm world;
    if (is_global) {
        world = MPI_COMM_WORLD;
    } else {
        world = node.local_comm;
    }

    int sendcount;
    int* recvcounts;
    int* recvoffset;

    ibinstream m;
    m << to_send;
    sendcount = m.size();

    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, world);

    char* sendbuf = m.get_buf();  // ibinstream will delete it
    char* recvbuf;

    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, world);
}

// ================================================================
// bcast
template <class T>
void master_bcast(Node & node, bool is_global, T& to_send) {
    // broadcast
    MPI_Comm world;
    if (is_global) {
        world = MPI_COMM_WORLD;
    } else {
        world = node.local_comm;
    }

    ibinstream m;
    m << to_send;
    int size = m.size();

    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, world);

    char* sendbuf = m.get_buf();
    MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, world);
}

template <class T>
void slave_bcast(Node & node, bool is_global, T& to_get) {
    // broadcast
    MPI_Comm world;
    if (is_global) {
        world = MPI_COMM_WORLD;
    } else {
        world = node.local_comm;
    }

    int size;
    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, world);

    char* recvbuf = new char[size];  // obinstream will delete it
    MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, world);

    obinstream um(recvbuf, size);
    um >> to_get;
}
