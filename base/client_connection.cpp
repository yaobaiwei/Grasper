/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <iostream>
#include <stdio.h>
#include <string.h>

#include "base/client_connection.hpp"
#include "utils/global.hpp"

ClientConnection::~ClientConnection() {
    for (int i = 0 ; i < senders_.size(); i++) {
        delete senders_[i];
    }

    for (int i = 0 ; i < receivers_.size(); i++) {
        delete receivers_[i];
    }
}

void ClientConnection::Init(vector<Node> & nodes) {
    senders_.resize(nodes.size());
    for (int i = 0 ; i < nodes.size(); i++) {
        if (i == MASTER_RANK) {
            senders_[i] = new zmq::socket_t(context_, ZMQ_REQ);
        } else {
            senders_[i] = new zmq::socket_t(context_, ZMQ_PUSH);
        }
        char addr[64];
        sprintf(addr, "tcp://%s:%d", nodes[i].hostname.c_str(), nodes[i].tcp_port);
        senders_[i]->connect(addr);
    }

    receivers_.resize(nodes.size()-1);
    for (int i = 0 ; i < nodes.size()-1; i++) {
        receivers_[i] = new zmq::socket_t(context_, ZMQ_PULL);
        char addr[64];
        sprintf(addr, "tcp://*:%d", nodes[i+1].tcp_port + i + 1);
        receivers_[i]->bind(addr);
    }
}

void ClientConnection::Send(int nid, ibinstream & m) {
    zmq::message_t msg(m.size());
    memcpy((void *)msg.data(), m.get_buf(), m.size());
    senders_[nid]->send(msg);
}

void ClientConnection::Recv(int nid, obinstream & um) {
    zmq::message_t msg;
    if (nid == MASTER_RANK) {
        if (senders_[nid]->recv(&msg) < 0) {
            std::cout << "Client recvs with error " << strerror(errno) << std::endl;
            exit(-1);
        }
    } else {
        if (receivers_[nid - 1]->recv(&msg) < 0) {
            std::cout << "Client recvs with error " << strerror(errno) << std::endl;
            exit(-1);
        }
    }

    char* buf = new char[msg.size()];
    memcpy(buf, msg.data(), msg.size());
    um.assign(buf, msg.size(), 0);
}
