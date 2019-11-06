/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef CLIENT_CONNECTION_HPP_
#define CLIENT_CONNECTION_HPP_

#include <vector>

#include "base/node.hpp"
#include "base/serialization.hpp"
#include "third_party/zmq.hpp"

class ClientConnection {
 public:
    ~ClientConnection();
    void Init(vector<Node> & nodes);
    void Send(int nid, ibinstream & m);
    void Recv(int nid, obinstream & um);

 private:
    zmq::context_t context_;
    vector<zmq::socket_t *> senders_;
    vector<zmq::socket_t *> receivers_;
};

#endif /* CLIENT_CONNECTION_HPP_ */
