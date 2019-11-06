/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <fstream>
#include <stdexcept>
#include <set>
#include <map>

#include "base/node_util.hpp"
#include "glog/logging.h"

std::vector<Node> ParseFile(const std::string& filename) {
    std::vector<Node> nodes;
    std::ifstream input_file(filename.c_str());
    CHECK(input_file.is_open()) << "Error opening file: " << filename;
    std::string line;
    int rank = 0;
    while (getline(input_file, line)) {
        size_t hostname_pos = line.find(":");
        CHECK_NE(hostname_pos, std::string::npos);
        std::string hostname = line.substr(0, hostname_pos);
        size_t ibname_pos = line.find(":", hostname_pos+1);
        CHECK_NE(ibname_pos, std::string::npos);
        std::string ibname = line.substr(hostname_pos+1, ibname_pos - hostname_pos - 1);
        size_t tcp_port_pos = line.find(":", ibname_pos+1);
        CHECK_NE(tcp_port_pos, std::string::npos);
        std::string tcp_port = line.substr(ibname_pos+1, tcp_port_pos - ibname_pos - 1);
        std::string rdma_port = line.substr(tcp_port_pos+1, line.size() - tcp_port_pos - 1);
        try {
            Node node;
            node.set_world_rank(rank++);
            node.hostname = std::move(hostname);
            node.ibname = std::move(ibname);
            node.tcp_port = std::stoi(tcp_port);
            node.rdma_port = std::stoi(rdma_port);
            nodes.push_back(std::move(node));
        }
        catch(const std::invalid_argument& ia) {
            LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
        }
    }
    return nodes;
}

Node & GetNodeById(std::vector<Node>& nodes, int id) {
    for (int i = 0; i < nodes.size(); i++) {
        if (id == nodes[i].get_world_rank()) {
          return nodes[i];
        }
    }
    CHECK(false) << "Node" << id << " is not in the given node list";
}

bool CheckUniquePort(std::vector<Node>& nodes) {
    for (const auto& node : nodes) {
        if (node.tcp_port == node.rdma_port)
            return false;
    }
    return true;
}

bool HasNode(std::vector<Node>& nodes, uint32_t id) {
    for (int i = 0; i < nodes.size(); i++) {
        if (nodes[i].get_world_rank() == id) {
          return true;
        }
    }
    return false;
}
