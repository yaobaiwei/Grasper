/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "storage/layout.hpp"

string Vertex::DebugString() const {
    stringstream ss;
    ss << "Vertex: { id = " << id.vid << " in_nbs = [";
    for (auto & vid : in_nbs)
        ss << vid.vid << ", ";
    ss << "] out_nbs = [";
    for (auto & vid : out_nbs)
        ss << vid.vid << ", ";
    ss << "] vp_list = [";
    for (auto & p : vp_list)
        ss << p << ", ";
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Vertex& v) {
    m << v.id;
    // m << v.label;
    m << v.in_nbs;
    m << v.out_nbs;
    m << v.vp_list;
    return m;
}

obinstream& operator>>(obinstream& m, Vertex& v) {
    m >> v.id;
    // m >> v.label;
    m >> v.in_nbs;
    m >> v.out_nbs;
    m >> v.vp_list;
    return m;
    return m;
}

string Edge::DebugString() const {
    stringstream ss;
    ss << "Edge: { id = " << id.in_v << "," << id.out_v <<  " ep_list = [";
    for (auto & ep : ep_list)
        ss << ep << ", ";
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Edge& e) {
    m << e.id;
    // m << e.label;
    m << e.ep_list;
    return m;
}

obinstream& operator>>(obinstream& m, Edge& e) {
    m >> e.id;
    // m >> e.label;
    m >> e.ep_list;
    return m;
}

string V_KVpair::DebugString() const {
    stringstream ss;
    ss << "V_KVpair: { key = " << key.vid << "|" << key.pid << ", value.type = " << (int)value.type << " }" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, V_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

string VProperty::DebugString() const {
    stringstream ss;
    ss << "VProperty: { id = " << id.vid <<  " plist = [" << endl;
    for (auto & vp : plist)
        ss << vp.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const VProperty& vp) {
    m << vp.id;
    m << vp.plist;
    return m;
}

obinstream& operator>>(obinstream& m, VProperty& vp) {
    m >> vp.id;
    m >> vp.plist;
    return m;
}

string E_KVpair::DebugString() const {
    stringstream ss;
    ss << "E_KVpair: { key = " << key.in_vid << "|" << key.out_vid << "|" << key.pid << ", value.type = " << (int)value.type << " }" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, E_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

string EProperty::DebugString() const {
    stringstream ss;
    ss << "EProperty: { id = " << id.in_v << "," << id.out_v <<  " plist = [" << endl;
    for (auto & ep : plist)
        ss << ep.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const EProperty& ep) {
    m << ep.id;
    m << ep.plist;
    return m;
}

obinstream& operator>>(obinstream& m, EProperty& ep) {
    m >> ep.id;
    m >> ep.plist;
    return m;
}
