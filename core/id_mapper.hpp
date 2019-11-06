/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef IDMAPPER_HPP_
#define IDMAPPER_HPP_

#include <vector>

#include "core/abstract_id_mapper.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/mymath.hpp"
#include "base/type.hpp"
#include "base/node.hpp"

#include "glog/logging.h"

static uint64_t _VIDFLAG = 0xFFFFFFFFFFFFFFFF >> (64-VID_BITS);
static uint64_t _PIDLFLAG = 0xFFFFFFFFFFFFFFFF >> (64- 2*VID_BITS);

class NaiveIdMapper : public AbstractIdMapper {
 public:
    NaiveIdMapper(Node & node) : my_node_(node) {
        config_ = Config::GetInstance();
    }

    bool IsVertex(uint64_t v_id) {
        bool has_v = v_id & _VIDFLAG;
        return has_v;
    }

    bool IsEdge(uint64_t e_id) {
        bool has_out_v = e_id & _VIDFLAG;
        e_id >>= VID_BITS;
        bool has_in_v = e_id & _VIDFLAG;
        return has_out_v && has_in_v;
    }

    bool IsVProperty(uint64_t vp_id) {
        bool has_p = vp_id & _PIDLFLAG;
        vp_id >>= PID_BITS;
        vp_id >>= VID_BITS;
        bool has_v = vp_id & _VIDFLAG;
        return has_p && has_v;
    }

    bool IsEProperty(uint64_t ep_id) {
        bool has_p = ep_id & _PIDLFLAG;
        ep_id >>= PID_BITS;
        bool has_out_v = ep_id & _VIDFLAG;
        ep_id >>= VID_BITS;
        bool has_in_v = ep_id & _VIDFLAG;
        return has_p && has_out_v && has_in_v;
    }

    // judge if vertex/edge/property local
    bool IsVertexLocal(const vid_t v_id) {
        return GetMachineIdForVertex(v_id) == my_node_.get_local_rank();
    }

    bool IsEdgeLocal(const eid_t e_id) {
        return GetMachineIdForEdge(e_id) == my_node_.get_local_rank();
    }

    bool IsVPropertyLocal(const vpid_t vp_id) {
        return GetMachineIdForVProperty(vp_id) == my_node_.get_local_rank();
    }

    bool IsEPropertyLocal(const epid_t ep_id) {
        return GetMachineIdForEProperty(ep_id) == my_node_.get_local_rank();
    }

    // vertex/edge/property -> machine index mapping
    int GetMachineIdForVertex(vid_t v_id) {
        return mymath::hash_mod(v_id.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEdge(eid_t e_id) {
        return mymath::hash_mod(e_id.hash(), my_node_.get_local_size());
    }

// #define BY_EV_ID
#ifdef BY_EV_ID
    int GetMachineIdForVProperty(vpid_t vp_id) {
        vid_t v(vp_id.vid);
        return mymath::hash_mod(v.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEProperty(epid_t ep_id) {
        eid_t e(ep_id.in_vid, ep_id.out_vid);
        return mymath::hash_mod(e.hash(), my_node_.get_local_size());
    }
#else
    int GetMachineIdForVProperty(vpid_t vp_id) {
        return mymath::hash_mod(vp_id.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEProperty(epid_t ep_id) {
        return mymath::hash_mod(ep_id.hash(), my_node_.get_local_size());
    }
#endif

 private:
    Config * config_;
    Node my_node_;
};

#endif /* IDMAPPER_HPP_ */
