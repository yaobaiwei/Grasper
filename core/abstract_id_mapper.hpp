/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef ABSTRACT_IDMAPPER_HPP_
#define ABSTRACT_IDMAPPER_HPP_

#include <stdint.h>
#include "base/type.hpp"

class AbstractIdMapper {
 public:
    virtual ~AbstractIdMapper() {}

    virtual bool IsVertex(uint64_t v_id) = 0;
    virtual bool IsEdge(uint64_t e_id) = 0;
    virtual bool IsVProperty(uint64_t vp_id) = 0;
    virtual bool IsEProperty(uint64_t ep_id) = 0;

    virtual bool IsVertexLocal(const vid_t v_id) = 0;
    virtual bool IsEdgeLocal(const eid_t e_id) = 0;
    virtual bool IsVPropertyLocal(const vpid_t p_id) = 0;
    virtual bool IsEPropertyLocal(const epid_t p_id) = 0;

    // vertex/edge/property -> machine index mapping
    virtual int GetMachineIdForVertex(vid_t v_id) = 0;
    virtual int GetMachineIdForEdge(eid_t e_id) = 0;
    virtual int GetMachineIdForVProperty(vpid_t p_id) = 0;
    virtual int GetMachineIdForEProperty(epid_t p_id) = 0;
};

#endif /* ABSTRACT_IDMAPPER_HPP_ */
