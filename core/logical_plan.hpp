/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef LOGICAL_PLAN_HPP_
#define LOGICAL_PLAN_HPP_

#include <vector>
#include "base/type.hpp"
#include "core/expert_object.hpp"

class LogicPlan {
 public:
    LogicPlan() {}
    explicit LogicPlan(qid_t id) : qid(id) {}
    void Feed(vector<Expert_Object> & experts_) {
        experts = move(experts_);
    }

    qid_t qid;
    vector<Expert_Object> experts;
};

#endif /* LOGICAL_PLAN_HPP_ */
