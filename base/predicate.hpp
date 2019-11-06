/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#pragma once

#include "base/type.hpp"
#include "utils/tool.hpp"

bool operator ==(const value_t& v1, const value_t& v2);

bool operator !=(const value_t& v1, const value_t& v2);

bool operator <(const value_t& v1, const value_t& v2);

bool operator >(const value_t& v1, const value_t& v2);

bool operator <=(const value_t& v1, const value_t& v2);

bool operator >=(const value_t& v1, const value_t& v2);

struct PredicateValue {
    Predicate_T pred_type;
    vector<value_t> values;

    PredicateValue(Predicate_T _pred_type, vector<value_t> _values) : pred_type(_pred_type), values(_values) {}

    PredicateValue(Predicate_T _pred_type, value_t value) : pred_type(_pred_type) {
        Tool::value_t2vec(value, values);
    }
};

struct PredicateHistory {
    Predicate_T pred_type;
    vector<int> history_step_labels;

    PredicateHistory(Predicate_T _pred_type, vector<int> _step_labels) : pred_type(_pred_type), history_step_labels(_step_labels) {}
};

bool Evaluate(PredicateValue & pv, const value_t *value = NULL);

bool Evaluate(Predicate_T pred_type, value_t & val1, value_t & val2);
