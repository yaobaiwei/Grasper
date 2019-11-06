/* Copyright 2019 Husky Data Lab, CUHK

Authors: Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#include "base/predicate.hpp"

bool operator ==(const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if ((v1.type == 1 && v2.type == 2) || (v1.type == 2 && v2.type == 1)) {
            return Tool::DebugString(const_cast<value_t&>(v1)) == Tool::DebugString(const_cast<value_t&>(v2));
        }
        return false;
    }
    return v1.content == v2.content;
}

bool operator !=(const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if ((v1.type == 1 && v2.type == 2) || (v1.type == 2 && v2.type == 1)) {
            return Tool::DebugString(const_cast<value_t&>(v1)) != Tool::DebugString(const_cast<value_t&>(v2));
        }
        return true;
    }
    return v1.content != v2.content;
}

bool operator <(const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if (v1.type == 1 && v2.type == 2) {
            return Tool::value_t2int(v1) < Tool::value_t2double(v2);
        } else if (v1.type == 2 && v2.type == 1) {
            return Tool::value_t2double(v1) < Tool::value_t2int(v2);
        }
        return v1.content < v2.content;
    }

    switch (v1.type) {
        case 1:     return Tool::value_t2int(v1) < Tool::value_t2int(v2);
        case 2:     return Tool::value_t2double(v1) < Tool::value_t2double(v2);
        case 5:     return Tool::value_t2uint64_t(v1) < Tool::value_t2uint64_t(v2);
        default:    return v1.content < v2.content;
    }
}

bool operator> (const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if (v1.type == 1 && v2.type == 2) {
            return Tool::value_t2int(v1) > Tool::value_t2double(v2);
        } else if (v1.type == 2 && v2.type == 1) {
            return Tool::value_t2double(v1) > Tool::value_t2int(v2);
        }
        return v1.content > v2.content;
    }

    switch (v1.type) {
        case 1:     return Tool::value_t2int(v1) > Tool::value_t2int(v2);
        case 2:     return Tool::value_t2double(v1) > Tool::value_t2double(v2);
        case 5:     return Tool::value_t2uint64_t(v1) > Tool::value_t2uint64_t(v2);
        default:    return v1.content > v2.content;
    }
}

bool operator<= (const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if (v1.type == 1 && v2.type == 2) {
            return Tool::value_t2int(v1) <= Tool::value_t2double(v2);
        } else if (v1.type == 2 && v2.type == 1) {
            return Tool::value_t2double(v1) <= Tool::value_t2int(v2);
        }
        return v1.content <= v2.content;
    }

    switch (v1.type) {
        case 1:     return Tool::value_t2int(v1) <= Tool::value_t2int(v2);
        case 2:     return Tool::value_t2double(v1) <= Tool::value_t2double(v2);
        case 5:     return Tool::value_t2uint64_t(v1) <= Tool::value_t2uint64_t(v2);
        default:    return v1.content <= v2.content;
    }
}

bool operator>= (const value_t& v1, const value_t& v2) {
    if (v1.type != v2.type) {
        if (v1.type == 1 && v2.type == 2) {
            return Tool::value_t2int(v1) >= Tool::value_t2double(v2);
        } else if (v1.type == 2 && v2.type == 1) {
            return Tool::value_t2double(v1) >= Tool::value_t2int(v2);
        }
        return v1.content >= v2.content;
    }

    switch (v1.type) {
        case 1:     return Tool::value_t2int(v1) >= Tool::value_t2int(v2);
        case 2:     return Tool::value_t2double(v1) >= Tool::value_t2double(v2);
        case 5:     return Tool::value_t2uint64_t(v1) >= Tool::value_t2uint64_t(v2);
        default:    return v1.content >= v2.content;
    }
}

bool Evaluate(PredicateValue & pv, const value_t *value) {
    assert(pv.values.size() > 0);

    // no value
    if (value == NULL) {
        return pv.pred_type == Predicate_T::NONE;
    }

    // has value
    switch (pv.pred_type) {
    case Predicate_T::ANY:
        return true;
    case Predicate_T::NONE:
        return false;
    case Predicate_T::EQ:
        return *value == pv.values[0];
    case Predicate_T::NEQ:
        return *value != pv.values[0];
    case Predicate_T::LT:
        return *value < pv.values[0];
    case Predicate_T::LTE:
        return *value <= pv.values[0];
    case Predicate_T::GT:
        return *value > pv.values[0];
    case Predicate_T::GTE:
        return *value >= pv.values[0];
    case Predicate_T::INSIDE:
        assert(pv.values.size() == 2);
        return *value > pv.values[0] && *value < pv.values[1];
    case Predicate_T::OUTSIDE:
        assert(pv.values.size() == 2);
        return *value < pv.values[0] || *value > pv.values[1];
    case Predicate_T::BETWEEN:
        assert(pv.values.size() == 2);
        return *value >= pv.values[0] && *value <= pv.values[1];
    case Predicate_T::WITHIN:
        for (auto v : pv.values) {
            if (v == *value) {
                return true;
            }
        }
        return false;
    case Predicate_T::WITHOUT:
        for (auto v : pv.values) {
            if (v == *value) {
                return false;
            }
        }
        return true;
    }
}

bool Evaluate(Predicate_T pred_type, value_t & val1, value_t & val2) {
    switch (pred_type) {
        case Predicate_T::ANY:
            return true;
        case Predicate_T::NONE:
            return false;
        case Predicate_T::EQ:
            return val1 == val2;
        case Predicate_T::NEQ:
            return val1 != val2;
        case Predicate_T::LT:
            return val1 < val2;
        case Predicate_T::LTE:
            return val1 <= val2;
        case Predicate_T::GT:
            return val1 > val2;
        case Predicate_T::GTE:
            return val1 >= val2;
    }
}
