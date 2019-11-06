/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <assert.h>

#ifndef UNIT_HPP_
#define UNIT_HPP_

#define KiB2B(_x) ((_x) * 1024ul)
#define MiB2B(_x) (KiB2B((_x)) * 1024ul)
#define GiB2B(_x) (MiB2B((_x)) * 1024ul)

#define B2KiB(_x) ((_x) / 1024.0)
#define B2MiB(_x) (B2KiB((_x)) / 1024.0)
#define B2GiB(_x) (B2MiB((_x)) / 1024.0)

uint64_t inline floor(uint64_t original, uint64_t n) {
    assert(n != 0);
    return original - original % n;
}

uint64_t inline ceil(uint64_t original, uint64_t n) {
    assert(n != 0);
    if (original % n == 0)
        return original;
    return original - original % n + n;
}
#endif /* UNIT_HPP_ */
