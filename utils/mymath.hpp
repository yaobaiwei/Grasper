/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <iostream>
#include <vector>
#include <assert.h>

class mymath {
 public:
    uint64_t static get_distribution(int r, std::vector<int>& distribution) {
        int sum = 0;
        for (int i = 0; i < distribution.size(); i++)
            sum += distribution[i];

        assert(sum > 0);
        r = r % sum;
        for (int i = 0; i < distribution.size(); i++) {
            if (r < distribution[i])
                return i;
            r -= distribution[i];
        }
        assert(false);
    }

    inline int static hash_mod(uint64_t n, int m) {
        if (m == 0)
            assert(false);
        return n % m;
    }

    // TomasWang's 64 bit integer hash
    static uint64_t hash_u64(uint64_t key) {
        key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8);  // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4);  // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
    }

    static uint64_t inverse_hash_u64(uint64_t key) {
        uint64_t tmp;

        // Invert key = key + (key << 31)
        tmp = key - (key << 31);
        key = key - (tmp << 31);

        // Invert key = key ^ (key >> 28)
        tmp = key ^ key >> 28;
        key = key ^ tmp >> 28;

        // Invert key *= 21
        key *= 14933078535860113213u;

        // Invert key = key ^ (key >> 14)
        tmp = key ^ key >> 14;
        tmp = key ^ tmp >> 14;
        tmp = key ^ tmp >> 14;
        key = key ^ tmp >> 14;

        // Invert key *= 265
        key *= 15244667743933553977u;

        // Invert key = key ^ (key >> 24)
        tmp = key ^ key >> 24;
        key = key ^ tmp >> 24;

        // Invert key = (~key) + (key << 21)
        tmp = ~key;
        tmp = ~(key - (tmp << 21));
        tmp = ~(key - (tmp << 21));
        key = ~(key - (tmp << 21));

        return key;
    }

    static uint64_t hash_prime_u64(uint64_t upper) {
        if (upper >= (1l << 31)) {
            std::cout << "WARNING: " << upper << " is too large!"
                 << std::endl;
            return upper;
        }

        if (upper >= 1610612741l) return 1610612741l;     // 2^30 ~ 2^31
        else if (upper >= 805306457l) return 805306457l;  // 2^29 ~ 2^30
        else if (upper >= 402653189l) return 402653189l;  // 2^28 ~ 2^29
        else if (upper >= 201326611l) return 201326611l;  // 2^27 ~ 2^28
        else if (upper >= 100663319l) return 100663319l;  // 2^26 ~ 2^27
        else if (upper >= 50331653l) return 50331653l;    // 2^25 ~ 2^26
        else if (upper >= 25165843l) return 25165843l;    // 2^24 ~ 2^25
        else if (upper >= 12582917l) return 12582917l;    // 2^23 ~ 2^24
        else if (upper >= 6291469l) return 6291469l;      // 2^22 ~ 2^23
        else if (upper >= 3145739l) return 3145739l;      // 2^21 ~ 2^22
        else if (upper >= 1572869l) return 1572869l;      // 2^20 ~ 2^21
        else if (upper >= 786433l) return 786433l;        // 2^19 ~ 2^20
        else if (upper >= 393241l) return 393241l;        // 2^18 ~ 2^19
        else if (upper >= 196613l) return 196613l;        // 2^17 ~ 2^18
        else if (upper >= 98317l) return 98317l;          // 2^16 ~ 2^17

        std::cout << "WARNING: " << upper << " is too small!"
             << std::endl;
        return upper;
    }

    // Hash128to64 function from Google's cityhash (available under the MIT License)
    static uint64_t hash_u128_to_u64(uint64_t high, uint64_t low) {
        // Murmur-inspired hashing.
        const uint64_t kMul = 0x9ddfea08eb382d69ULL;
        uint64_t a = (low ^ high) * kMul;
        a ^= (a >> 47);
        uint64_t b = (high ^ a) * kMul;
        b ^= (b >> 47);
        b *= kMul;
        return b;
    }

    static void hash_combine(size_t& seed, int v) {
        seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
};
