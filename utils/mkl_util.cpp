/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "mkl_util.hpp"
#include <fstream>

using namespace std;

static __inline__ unsigned long long GetCycleCount() {
    unsigned hi, lo;
    __asm__ volatile("rdtsc":"=a"(lo), "=d"(hi));
    return ((unsigned long long)lo)|(((unsigned long long)hi) << 32);
}

MKLUtil::MKLUtil(int tid) {
    tid_ = tid;
    int cc = GetCycleCount();
    vslNewStream(&rng_stream_, VSL_BRNG_SFMT19937, cc);
}

void MKLUtil::Test() {
    double r[1000];  /* buffer for random numbers */
    double s;  /* average */
    VSLStreamStatePtr stream;
    int i, j;

    /* Initializing */
    s = 0.0;
    vslNewStream(&stream, VSL_BRNG_MT19937, 777);

    /* Generating */
    for (i = 0; i < 10; i++) {
        vdRngGaussian(VSL_RNG_METHOD_GAUSSIAN_ICDF, stream, 1000, r, 5.0, 2.0);
        for (j = 0; j < 1000; j++) {
            s += r[j];
        }
    }
    s /= 10000.0;

    /* Deleting the stream */
    vslDeleteStream(&stream);

    /* Printing results */
    printf("Sample mean of normal distribution = %f\n", s);
}

bool MKLUtil::UniformRNGI4(int* dst, int len, int min, int max) {
    int status = viRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max + 1);

    return status == VSL_STATUS_OK;
}

bool MKLUtil::UniformRNGF4(float* dst, int len, float min, float max) {
    int status = vsRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max);

    return status == VSL_STATUS_OK;
}

bool MKLUtil::UniformRNGF8(double* dst, int len, double min, double max) {
    int status = vdRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max);

    return status == VSL_STATUS_OK;
}
