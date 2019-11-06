/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "base/serialization.hpp"
#include <iostream>

char* ibinstream::get_buf() {
    return &buf_[0];
}

void ibinstream::raw_byte(char c) {
    buf_.push_back(c);
}

void ibinstream::raw_bytes(const void* ptr, int size) {
    buf_.insert(buf_.end(), (const char*)ptr, (const char*)ptr + size);
}

size_t ibinstream::size() {
    return buf_.size();
}

void ibinstream::clear() {
    buf_.clear();
}

ibinstream& operator<<(ibinstream& m, size_t i) {
    m.raw_bytes(&i, sizeof(size_t));
    return m;
}

ibinstream& operator<<(ibinstream& m, bool i) {
    m.raw_bytes(&i, sizeof(bool));
    return m;
}

ibinstream& operator<<(ibinstream& m, int i) {
    m.raw_bytes(&i, sizeof(int));
    return m;
}

ibinstream& operator<<(ibinstream& m, double i) {
    m.raw_bytes(&i, sizeof(double));
    return m;
}

ibinstream& operator<<(ibinstream& m, unsigned long long i) {
    m.raw_bytes(&i, sizeof(unsigned long long));
    return m;
}

ibinstream& operator<<(ibinstream& m, uint8_t i) {
    m.raw_byte(i);
    return m;
}

ibinstream& operator<<(ibinstream& m, uint16_t i) {
    m.raw_bytes(&i, sizeof(uint16_t));
    return m;
}

ibinstream& operator<<(ibinstream& m, uint32_t i) {
    m.raw_bytes(&i, sizeof(uint32_t));
    return m;
}

// ibinstream& operator<<(ibinstream& m, uint64_t i)
// {
//     m.raw_bytes(&i, sizeof(uint64_t));
//     return m;
// }

ibinstream& operator<<(ibinstream& m, char c) {
    m.raw_byte(c);
    return m;
}

ibinstream& operator<<(ibinstream& m, const vector<int>& v) {
    m << v.size();
    m.raw_bytes(&v[0], v.size() * sizeof(int));
    return m;
}

ibinstream& operator<<(ibinstream& m, const vector<double>& v) {
    m << v.size();
    m.raw_bytes(&v[0], v.size() * sizeof(double));
    return m;
}

ibinstream& operator<<(ibinstream& m, const vector<char>& v) {
    m << v.size();
    m.raw_bytes(&v[0], v.size() * sizeof(char));
    return m;
}

ibinstream& operator<<(ibinstream& m, const string& str) {
    m << str.length();
    m.raw_bytes(str.c_str(), str.length());
    return m;
}


obinstream::obinstream() : buf_(NULL), size_(0), index_(0) {}
obinstream::obinstream(char* b, size_t s) : buf_(b), size_(s), index_(0) {}
obinstream::obinstream(char* b, size_t s, size_t idx) : buf_(b), size_(s), index_(idx) {}
obinstream::~obinstream() {
    delete[] buf_;
}

char obinstream::raw_byte() {
    return buf_[index_++];
}

void* obinstream::raw_bytes(unsigned int n_bytes) {
    char* ret = buf_ + index_;
    index_ += n_bytes;
    return ret;
}

void obinstream::assign(char* b, size_t s, size_t idx) {
    buf_ = b;
    size_ = s;
    index_ = idx;
}

void obinstream::clear() {
    delete[] buf_;
    buf_ = NULL;
    size_ = index_ = 0;
}

bool obinstream::end() {
    return index_ >= size_;
}

obinstream& operator>>(obinstream& m, size_t& i) {
    i = *(size_t*)m.raw_bytes(sizeof(size_t));
    return m;
}

obinstream& operator>>(obinstream& m, bool& i) {
    i = *(bool*)m.raw_bytes(sizeof(bool));
    return m;
}

obinstream& operator>>(obinstream& m, int& i) {
    i = *(int*)m.raw_bytes(sizeof(int));
    return m;
}

obinstream& operator>>(obinstream& m, double& i) {
    i = *(double*)m.raw_bytes(sizeof(double));
    return m;
}

obinstream& operator>>(obinstream& m, uint8_t& i) {
    i = m.raw_byte();
    return m;
}

obinstream& operator>>(obinstream& m, uint16_t& i) {
    i = *(uint16_t*)m.raw_bytes(sizeof(uint16_t));
    return m;
}

obinstream& operator>>(obinstream& m, uint32_t& i) {
    i = *(uint32_t*)m.raw_bytes(sizeof(uint32_t));
    return m;
}

// obinstream& operator>>(obinstream& m, uint64_t& i)
// {
//     i = *(uint64_t*)m.raw_bytes(sizeof(uint64_t));
//     return m;
// }

obinstream& operator>>(obinstream& m, unsigned long long& i) {
    i = *(unsigned long long*)m.raw_bytes(sizeof(unsigned long long));
    return m;
}

obinstream& operator>>(obinstream& m, char& c) {
    c = m.raw_byte();
    return m;
}

obinstream& operator>>(obinstream& m, vector<int>& v) {
    size_t size;
    m >> size;
    v.resize(size);
    int* data = (int*)m.raw_bytes(sizeof(int) * size);
    v.assign(data, data + size);
    return m;
}

obinstream& operator>>(obinstream& m, vector<double>& v) {
    size_t size;
    m >> size;
    v.resize(size);
    double* data = (double*)m.raw_bytes(sizeof(double) * size);
    v.assign(data, data + size);
    return m;
}

obinstream& operator>>(obinstream& m, vector<char>& v) {
    size_t size;
    m >> size;
    v.resize(size);
    char* data = (char*)m.raw_bytes(sizeof(char) * size);
    v.assign(data, data + size);
    return m;
}

obinstream& operator>>(obinstream& m, string& str) {
    size_t length;
    m >> length;
    str.clear();
    char* data = (char*)m.raw_bytes(length);
    str.append(data, length);
    return m;
}
