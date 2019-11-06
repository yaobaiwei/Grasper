/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/


#ifndef SERIALIZATION_HPP_
#define SERIALIZATION_HPP_

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <ext/hash_set>
#include <ext/hash_map>
#include <sys/stat.h>

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;
using namespace std;

class ibinstream {
 public:
    char* get_buf();

    void raw_byte(char c);

    void raw_bytes(const void* ptr, int size);

    size_t size();

    void clear();

 private:
    vector<char> buf_;
};

ibinstream& operator<<(ibinstream& m, size_t i);

ibinstream& operator<<(ibinstream& m, bool i);

ibinstream& operator<<(ibinstream& m, int i);

ibinstream& operator<<(ibinstream& m, double i);

ibinstream& operator<<(ibinstream& m, uint8_t i);

ibinstream& operator<<(ibinstream& m, uint16_t i);

ibinstream& operator<<(ibinstream& m, uint32_t i);

ibinstream& operator<<(ibinstream& m, unsigned long long i);

ibinstream& operator<<(ibinstream& m, char c);

template <class T>
ibinstream& operator<<(ibinstream& m, const T* p);

template <class T>
ibinstream& operator<<(ibinstream& m, const vector<T>& v);

ibinstream& operator<<(ibinstream& m, const vector<int>& v);

ibinstream& operator<<(ibinstream& m, const vector<double>& v);

ibinstream& operator<<(ibinstream& m, const vector<char>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const list<T>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const set<T>& v);

ibinstream& operator<<(ibinstream& m, const string& str);

template <class T1, class T2>
ibinstream& operator<<(ibinstream& m, const pair<T1, T2>& v);

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const map<KeyT, ValT>& v);

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const hash_map<KeyT, ValT>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const hash_set<T>& v);

template <class T, class _HashFcn,  class _EqualKey >
ibinstream& operator<<(ibinstream& m, const hash_set<T, _HashFcn, _EqualKey>& v);

class obinstream {
 public:
    obinstream();
    obinstream(char* b, size_t s);
    obinstream(char* b, size_t s, size_t idx);
    ~obinstream();

    char raw_byte();
    void* raw_bytes(unsigned int n_bytes);

    void assign(char* b, size_t s, size_t idx = 0);

    void clear();

    bool end();

 private:
    char* buf_;  // responsible for deleting the buffer, do not delete outside
    size_t size_;
    size_t index_;
};

obinstream& operator>>(obinstream& m, size_t& i);

obinstream& operator>>(obinstream& m, bool& i);

obinstream& operator>>(obinstream& m, int& i);

obinstream& operator>>(obinstream& m, double& i);

obinstream& operator>>(obinstream& m, uint8_t& i);

obinstream& operator>>(obinstream& m, uint16_t& i);

obinstream& operator>>(obinstream& m, uint32_t& i);

obinstream& operator>>(obinstream& m, unsigned long long& i);

obinstream& operator>>(obinstream& m, char& c);

template <class T>
obinstream& operator>>(obinstream& m, T*& p);

template <class T>
obinstream& operator>>(obinstream& m, vector<T>& v);

obinstream& operator>>(obinstream& m, vector<int>& v);

obinstream& operator>>(obinstream& m, vector<double>& v);

obinstream& operator>>(obinstream& m, vector<char>& v);

template <class T>
obinstream& operator>>(obinstream& m, list<T>& v);

template <class T>
obinstream& operator>>(obinstream& m, set<T>& v);

obinstream& operator>>(obinstream& m, string& str);

template <class T1, class T2>
obinstream& operator>>(obinstream& m, pair<T1, T2>& v);

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, map<KeyT, ValT>& v);

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, hash_map<KeyT, ValT>& v);

template <class T>
obinstream& operator>>(obinstream& m, hash_set<T>& v);

template <class T, class _HashFcn,  class _EqualKey >
obinstream& operator>>(obinstream& m, hash_set<T, _HashFcn, _EqualKey>& v);

#include "serialization.tpp"

#endif /* SERIALIZATION_HPP_ */
