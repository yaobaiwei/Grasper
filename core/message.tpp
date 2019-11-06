/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

template<class T1, class T2>
size_t MemSize(const pair<T1, T2>& p) {
    size_t s = MemSize(p.first);
    s += MemSize(p.second);
    return s;
}

template<class T>
size_t MemSize(const vector<T>& data) {
    size_t s = sizeof(size_t);
    for (auto& t : data) {
        s += MemSize(t);
    }
    return s;
}
