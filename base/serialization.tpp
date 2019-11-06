/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

template <class T>
ibinstream& operator<<(ibinstream& m, const T* p) {
    return m << *p;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const vector<T>& v) {
    m << v.size();
    for (typename vector<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const list<T>& v) {
    m << v.size();
    for (typename list<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const set<T>& v) {
    m << v.size();
    for (typename set<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <class T1, class T2>
ibinstream& operator<<(ibinstream& m, const pair<T1, T2>& v) {
    m << v.first;
    m << v.second;
    return m;
}

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const map<KeyT, ValT>& v) {
    m << v.size();
    for (typename map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << it->first;
        m << it->second;
    }
    return m;
}

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const hash_map<KeyT, ValT>& v) {
    m << v.size();
    for (typename hash_map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << it->first;
        m << it->second;
    }
    return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const hash_set<T>& v) {
    m << v.size();
    for (typename hash_set<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <class T, class _HashFcn,  class _EqualKey >
ibinstream& operator<<(ibinstream& m, const hash_set<T, _HashFcn, _EqualKey>& v) {
    m << v.size();
    for (typename hash_set<T, _HashFcn, _EqualKey>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, T*& p) {
    p = new T;
    return m >> (*p);
}

template <class T>
obinstream& operator>>(obinstream& m, vector<T>& v) {
    size_t size;
    m >> size;
    v.resize(size);
    for (typename vector<T>::iterator it = v.begin(); it != v.end(); ++it) {
        m >> *it;
    }
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, list<T>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T tmp;
        m >> tmp;
        v.push_back(tmp);
    }
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, set<T>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T tmp;
        m >> tmp;
        v.insert(v.end(), tmp);
    }
    return m;
}

template <class T1, class T2>
obinstream& operator>>(obinstream& m, pair<T1, T2>& v) {
    m >> v.first;
    m >> v.second;
    return m;
}

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, map<KeyT, ValT>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        KeyT key;
        m >> key;
        m >> v[key];
    }
    return m;
}

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, hash_map<KeyT, ValT>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        KeyT key;
        m >> key;
        m >> v[key];
    }
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, hash_set<T>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T key;
        m >> key;
        v.insert(key);
    }
    return m;
}

template <class T, class _HashFcn,  class _EqualKey >
obinstream& operator>>(obinstream& m, hash_set<T, _HashFcn, _EqualKey>& v) {
    v.clear();
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T key;
        m >> key;
        v.insert(key);
    }
    return m;
}
