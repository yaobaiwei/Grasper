/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "cpuinfo_util.hpp"

CPUInfoUtil::CPUInfoUtil() {
    // read the whole cpuinfo
    FILE* fp;
    fp = fopen("/proc/cpuinfo", "r");

    char *buffer = NULL;
    size_t len = 0;
    int nread;

    int siblings_val;
    int cpu_cores_val;

    while (nread = getline(&buffer, &len, fp) != -1) {
        int val = 0;

        char* cptr = strstr(buffer, ":");

        // to make the code looks short, do much more redundant computation here
        // actually this is extremely low efficiency
        // but who cares, just once
        if (cptr != NULL)
        while (*cptr != '\n' && *cptr != 0) {
            if (*cptr >= '0' && *cptr <= '9') {
                val *= 10;
                val += *cptr - '0';
            }
            cptr++;
        }

        if (strstr(buffer, "processor") !=NULL) {
            processor_.push_back(val);
        }

        if (strstr(buffer, "physical id") !=NULL) {
            physical_id_.push_back(val);

            if (physical_id_map_.count(val) == 0)
                physical_id_map_.insert(std::make_pair(val, 1));
            else
                physical_id_map_[val]++;
        }

        if (strstr(buffer, "siblings") !=NULL) {
            siblings_.push_back(val);
            siblings_val = val;
        }

        if (strstr(buffer, "core id") !=NULL) {
            core_id_.push_back(val);
        }

        if (strstr(buffer, "cpu cores") !=NULL) {
            cpu_cores_.push_back(val);
            cpu_cores_val = val;
        }
    }

    fclose(fp);

    // check avail
    for (auto _val : siblings_)
        assert(_val == siblings_val);
    for (auto _val : cpu_cores_)
        assert(_val == cpu_cores_val);

    // get constant
    total_thread_cnt_ = processor_.size();
    thread_per_socket_ = siblings_val;
    thread_per_core_ = siblings_val / cpu_cores_val;
    total_core_cnt_ = total_thread_cnt_ / thread_per_core_;
    total_socket_cnt_ = physical_id_map_.size();
    core_per_socket_ = total_core_cnt_ / total_socket_cnt_;

    // socket mapping can be directly read from physical_id
    socket_mapping_ = physical_id_;

    // construct the map of printed core_id to continual core_id starts from 0
    for (auto core_id : core_id_)
        core_id_map_.insert(std::make_pair(core_id, core_id));

    int act_local_core_id = 0;
    for (auto & kv : core_id_map_) {
        kv.second = act_local_core_id;
        act_local_core_id++;
    }

    // construct core_in_socket_mapping_ according the core_id_map_
    for (auto core_id : core_id_)
        core_in_socket_mapping_.push_back(core_id_map_[core_id]);

    // construct core_in_global_mapping_
    for (int i = 0; i < core_in_socket_mapping_.size(); i++) {
        core_in_global_mapping_.push_back(socket_mapping_[i] * core_per_socket_ + core_in_socket_mapping_[i]);
    }

    // build core_to_tid_map_
    core_to_tid_map_.resize(total_core_cnt_);
    // for(auto global_core_id : core_in_global_mapping_)
    for (int i = 0; i < total_thread_cnt_; i++) {
        int global_core_id = core_in_global_mapping_[i];
        core_to_tid_map_[global_core_id].push_back(i);
    }
}
