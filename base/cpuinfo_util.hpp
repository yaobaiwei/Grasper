/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string.h>
#include <iostream>
#include <assert.h>

class CPUInfoUtil {
 private:
    // core here means physical core
    // thread here means logical core
    // socket, or node, the physical CPU count, and the NUMA node count (maybe
    int total_core_cnt_, total_socket_cnt_, core_per_socket_;
    int total_thread_cnt_, thread_per_socket_, thread_per_core_;

    // info that read in
    std::vector<int> siblings_;  // thread count per socket; need to be the same. Will this double as the physical core cnt when HT disabled? No. It has been proved that when HT disabled, siblings will equals to the core per socket.
    std::vector<int> cpu_cores_;  // core count per socket; need to be the same
    std::vector<int> core_id_;  // core id may not be continual. a 10-core processor may have a core id of 12
    std::vector<int> physical_id_;
    std::vector<int> processor_;

    // thread level mapping info
    std::vector<int> socket_mapping_;
    std::vector<int> core_in_socket_mapping_;
    std::vector<int> core_in_global_mapping_;

    // notice that core_id_ is not the "real" core_id. eg: 0~5, 9~12
    // keys may not be continual, but values will be continual
    std::map<int, int> core_id_map_;
    // how much tid associated with a physical id
    std::map<int, int> physical_id_map_;

    // the core, to what tid. rely on core_in_global_mapping_
    std::vector<std::vector<int>> core_to_tid_map_;

    CPUInfoUtil(const CPUInfoUtil&);  // not to def
    CPUInfoUtil& operator=(const CPUInfoUtil&);  // not to def
    ~CPUInfoUtil() {}

 public:
    static CPUInfoUtil* GetInstance() {
        static CPUInfoUtil cpuinfo_single_instance;
        return &cpuinfo_single_instance;
    }

    // void GetCoreInfo();
    int GetTotalCoreCount() const {return total_core_cnt_;}
    int GetTotalSocketCount() const {return total_socket_cnt_;}
    int GetCorePerSocket() const {return core_per_socket_;}
    int GetTotalThreadCount() const {return total_thread_cnt_;}
    int GetThreadPerSocket() const {return thread_per_socket_;}
    int GetThreadPerCore() const {return thread_per_core_;}

    const std::vector<int>& GetSocketMappingVector() const {return socket_mapping_;}
    const std::vector<int>& GetCoreInSocketMappingVector() const {return core_in_socket_mapping_;}
    const std::vector<int>& GetCoreInGlobalMappingVector() const {return core_in_global_mapping_;}
    const std::vector<int>& GetSiblingVector() const {return siblings_;}
    const std::vector<int>& GetCPUCoresVector() const {return cpu_cores_;}
    const std::vector<int>& GetCoreIdVector() const {return core_id_;}
    const std::vector<int>& GetPhysicalIdVector() const {return physical_id_;}
    const std::vector<int>& GetProcessorVector() const {return processor_;}
    const std::vector<int>& GetCoreThreads(int core_id) const {return core_to_tid_map_[core_id];}

    int GetSocketMappingVector(int pos) const {return socket_mapping_[pos];}
    int GetCoreInSocketMappingVector(int pos) const {return core_in_socket_mapping_[pos];}
    int GetCoreInGlobalMappingVector(int pos) const {return core_in_global_mapping_[pos];}
    int GetSiblingVector(int pos) const {return siblings_[pos];}
    int GetCPUCoresVector(int pos) const {return cpu_cores_[pos];}
    int GetCoreIdVector(int pos) const {return core_id_[pos];}
    int GetPhysicalIdVector(int pos) const {return physical_id_[pos];}
    int GetProcessorVector(int pos) const {return processor_[pos];}

 private:
    CPUInfoUtil();
};
