/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "utils/timer.hpp"

int timer::N_Timers = 0;
std::vector<double> timer::_timers = std::vector<double>();
std::vector<double> timer::_acc_time = std::vector<double>();

void timer::init_timers(int count) {
    N_Timers = count;
    _timers.resize(count);
    _acc_time.resize(count);
}

void timer::reset_timers() {
    for (int i = 0; i < N_Timers; i ++) {
        _timers[i] = get_current_time();
        _acc_time[i] = 0;
    }
}

double timer::get_current_time() {
    timeval t;
    gettimeofday(&t, 0);
    return (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000;
}

// currently, only 4 timers are used, others can be defined by users

void timer::start_timer(int i) {
    _timers[i] = get_current_time();
}

void timer::reset_timer(int i) {
    _timers[i] = get_current_time();
    _acc_time[i] = 0;
}

void timer::stop_timer(int i) {
    double t = get_current_time();
    _acc_time[i] += t - _timers[i];
}

double timer::get_timer(int i) {
    return _acc_time[i];
}

void timer::print_timer(std::string str, int i) {
     printf("%s : %f miliseconds\n", str, get_timer(i));
}
