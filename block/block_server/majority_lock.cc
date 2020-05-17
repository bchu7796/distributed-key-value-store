#include "majority_lock.h"
#include <inttypes.h>
#include <iostream>

majority_lock::majority_lock() {
    this->voted = false;
}

int32_t majority_lock::acquire_lock(lamport_clock clock) {
    this->lock.lock();

    requests[clock] = 1;

    if(voted == false && this->requests.begin()->first == clock) {
        this->voted_request = clock;
        this->voted = true;
        this->lock.unlock();
        return 1;
    }
    else {
        this->lock.unlock();
        return 0;
    }
}

int32_t majority_lock::check_lock(lamport_clock clock) {
    this->lock.lock();
    if(this->requests.empty()) {
        std::cout << "check lock while request queue empty" << std::endl;
        this->lock.unlock();
        return -1;
    }
    if(voted == false && this->requests.begin()->first == clock) {
        this->voted_request = clock;
        this->voted = true;
        this->lock.unlock();
        return 1;
    }
    this->lock.unlock();
    return 0;
}

int32_t majority_lock::giveup_lock(lamport_clock clock) {
    this->lock.lock();
    if(this->requests.empty()) {
        std::cout << "give up lock while request queue empty" << std::endl;
        this->lock.unlock();
        return -1;
    }
    if(this->voted == true && this->voted_request == clock) {
        this->voted = false;
    }
    this->requests.erase(clock);
    this->lock.unlock();
    return 1;
}

int32_t majority_lock::release_lock(lamport_clock clock) {
    this->lock.lock();
    if(this->requests.empty()) {
        std::cout << "release lock while request queue empty" << std::endl;
        this->lock.unlock();
        return -1;
    }
    if(this->voted == true && this->voted_request == clock) {
        this->voted = false;
        this->requests.erase(clock);
        this->lock.unlock();
        return 1;
    }
    else {
        this->requests.erase(clock);
        this->lock.unlock();
        return -1;
    }
}