#ifndef MAJORITY_LOCK_H
#define MAJORITY_LOCK_H

#include <inttypes.h>
#include <list>
#include <mutex>
#include <map>
#include "../tools/data.h"

class majority_lock {
    public:
        majority_lock();
        int32_t acquire_lock(lamport_clock clock);
        int32_t check_lock(lamport_clock clock);
        int32_t giveup_lock(lamport_clock clock);
        int32_t release_lock(lamport_clock clock);
    private:
        std::mutex lock;
        bool voted;
        std::map<lamport_clock, int> requests;
        lamport_clock voted_request;
};

#endif