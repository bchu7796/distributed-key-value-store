#include "data.h"
#include <mutex>

/**
 * Definition of lamport_clock
 * 
 */ 
lamport_clock::lamport_clock() {
    this->id = 0;
    this->time = 0;
}

lamport_clock::lamport_clock(int32_t id, int32_t time) {
    this->id = id;
    this->time = time;
}

void lamport_clock::set(lamport_clock clock) {
    std::mutex time_lock;
    time_lock.lock();
    this->time = clock.time;
    time_lock.unlock();
}

bool lamport_clock::operator<(const lamport_clock &clock) const {
    if(this->time == clock.time) {
        return this->id < clock.id;
    }
    else {
        return this->time < clock.time;
    }
}

void lamport_clock::set_id(int32_t id) {
    this->id = id;
}
void lamport_clock::set_time(int32_t time) {
    std::mutex time_lock;
    time_lock.lock();
    this->time = time;
    time_lock.unlock();
}
int32_t lamport_clock::get_id() const {
    return this->id;
}
int32_t lamport_clock::get_time() const {
    return this->time;
}

bool lamport_clock::operator==(const lamport_clock &clock) const{
    if(this->id == clock.get_id() && this->time == clock.get_time()) {
        return true;
    }
    else {
        return false;
    }
}

bool lamport_clock::operator!=(const lamport_clock &clock) const{
    if(this->id == clock.get_id() && this->time == clock.get_time()) {
        return false;
    }
    else {
        return true;
    }
}

lamport_clock lamport_clock::operator+(int32_t offset) const {
    lamport_clock ret(this->id, this->time + 1);
    return ret;
}

/**
 * Definition of kv_pair
 * 
 */ 
kv_pair::kv_pair() {
    this->key = "";
    this->value = "";
}
kv_pair::kv_pair(std::string key, std::string value) {
    this->key = key;
    this->value = value;
}
void kv_pair::set_key(std::string key) {
    this->key = key;
}
void kv_pair::set_value(std::string value) {
    this->value = value;
}
std::string kv_pair::get_key() const {
    return this->key;
}
std::string kv_pair::get_value() const {
    return this->value;
}

/**
 * Definition of value_time_pair
 * 
 */ 

value_time_pair::value_time_pair() {
    this->kv.set_key("");
    this->kv.set_value("");
    this->clock.set_id(-1);
    this->clock.set_time(0);
}
value_time_pair::value_time_pair(std::string key, std::string value, int32_t id, int32_t time) {
    this->kv.set_key(key);
    this->kv.set_value(value);
    this->clock.set_id(id);
    this->clock.set_time(time);
}

value_time_pair::value_time_pair(kv_pair kv, lamport_clock clock) {
    this->kv = kv;
    this->clock = clock;
}

bool value_time_pair::operator==(const value_time_pair &vtp) const{
    if(this->kv.get_key() == vtp.kv.get_key() &&\
       this->kv.get_value() == vtp.kv.get_value() &&\ 
       this->clock.get_id() == vtp.clock.get_id() &&\
       this->clock.get_time() == vtp.clock.get_time()
    ) {
        return true;
    }
    else {
        return false;
    }
}

bool value_time_pair::operator!=(const value_time_pair &vtp) const{
    if(this->kv.get_key() == vtp.kv.get_key() &&\
       this->kv.get_value() == vtp.kv.get_value() &&\ 
       this->clock.get_id() == vtp.clock.get_id() &&\
       this->clock.get_time() == vtp.clock.get_time()
    ) {
        return false;
    }
    else {
        return true;
    }
}

kv_pair value_time_pair::get_kv() const {
    return this->kv;
}
lamport_clock value_time_pair::get_clock() const {
    return this->clock;
}
