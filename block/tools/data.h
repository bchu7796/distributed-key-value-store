#ifndef DATA_H
#define DATA_H

#include <inttypes.h>
#include <string>

class lamport_clock {
public:
    lamport_clock();
    lamport_clock(int32_t id, int32_t time);
    bool operator<(const lamport_clock &clock) const;
    void set_id(int32_t id);
    void set_time(int32_t time);
    void set(lamport_clock clock);
    int32_t get_id() const;
    int32_t get_time() const;
    bool operator==(const lamport_clock &clock) const;
    bool operator!=(const lamport_clock &clock) const;
    lamport_clock operator+(int32_t offset) const;
private:
    int32_t id;
    int32_t time;
};

class kv_pair {
public:
    kv_pair();
    kv_pair(std::string, std::string);
    void set_key(std::string);
    void set_value(std::string);
    std::string get_key() const;
    std::string get_value() const;
private:
    std::string key;
    std::string value;
};

struct value_time_pair {
public:
    value_time_pair();
    value_time_pair(std::string key, std::string value, int32_t id, int32_t time);
    value_time_pair(kv_pair kv, lamport_clock clock);
    bool operator==(const value_time_pair &vtp) const;
    bool operator!=(const value_time_pair &vtp) const;
    kv_pair get_kv() const;
    lamport_clock get_clock() const;
private:
    kv_pair kv;
    lamport_clock clock;
};

#endif