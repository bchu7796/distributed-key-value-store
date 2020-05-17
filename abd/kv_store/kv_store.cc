#include <inttypes.h>
#include <iostream>
#include <mutex>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include "kv_store.h"
#include "../tools/util.h"

kv_store::kv_store() {
    this->file_name = "kv_store";
    this->log_name = "log";
    read_hash_log();
}

kv_store::kv_store(std::string file_name) {
    this->file_name = file_name;
}

value_time_pair kv_store::kv_get(std::string key) {
    std::mutex kv_lock;
    kv_lock.lock();
    if(hash.count(key) == 0) {
        kv_lock.unlock();
        return value_time_pair("", "", -1, -1);
    }
    else {
        std::ifstream infile(this->file_name);
        if(infile) {
            char line[256];
            infile.seekg(hash[key]);
            infile.getline(line, 256);
            std::string s = line;
            std::vector<std::string> parsed;
            split(line, parsed);
            value_time_pair ret(key, parsed[0], std::stoi(parsed[1]), std::stoi(parsed[2]));
            kv_lock.unlock();
            return ret;
        }
        else {
            kv_lock.unlock();
            return value_time_pair("","", -1, -1);
        }
    }
}

int32_t kv_store::kv_set(value_time_pair vtp) {
    std::mutex kv_lock;
    kv_lock.lock();

    std::string key = vtp.get_kv().get_key();
    std::string value = vtp.get_kv().get_value();
    int32_t id = vtp.get_clock().get_id();
    int32_t time = vtp.get_clock().get_time();

    std::ofstream outfile(this->file_name, std::ofstream::out | std::ofstream::app);
    std::ofstream outlog(this->log_name, std::ofstream::out | std::ofstream::app);
    if(outfile && outlog) {
        lamport_clock fail_value(-1, -1);
        lamport_clock latest_time = get_latest_time(key);
        if(latest_time != fail_value && latest_time < vtp.get_clock()) {
            uint32_t offset = outfile.tellp();
            outfile << value << " ";
            outfile << id << " " << time << std::endl;
            hash[vtp.get_kv().get_key()] = offset;
            outlog << vtp.get_kv().get_key() << " " << offset << std::endl;
        }
        kv_lock.unlock();
        return 0;
    }
    else {
        kv_lock.unlock();
        return -1;
    }
}

int32_t kv_store::kv_delete(value_time_pair vtp) {
    std::mutex kv_lock;
    kv_lock.lock();
    std::string key = vtp.get_kv().get_key();
    lamport_clock fail_value(-1, -1);
    lamport_clock latest_time = get_latest_time(key);

    if(latest_time != fail_value && latest_time < vtp.get_clock()) {
        if(hash.count(key) == 1) {
            hash.erase(key);
        }
    }
    kv_lock.unlock();
    return 0;
}

int32_t kv_store::read_hash_log() {
    std::fstream config_file;
    config_file.open(this->log_name, std::ios::in);
    std::string line;
    std::vector<std::string> parsed_line;
    while(getline(config_file, line)) {
        split(line, parsed_line);
        hash[parsed_line[0]] = std::stoi(parsed_line[1]);
    }
    return 0;
}

lamport_clock kv_store::get_latest_time(std::string key) {
    if(hash.count(key) == 0) {
        return lamport_clock(-1, 0);
    }
    else {
        std::ifstream infile(this->file_name);
        if(infile) {
            char line[256];
            infile.seekg(hash[key]);
            infile.getline(line, 256);
            std::string s = line;
            std::vector<std::string> parsed;
            split(line, parsed);
            lamport_clock ret(std::stoi(parsed[1]), std::stoi(parsed[2]));
            return ret;
        }
        else {
            return lamport_clock(-1, -1);
        }
    }
}