#include "block_client.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <fstream>
#include <string.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include "../blockserver.grpc.pb.h"

#define ATTEMPT 3

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using blockserver::BlockServer;
using blockserver::TimeRequest;
using blockserver::TimeReply;
using blockserver::ClientReadRequest;
using blockserver::ClientReadReply;
using blockserver::ClientWriteRequest;
using blockserver::ClientWriteReply;
using blockserver::ClientDeleteRequest;
using blockserver::ClientDeleteReply;
using blockserver::AcquireLockRequest;
using blockserver::AcquireLockReply;
using blockserver::CheckLockRequest;
using blockserver::CheckLockReply;
using blockserver::GiveupLockRequest;
using blockserver::GiveupLockReply;
using blockserver::ReleaseLockRequest;
using blockserver::ReleaseLockReply;

/**
 * Implementation of BlockServerClient class 
 * 
 */

bool BlockServerClient::operator<(const BlockServerClient &blockclient) const{
    return this->get_return_clock() < blockclient.get_return_clock();
}

value_time_pair BlockServerClient::read_rpc_(std::string key) {
    ClientReadRequest request;
    ClientReadReply reply;
    ClientContext context;

    request.set_key(key);

    Status status = stub_->client_read(&context, request, &reply);

    if(status.ok()){
        value_time_pair ret(reply.key(), reply.value(), reply.id(), reply.time());
        return ret;
    } 
    else {
        value_time_pair ret("", "", -1, -1);
        return ret;
    }
}

int32_t BlockServerClient::write_rpc_(value_time_pair vt) {
    ClientWriteRequest request;
    ClientWriteReply reply;
    ClientContext context;

    request.set_id(vt.get_clock().get_id());
    request.set_time(vt.get_clock().get_time());
    request.set_key(vt.get_kv().get_key());
    request.set_value(vt.get_kv().get_value()); 

    Status status = stub_->client_write(&context, request, &reply);

    if(status.ok()){
        return reply.ack();
    } 
    else {
        return -1;
    }
}

int32_t BlockServerClient::delete_rpc_(value_time_pair vt) {
    ClientDeleteRequest request;
    ClientDeleteReply reply;
    ClientContext context;

    request.set_id(vt.get_clock().get_id());
    request.set_time(vt.get_clock().get_time());
    request.set_key(vt.get_kv().get_key());
    request.set_value(vt.get_kv().get_value()); 

    Status status = stub_->client_delete(&context, request, &reply);

    if(status.ok()){
        return reply.ack();
    } 
    else {
        return -1;
    }
}

lamport_clock BlockServerClient::gettime_rpc_() {
    TimeRequest request;
    TimeReply reply;
    ClientContext context;

    Status status = stub_->client_gettime(&context, request, &reply);

    if(status.ok()){
        lamport_clock ret(reply.id(), reply.time());
        return ret;
    } 
    else {
        lamport_clock ret(-1, -1);
        return ret;
    }
}

value_time_pair BlockServerClient::get_return_value() const {
    return this->return_vtp;
}

lamport_clock BlockServerClient::get_return_clock() const {
    return this->clock;
}

bool BlockServerClient::check_lock_status() const {
    return this->lock_status;
}

void BlockServerClient::read_rpc(std::string key, uint32_t& success_counter) {
    std::mutex count_lock;
    const value_time_pair fail_value("","",-1,-1);
    return_vtp = fail_value;
    for(int32_t i = 0; i < ATTEMPT; i++) {
        value_time_pair ret = read_rpc_(key);
        if(ret != fail_value) {
            return_vtp = ret;
            count_lock.lock();
            success_counter++;
            count_lock.unlock();
            break;
        }
    }
}

void BlockServerClient::write_rpc(value_time_pair vtp, uint32_t& success_counter) {
    std::mutex count_lock;
    for(int32_t i = 0; i < ATTEMPT; i++) {
        int32_t ret = write_rpc_(vtp);
        if(ret != -1) {
            count_lock.lock();
            success_counter++;
            count_lock.unlock();
            break;
        }
    }
}

void BlockServerClient::gettime_rpc(uint32_t& success_counter) {
    std::mutex count_lock;
    const lamport_clock fail_value(-1,-1);
    return_vtp = value_time_pair("", "", -1, -1);
    for(int32_t i = 0; i < ATTEMPT; i++) {
        lamport_clock ret = gettime_rpc_();
        if(ret != fail_value) {
            return_vtp = value_time_pair("", "", ret.get_id(), ret.get_time());
            count_lock.lock();
            success_counter++;
            count_lock.unlock();
            break;
        }
    }
}

void BlockServerClient::delete_rpc(value_time_pair vtp, uint32_t& success_counter) {
    std::mutex count_lock;
    for(int32_t i = 0; i < ATTEMPT; i++) {
        int32_t ret = delete_rpc_(vtp);
        if(ret != -1) {
            count_lock.lock();
            success_counter++;
            count_lock.unlock();
            break;
        }
    }
}

void BlockServerClient::acquire_lock(lamport_clock clock) {
    AcquireLockRequest request;
    AcquireLockReply reply;
    ClientContext context;

    request.set_id(clock.get_id());
    request.set_time(clock.get_time());

    Status status = stub_->client_acquire_lock(&context, request, &reply);

    if(status.ok()){
        if(reply.success() == 1) {
            this->lock_status = true;
        }
    } 
}

void BlockServerClient::check_lock(lamport_clock clock) {
    CheckLockRequest request;
    CheckLockReply reply;
    ClientContext context;

    request.set_id(clock.get_id());
    request.set_time(clock.get_time());

    Status status = stub_->client_check_lock(&context, request, &reply);

    if(status.ok()){
        if(reply.success() == 1) {
            this->lock_status = true;
        }
    } 
}

void BlockServerClient::giveup_lock(lamport_clock clock) {
    GiveupLockRequest request;
    GiveupLockReply reply;
    ClientContext context;

    request.set_id(clock.get_id());
    request.set_time(clock.get_time());

    Status status = stub_->client_giveup_lock(&context, request, &reply);

    if(status.ok()){
        if(reply.success() == 1) {
            this->lock_status = false;
        }
    } 
}

void BlockServerClient::release_lock(lamport_clock clock) {
    ReleaseLockRequest request;
    ReleaseLockReply reply;
    ClientContext context;

    request.set_id(clock.get_id());
    request.set_time(clock.get_time());

    Status status = stub_->client_release_lock(&context, request, &reply);

    if(status.ok()){
        if(reply.success() == 1) {
            this->lock_status = false;
        }
    } 
}

client::client() {
    this->success_count = 0;
    read_config("client_config");
}

void client::init_status() {
    this->success_count = 0;
}

uint32_t client::get_id() {
    return this->id;
}

lamport_clock client::get_log_time() {
    return this->time_stamp;
}

int32_t client::read_config(std::string config_name) {
    std::fstream config_file;
    config_file.open(config_name, std::ios::in);
    std::string line;
    std::vector<std::string> parsed_line;
    while(getline(config_file, line)) {
        split(line, parsed_line);
        if(parsed_line[0] == "SERVER") {
            this->rpc_servers.push_back(BlockServerClient(grpc::CreateChannel(parsed_line[1], grpc::InsecureChannelCredentials())));
        }
        else if(parsed_line[0] == "ID") {
            this->id = std::stoi(parsed_line[1]);
        }
    }
    this->server_num = rpc_servers.size();
    return 0;
}

kv_pair client::read_DB(std::string key) {
    std::vector<std::thread> threads;
    lamport_clock current_time = get_time();
    lamport_clock fail_time(-1, -1);
    if(current_time == fail_time) {
        std::cout << "Get time failed" << std::endl;
        return kv_pair("","");
    }
    current_time.set_id(this->id);

    if(!acquire_lock(current_time)) {
        std::cout << "Cannot acquire locks" << std::endl;
        return kv_pair("","");
    }

    // after receive majority votes
    init_status();
    for(int32_t i = 0; i < this->locked_servers.size(); i++) {
        threads.push_back(std::thread(&BlockServerClient::read_rpc, &(this->rpc_servers[locked_servers[i]]), key, std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->locked_servers.size(); i++) {
        threads[i].join();
    }

    if(this->success_count >= 1) {
        kv_pair ret_val("","");
        lamport_clock ret_clock(-1,-1);
        for(int i = 0; i < locked_servers.size(); i++) {
            if(ret_clock < rpc_servers[locked_servers[i]].get_return_value().get_clock()) {
                ret_clock = rpc_servers[locked_servers[i]].get_return_value().get_clock();
                ret_val = rpc_servers[locked_servers[i]].get_return_value().get_kv();
            }
        }

        if(ret_val.get_key() == "") {
            release_lock(current_time);
            std::cout << "Server returns empty value" << std::endl;
            return ret_val;
        }
        if(write_DB(ret_val.get_key(), ret_val.get_value(), ret_clock) != -1) {
            release_lock(current_time);
            this->time_stamp = ret_clock;
            return ret_val;
        }
        else {
            release_lock(current_time);
            std::cout << "Write after read failed" << std::endl;
            return kv_pair("", "");
        }
    }
    else {
        release_lock(current_time);
        std::cout << "Not all locked servers response" << std::endl;
        return kv_pair("", "");
    }
}

int32_t client::write_DB(std::string key, std::string val, lamport_clock current_time) {
    if(key == "") {
        std::cout << "key cannot be empty" << std::endl;
        return -1;
    }

    if(current_time.get_id() == -1) {
        current_time = get_time();
        this->time_stamp = current_time;
    }

    lamport_clock fail_time(-1, -1);
    if(current_time == fail_time) {
        std::cout << "Write failed" << std::endl;
        return -1;
    }

    current_time.set_id(this->id);

    kv_pair kv(key, val);
    value_time_pair vpt(kv, current_time);
    std::vector<std::thread> threads;
    init_status();

    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&BlockServerClient::write_rpc, &(this->rpc_servers[i]), vpt, std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }

    if(this->success_count > (this->server_num / 2)) {
        return 0;
    }
    else {
        std::cout << "Write failed" << std::endl;
        return -1;
    }
}

int32_t client::delete_DB(std::string key) {
    kv_pair delete_kv(key, "");
    lamport_clock current_time = get_time();
    value_time_pair vpt(delete_kv, current_time);
    std::vector<std::thread> threads;
    init_status();

    lamport_clock fail_time(-1, -1);
    if(current_time == fail_time) {
        std::cout << "Delete failed" << std::endl;
        return -1;
    }

    current_time.set_id(this->id);

    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&BlockServerClient::delete_rpc, &(this->rpc_servers[i]), vpt, std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }
    if(this->success_count > (this->server_num / 2)) {
        return 0;
    }
    else {
        std::cout << "Delete failed" << std::endl;
        return -1;
    }
}

lamport_clock client::get_time() {
    std::vector<std::thread> threads;
    init_status();
    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&BlockServerClient::gettime_rpc, &(this->rpc_servers[i]), std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }
    if(this->success_count > (this->server_num / 2)) {
        sort(this->rpc_servers.begin(), this->rpc_servers.end());

        lamport_clock ret((this->rpc_servers.back().get_return_value().get_clock()) + 1);
        return ret;
    }
    else {
        std::cout << "Get time failed" << std::endl;
        return lamport_clock(-1, -1);
    }
}

bool client::acquire_lock(lamport_clock current_time) {
    // acquire lock
    std::vector<std::thread> threads;
    
    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&BlockServerClient::acquire_lock, &(this->rpc_servers[i]), current_time));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }
    // check status
    for(int32_t i = 0; i < this->server_num; i++) {
        if(this->rpc_servers[i].check_lock_status()) {
            this->locked_servers.push_back(i);
        }
        else {
            this->unlock_servers.push_back(i);
        }
    }

    //if not get majority
    if(locked_servers.size() <= (this->server_num / 2)) {
        for(int i = 0; i < ATTEMPT; i++) {
            sleep(2);
            threads.clear();
            for(int32_t i = 0; i < this->unlock_servers.size(); i++) {
                threads.push_back(std::thread(&BlockServerClient::check_lock, &(this->rpc_servers[unlock_servers[i]]), current_time));
            }
            for(int32_t i = 0; i < this->unlock_servers.size(); i++) {
                threads[i].join();
            }
            // check status
            std::vector<int>::iterator it = unlock_servers.begin();
            while(it != unlock_servers.end()) {
                if(this->rpc_servers[*it].check_lock_status()) {
                    this->locked_servers.push_back(*it);
                    it = this->unlock_servers.erase(it);
                }
                else {
                    it++;
                }
            }
            if(locked_servers.size() > (this->server_num / 2)) {
                break;
            }
        }
    }

    if(locked_servers.size() > (this->server_num / 2)) {
        return true;
    }
    else {
        // giveup
        threads.clear();
        for(int32_t i = 0; i < this->server_num; i++) {
            threads.push_back(std::thread(&BlockServerClient::giveup_lock, &(this->rpc_servers[i]), current_time));
        }
        for(int32_t i = 0; i < this->server_num; i++) {
            threads[i].join();
        }
        this->locked_servers.clear();
        this->unlock_servers.clear();
        return false;
    }
}

bool client::release_lock(lamport_clock current_time) {
    std::vector<std::thread> threads;
    threads.clear();
    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&BlockServerClient::release_lock, &(this->rpc_servers[locked_servers[i]]), current_time));
    }
    for(int i = 0; i < this->server_num; i++) {
        threads[i].join();
    }

    this->locked_servers.clear();
    this->unlock_servers.clear();
    return true;
}

int main(int32_t argc, char **argv) {
    if(argc == 1) {
        std::cout << "Usage: client [command] [arg1] [arg2] ..." << std::endl;
    }
    else if(argc > 1) {
        client client_obj;
        if(strncmp(argv[1], "read", sizeof("read")) == 0 && argc == 3) {
            kv_pair read_result = client_obj.read_DB(argv[2]);
            if(read_result.get_key() == "") {
                std::cout << "read failed" << std::endl;
            }
            else {
                std::cout << "read value: " << read_result.get_value() << std::endl;
            }
        }
        else if(strncmp(argv[1], "write", sizeof("write")) == 0 && argc == 4) {
            int32_t write_result = client_obj.write_DB(argv[2], argv[3], lamport_clock(-1, -1));
            if(write_result >= 0) {
                std::cout << "write success" << std::endl;
            }
            else {
                std::cout << "write failed" << std::endl;
            }
        }
        else if(strncmp(argv[1], "delete", sizeof("delete")) == 0 && argc == 3) {
            int32_t delete_result = client_obj.delete_DB(argv[2]);
            if(delete_result >= 0) {
                std::cout << "delete success" << std::endl;
            }
            else {
                std::cout << "delete failed" << std::endl;
            }
        }
        else {
            std::cout << "Usage: client [command] [arg1] [arg2] ..." << std::endl;
        }
    }
    return 0;
}