#include "abd_client.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <fstream>
#include <string.h>

#include <grpcpp/grpcpp.h>
#include "../abdserver.grpc.pb.h"

#define ATTEMPT 3

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using abdserver::ABDServer;
using abdserver::TimeRequest;
using abdserver::TimeReply;
using abdserver::ClientReadRequest;
using abdserver::ClientReadReply;
using abdserver::ClientWriteRequest;
using abdserver::ClientWriteReply;
using abdserver::ClientDeleteRequest;
using abdserver::ClientDeleteReply;

/**
 * Implementation of ABDServerClient class 
 * 
 */

bool ABDServerClient::operator<(const ABDServerClient &abdclient) const{
    return this->get_return_clock() < abdclient.get_return_clock();
}

value_time_pair ABDServerClient::read_rpc_(std::string key) {
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

int32_t ABDServerClient::write_rpc_(value_time_pair vt) {
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

int32_t ABDServerClient::delete_rpc_(value_time_pair vt) {
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

lamport_clock ABDServerClient::gettime_rpc_() {
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

value_time_pair ABDServerClient::get_return_value() const {
    return this->return_vtp;
}

lamport_clock ABDServerClient::get_return_clock() const {
    return this->clock;
}

void ABDServerClient::read_rpc(std::string key, uint32_t& success_counter) {
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

void ABDServerClient::write_rpc(value_time_pair vtp, uint32_t& success_counter) {
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

void ABDServerClient::gettime_rpc(uint32_t& success_counter) {
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

void ABDServerClient::delete_rpc(value_time_pair vtp, uint32_t& success_counter) {
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

client::client() {
    success_count = 0;
    read_config("client_config");
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
            this->rpc_servers.push_back(ABDServerClient(grpc::CreateChannel(parsed_line[1], grpc::InsecureChannelCredentials())));
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
    this->success_count = 0;
    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&ABDServerClient::read_rpc, &(this->rpc_servers[i]), key, std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }
    if(this->success_count > (this->server_num / 2)) {
        sort(this->rpc_servers.begin(), this->rpc_servers.end());
        kv_pair ret_val(this->rpc_servers.back().get_return_value().get_kv());
        lamport_clock ret_clock(this->rpc_servers.back().get_return_value().get_clock());

        if(ret_val.get_key() == "") return ret_val;
        if(write_DB(ret_val.get_key(), ret_val.get_value(), ret_clock) != -1) {
            this->time_stamp = ret_clock;
            return ret_val;
        }
        else {
            std::cout << "Read failed" << std::endl;
            return kv_pair("", "");
        }
    }
    else {
        std::cout << "Read failed" << std::endl;
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

    current_time.set_id(this->id);

    lamport_clock fail_time(-1, -1);
    if(current_time == fail_time) {
        std::cout << "Write failed" << std::endl;
        return -1;
    }

    kv_pair kv(key, val);
    value_time_pair vpt(kv, current_time);
    std::vector<std::thread> threads;
    this->success_count = 0;

    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&ABDServerClient::write_rpc, &(this->rpc_servers[i]), vpt, std::ref(this->success_count)));
    }
    for(int32_t i = 0; i < this->server_num; i++) {
        threads[i].join();
    }

    if(this->success_count > (this->server_num / 2)) {
        std::ofstream log("history", std::ofstream::out | std::ofstream::app);
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
    current_time.set_id(this->id);
    value_time_pair vpt(delete_kv, current_time);
    std::vector<std::thread> threads;
    this->success_count = 0;

    lamport_clock fail_time(-1, -1);
    if(current_time == fail_time) {
        std::cout << "Delete failed" << std::endl;
        return -1;
    }

    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&ABDServerClient::delete_rpc, &(this->rpc_servers[i]), vpt, std::ref(this->success_count)));
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
    for(int32_t i = 0; i < this->server_num; i++) {
        threads.push_back(std::thread(&ABDServerClient::gettime_rpc, &(this->rpc_servers[i]), std::ref(this->success_count)));
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