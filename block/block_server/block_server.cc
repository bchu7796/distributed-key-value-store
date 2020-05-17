#include "block_server.h"

#include <iostream>
#include <string>
#include <fstream>
#include "../tools/data.h"
#include "../tools/util.h"

#include <grpcpp/grpcpp.h>
#include "../blockserver.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
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

void BlockServerServiceImplementation::init(lamport_clock init_time) {
    this->server_time.set(init_time);
}

Status BlockServerServiceImplementation::client_gettime(
    ServerContext* context, 
    const TimeRequest* request, 
    TimeReply* reply
) {
    printf("Received Request: GetTime\n");

    // reply message
    reply->set_id(server_time.get_id());
    reply->set_time(server_time.get_time());
    std::cout << "server_time: " << server_time.get_time() << std::endl;
    printf("rpc call returned\n");
    
    return Status::OK;
} 

Status BlockServerServiceImplementation::client_read(
    ServerContext* context, 
    const ClientReadRequest* request, 
    ClientReadReply* reply
) {
    std::string key = request->key();
    std::cout << "Received Request: Read(" << key << ")" << std::endl;

    value_time_pair ret = DB.kv_get(key);

    // reply message
    reply->set_id(ret.get_clock().get_id());
    reply->set_time(ret.get_clock().get_time());
    reply->set_key(ret.get_kv().get_key());
    reply->set_value(ret.get_kv().get_value());

    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

Status BlockServerServiceImplementation::client_write(
    ServerContext* context, 
    const ClientWriteRequest* request, 
    ClientWriteReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    kv_pair kv(request->key(), request->value());
    value_time_pair vtp(kv, request_time);
    std::cout << "Received Request: Write(" << kv.get_key() << ", " << kv.get_value() << ", "\
              << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    DB.kv_set(vtp);
    server_time.set(request_time);
    
    // reply message
    reply->set_ack(1);

    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

Status BlockServerServiceImplementation::client_delete(
    ServerContext* context, 
    const ClientDeleteRequest* request, 
    ClientDeleteReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    kv_pair kv(request->key(), request->value());
    value_time_pair vtp(kv, request_time);
    std::cout << "Received Request: Delete(" << kv.get_key() << ", " << kv.get_value() << ", "\
              << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    DB.kv_delete(vtp);
    server_time.set(request_time);

    // reply message
    reply->set_ack(1);

    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

Status BlockServerServiceImplementation::client_acquire_lock(
    ServerContext* context, 
    const AcquireLockRequest* request, 
    AcquireLockReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    std::cout << "Received Request: client_acquire_lock(" << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    int32_t ret = this->ml.acquire_lock(request_time);
    reply->set_success(ret);

    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

Status BlockServerServiceImplementation::client_check_lock(
    ServerContext* context, 
    const CheckLockRequest* request, 
    CheckLockReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    std::cout << "Received Request: client_check_lock(" << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    int32_t ret = this->ml.check_lock(request_time);
    reply->set_success(ret);
    
    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
}

Status BlockServerServiceImplementation::client_giveup_lock(
    ServerContext* context, 
    const GiveupLockRequest* request, 
    GiveupLockReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    std::cout << "Received Request: client_giveup_lock(" << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    int32_t ret = this->ml.giveup_lock(request_time);
    reply->set_success(ret);
    
    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

Status BlockServerServiceImplementation::client_release_lock(
    ServerContext* context, 
    const ReleaseLockRequest* request, 
    ReleaseLockReply* reply
) {
    lamport_clock request_time(request->id(), request->time());
    std::cout << "Received Request: client_release_lock(" << request_time.get_id() << ", " << request_time.get_time() << ")" << std::endl;

    int32_t ret = this->ml.release_lock(request_time);
    reply->set_success(ret);
    
    std::cout << "rpc call returned" << std::endl;

    return Status::OK;
} 

void DB_server::start_server() {
    read_config("server_config");
    this->service.init(this->server_init_time);
    ServerBuilder builder;

    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&this->service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << this->server_address << std::endl;
    
    server->Wait();
}

int32_t DB_server::read_config(std::string config_name) {
    std::fstream config_file;
    config_file.open(config_name, std::ios::in);
    std::string line;
    std::vector<std::string> parsed_line;
    while(getline(config_file, line)) {
        split(line, parsed_line);
        if(parsed_line[0] =="ID") {
            this->server_init_time.set_id(std::stoi(parsed_line[1]));
        }
        else if(parsed_line[0] == "ADDRESS") {
            this->server_address = parsed_line[1];
        }
    }
    return 0;
}

int main(void) {
    DB_server server_node;
    server_node.start_server();
}
