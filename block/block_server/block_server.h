#ifndef BLOCK_SERVER_H
#define BLOCK_SERVER_H

#include <iostream>
#include <inttypes.h>
#include <string>
#include "../tools/data.h"
#include "../tools/util.h"
#include "../kv_store/kv_store.h"
#include "majority_lock.h"

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

class BlockServerServiceImplementation final : public BlockServer::Service {
public:
    void init(lamport_clock init_time);

    Status client_gettime(
        ServerContext* context, 
        const TimeRequest* request, 
        TimeReply* reply
    ) override;

    Status client_read(
        ServerContext* context, 
        const ClientReadRequest* request, 
        ClientReadReply* reply
    ) override;

    Status client_write(
        ServerContext* context, 
        const ClientWriteRequest* request, 
        ClientWriteReply* reply
    ) override;

    Status client_delete(
        ServerContext* context, 
        const ClientDeleteRequest* request, 
        ClientDeleteReply* reply
    ) override;

    Status client_acquire_lock(
        ServerContext* context, 
        const AcquireLockRequest* request, 
        AcquireLockReply* reply
    ) override;

    Status client_check_lock(
        ServerContext* context, 
        const CheckLockRequest* request, 
        CheckLockReply* reply
    ) override;

    Status client_giveup_lock(
        ServerContext* context, 
        const GiveupLockRequest* request, 
        GiveupLockReply* reply
    ) override;

    Status client_release_lock(
        ServerContext* context, 
        const ReleaseLockRequest* request, 
        ReleaseLockReply* reply
    ) override;
private:
    kv_store DB;
    lamport_clock server_time;
    majority_lock ml;
};

class DB_server {
public:
    void start_server();
private:
    /**
     * Read config file to get information of servers
     * and initialize rpc_clients
     * 
     * config_name     The file name of config_name
     * 
     * 
     * return          return value would be 0 if success
     *                 and -1 if fail                 
     **/ 
    int32_t read_config(std::string config_name);
    lamport_clock server_init_time;
    std::string server_address;
    BlockServerServiceImplementation service;
};

#endif