#ifndef ABD_SERVER_H
#define ABD_SERVER_H

#include <iostream>
#include <inttypes.h>
#include <string>
#include "../tools/data.h"
#include "../tools/util.h"
#include "../kv_store/kv_store.h"

#include <grpcpp/grpcpp.h>
#include "../abdserver.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
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

class ABDServerServiceImplementation final : public ABDServer::Service {
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
private:
    kv_store DB;
    lamport_clock server_time;
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
    ABDServerServiceImplementation service;
};

#endif