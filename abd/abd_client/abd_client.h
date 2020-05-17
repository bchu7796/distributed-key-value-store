#ifndef ABD_CLIENT_H
#define ABD_CLIENT_H

#include <iostream>
#include <string>
#include <vector>
#include <inttypes.h>
#include "../tools/data.h"
#include "../tools/util.h"

#include <grpcpp/grpcpp.h>
#include "../abdserver.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

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

class ABDServerClient {
public:
    ABDServerClient(std::shared_ptr<Channel> channel) : stub_(ABDServer::NewStub(channel)) {}
    bool operator<(const ABDServerClient &abdclient) const;
    value_time_pair get_return_value() const;
    lamport_clock get_return_clock() const;
    
    // APIs for clients
    void gettime_rpc(uint32_t& success_counter);
    void read_rpc(std::string key, uint32_t& success_counter);
    void write_rpc(value_time_pair vtp, uint32_t& success_counter);
    void delete_rpc(value_time_pair vtp, uint32_t& success_counter);
private:
    /**
     * do gettime rpc.
     * 
     * 
     * return              lamport clock <id, time>,
     *                     the value is <-1,-1> if fail.         
     **/ 
    lamport_clock gettime_rpc_();

    /**
     * Set rpc request with argument "key" and do read rpc.
     * 
     * key               The key we want to read value from.
     * 
     * 
     * return            return a pair of <kv, time>, 
     *                   the value is <"", "", -1, -1> if fail              
     **/ 
    value_time_pair read_rpc_(std::string key);

    /**
     * Set rpc request with argument value_time_pair and do write rpc.
     * 
     * value_time_pair    a pair of <kv, time>
     * 
     * 
     * return             return 0 if success, -1 otherwise            
     **/ 
    int32_t write_rpc_(value_time_pair);

    /**
     * Set rpc request with argument value_time_pair and do write rpc.
     * 
     * value_time_pair    a pair of <kv, time>
     * 
     * 
     * return             return 0 if success, -1 otherwise            
     **/ 
    int32_t delete_rpc_(value_time_pair);
    
    value_time_pair return_vtp;
    lamport_clock clock;
    std::unique_ptr<ABDServer::Stub> stub_;
};

class client {
public:
    client();

    uint32_t get_id();
    lamport_clock get_log_time();

    /**
     * Read from key-value store servers. Block until getting
     * response from majority of the servers.
     * 
     * key             The key used to find value in the database.
     * 
     * 
     * return          return a key-value pair              
     **/ 
    kv_pair read_DB(std::string key);

    /**
     * Write to key-value store servers. Block until getting
     * response from majority of the servers.
     * 
     * kv              The kv_pair we want to write to the database
     * 
     * 
     * return          return 0 if success, -1 otherwise           
     **/ 
    int32_t write_DB(std::string key, std::string val, lamport_clock current_time);

    /**
     * Delete from key-value store servers. Block until getting
     * response from majority of the servers.
     * 
     * kv              The kv_pair we want to write to the database
     * 
     * 
     * return          return 0 if success, -1 otherwise           
     **/ 
    int32_t delete_DB(std::string key);
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

    /**
     * Get global time from servers. Block until getting
     * response from majority of the servers.
     * 
     * return          return the highest time tag from servers.          
     **/ 
    lamport_clock get_time();
    
    uint32_t server_num;
    std::vector<ABDServerClient> rpc_servers;
    uint32_t success_count;
    uint32_t id;
    lamport_clock time_stamp;
};

#endif