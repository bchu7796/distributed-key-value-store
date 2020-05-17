#ifndef KV_STORE_H
#define KV_STORE_H

#include "../tools/data.h"
#include <string>
#include <unordered_map>

class kv_store {
public:
    /**
     * Constructor
     *            
     **/ 
    kv_store();
    
    /**
     * Constructor
     * 
     * file_name         The name of file we want to store kvs to.
     *            
     **/ 
    kv_store(std::string file_name);

    /**
     * Read a value from kv store
     * 
     * key               The key we want to read value from.
     * 
     * 
     * return            value_time_pair(key, value, id, time), 
     *                   <"", "", -1, -1> if error or entry not exist.             
     **/ 
    value_time_pair kv_get(std::string key);

    /**
     * Write an entry into kv store
     * 
     * vtp                The key-value pair and timestamp we want to store
     *                    into kv store.
     * 
     * 
     * return            0 if success, -1 otherwise            
     **/ 
    int32_t kv_set(value_time_pair vtp);

    /**
     * Delete a entry in kv store
     * 
     * vtp                The key-value pair and timestamp we want to delete
     *                    from kv store.
     * 
     * 
     * return            0 if success, -1 otherwise             
     **/ 
    int32_t kv_delete(value_time_pair vtp);
private:
    int32_t read_hash_log();
    lamport_clock get_latest_time(std::string key);
    std::unordered_map<std::string, uint32_t> hash;
    std::string file_name;
    std::string log_name;
};

#endif