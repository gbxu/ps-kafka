//
// Created by gbxu on 18-4-24.
//
/*
scheduler：1
servergroup：2
workergroup：4
worker_id:9, 11, 13, …
server_id:8, 10, 12, …
 */
#ifndef PSKAFKA_DEBUG_H
#define PSKAFKA_DEBUG_H

#define DEBUGORNOT false
#include "ps/internal/van.h"

namespace ps {

class DebugOut {
public:
    DebugOut(Node mynode) {
        log_stream_<<mynode.DebugString().c_str();
    }
    std::ostringstream &stream() {
        return log_stream_;
    }
    void Out(){
        if(DEBUGORNOT){
            std::cout<<log_stream_.str()<<std::endl;
        }
    }
private:
    std::ostringstream log_stream_;
};

}
#endif //PSKAFKA_DEBUG_H
