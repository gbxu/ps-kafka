//
// Created by gbxu on 18-4-24.
//

#ifndef PSKAFKA_DEBUG_H
#define PSKAFKA_DEBUG_H

#define DEBUGORNOT true
#define DEBUG(mynode) Debugout(mynode, stream)
#include "ps/internal/van.h"

namespace ps {

class DebugOut {
public:
    DebugOut(Node mynode) {
        std::string tmp;
        if(mynode.id == Node::kEmpty){
            tmp = "empty";
        } else {
            tmp = std::to_string(mynode.id);
        }
        log_stream_<<mynode.DebugString().c_str() \
        <<"id is " << tmp <<" ";
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
