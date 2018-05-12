//
// DONE:Created by gbxu on 18-4-20.
//

#ifndef PSLITE_KAFKA_VAN_H_
#define PSLITE_KAFKA_VAN_H_
#include <stdlib.h>
#include <thread>
#include <string>
#include <stdio.h>
#include <signal.h>
#include "ps/internal/van.h"
#include <librdkafka/rdkafka.h>
#include "ps/debug.h"
#include <set>
#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {

const char* TopicToConst(Topic topic){
    switch (topic){
        case TOSCHEDULER:
            return "TOSCHEDULER";//to scheduler
        case TOSERVERS:
            return  "TOSERVERS";// to servers
        case TOWORKERS:
            return "TOWORKERS";//to workers
        default:
            return "NONE";
    }
}
struct RD {
    rd_kafka_t *rk;//producer/consumer object
    rd_kafka_topic_t *rkt;//topic object
};
/**
 * \brief KAFKA based implementation
 */
class KAFKAVan : public Van {
public:
    KAFKAVan() { }
    virtual ~KAFKAVan() { }

protected:
    void Start(int customer_id) override {
        Van::Start(customer_id);
        if(my_node_.role == Node::WORKER){
            partitions_cnt = Postoffice::Get()->num_workers()+1;//consumer partitions
        } else if(my_node_.role == Node::SERVER){//lose one =... waste my time..
            partitions_cnt = Postoffice::Get()->num_servers()+1;
        } else{
            partitions_cnt = 1;
        }
    }

    void Stop() override {
        PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
        Van::Stop();

        for (auto& itr : producers_) {
            RD rd = itr.second;
            rd_kafka_t *rk = rd.rk;
            rd_kafka_topic_t *rkt = rd.rkt;
            /* Poll to handle delivery reports */
            rd_kafka_poll(rk, 0);
            /* Wait for messages to be delivered */
            while (rd_kafka_outq_len(rk) > 0)
                rd_kafka_poll(rk, 100);
            /* Destroy topic object */
            rd_kafka_topic_destroy(rkt);
            /* Destroy the instance */
            rd_kafka_destroy(rk);
        }
        for (auto& itr : consumers_) {
            RD rd = itr.second;
            rd_kafka_t *rk = rd.rk;
            rd_kafka_topic_t *rkt = rd.rkt;
            /* Stop consuming */
            for (int i = 0; i < partitions_cnt; ++i) {
                rd_kafka_consume_stop(rkt, i);
            }
            while (rd_kafka_outq_len(rk) > 0) rd_kafka_poll(rk, 10);//TODO:block when server!! cant finish!!
            /* Destroy topic */
            rd_kafka_topic_destroy(rkt);
            /* Destroy handle */
            rd_kafka_destroy(rk);

        }
#ifdef DODEBUG
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<"finish";
        debug.Out();
#endif
    }

    void Bind(const char *brokers, Topic topic) override {
        currConsumerTopic = topic;
        currConsumerPartition = 0;
        //consumer
        CHECK_NE(currConsumerTopic, NONE);//empty
#ifdef DODEBUG
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<" Bind "<<TopicToConst(currConsumerTopic);
        debug.Out();
#endif
        auto it = consumers_.find(TopicToConst(currConsumerTopic));// null
        if (it != consumers_.end()) {// exists, close the consumer
            RD rd = it->second;
            rd_kafka_t *rk = rd.rk;
            rd_kafka_topic_t *rkt = rd.rkt;
            /* Destroy topic object */
            rd_kafka_topic_destroy(rkt);
            /* Destroy the consumer instance */
            rd_kafka_destroy(rk);
        }
        //conf
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "message.max.bytes", "104857600",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
        if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", "104857600",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
        if (rd_kafka_conf_set(conf, "socket.keepalive.enable", "true",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
        //rd_kafka_conf_set(conf, "group.id", "0", errstr, sizeof(errstr));// dont need the group.id now
        //consumer
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
            CHECK(0)<<" Failed to create new consumer:"<<errstr;
        }
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
            CHECK(0)<<"No valid brokers specified";
        }
        //topic
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, TopicToConst(currConsumerTopic), NULL);
        if (!rkt) {
            rd_kafka_destroy(rk);
            CHECK(0)<<" Failed to create topic object:"<<rd_kafka_err2str(rd_kafka_last_error());
        }
        /* start the consume*/
        RD rd = {rk, rkt};
        consumers_[TopicToConst(currConsumerTopic)] = rd;

        StartConsumer();

    }

    void Connect(const char *brokers, Topic topic) override {
        //producer
        // by gbxu:
        //brokers ip:port,ip:port,ip:port
        //brokers ip,ip:port,ip:port //default port is 9092
        CHECK_NE(topic, NONE);//empty
#ifdef DODEBUG
        DebugOut tmp = DebugOut(my_node_);
        tmp.stream()<<" Connect: "<<TopicToConst(topic);
        tmp.Out();
#endif
        auto it = producers_.find(TopicToConst(topic));// null
        if (it != producers_.end()) {// exists, close the producer
            RD rd = it->second;
            rd_kafka_t *rk = rd.rk;
            rd_kafka_topic_t *rkt = rd.rkt;
            /* Destroy topic object */
            rd_kafka_topic_destroy(rkt);
            /* Destroy the producer instance */
            rd_kafka_destroy(rk);
        }

        //conf
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "message.max.bytes", "104857600",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }//8388608 8M 104857600 100M
        if (rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", "1048576",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
        if (rd_kafka_conf_set(conf, "socket.keepalive.enable", "true",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
//        if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", "0",
//                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
//            CHECK(0)<<"rd_kafka_conf_set error";
//        }
//        if (rd_kafka_conf_set(conf, "batch.num.messages", "10",
//                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
//            CHECK(0)<<"rd_kafka_conf_set error";
//        }
        if (rd_kafka_conf_set(conf, "request.required.acks", "0",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK){
            CHECK(0)<<"rd_kafka_conf_set error";
        }
        //producer
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
            CHECK(0)<<" Failed to create new producer "<<errstr;
        }
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
            CHECK(0)<<" No valid brokers specified";
        }
        //topic
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, TopicToConst(topic), NULL);
        if (!rkt) {
            rd_kafka_destroy(rk);
            CHECK(0)<<" Failed to create topic object: "<<rd_kafka_err2str(rd_kafka_last_error());
        }
        RD rd = {rk, rkt};
        producers_[TopicToConst(topic)] = rd;
    }


    int SendMsg(Message& msg) override {
        std::lock_guard<std::mutex> lk(mu_);

        //topic partition
        msg.meta.sender = my_node_.id;
        Topic topic = Postoffice::IDtoTopic(msg.meta.recver);
        int partition = Postoffice::IDtoPartition(msg.meta.recver);
        // find the producer
        auto it = producers_.find(TopicToConst(topic));
        if (it == producers_.end()) {
            CHECK(0)<<" scheduler:"<<Postoffice::Get()->is_scheduler()<<" " \
                    <<" cant find:"<<TopicToConst(topic)<<partition;
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;
        void *data_buff = malloc(1 << 26);//8 MB
        int send_bytes = PackMsg(data_buff, msg)+req_data_size(msg);
        retry:
        if(rd_kafka_produce(rkt,
                            partition,
                            RD_KAFKA_MSG_F_COPY,
                            (char *)data_buff, send_bytes,
                            "f", 1,
                            NULL) == -1){
            if (rd_kafka_last_error() ==
                RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                rd_kafka_poll(rk, 1000);
                goto retry;
            } else{
                CHECK(0)<<" producer: "<<rd_kafka_err2str(rd_kafka_last_error());
            }
        }
        rd_kafka_poll(rk, 0/*non-blocking*/);

#ifdef DODEBUG
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<" "<<"sendmsg to:"<<Postoffice::IDtoRoleIDConst(msg.meta.recver) \
                        <<" :"<<msg.meta.control.DebugString() \
                        <<" size :"<<send_bytes;
        debug.Out();
#endif
        free(data_buff);
        return send_bytes;
    }
    int req_data_size(const Message& msg) {
        int size = 0;
        for (int i = 0; i < msg.data.size(); i++) {
            size += sizeof(int) + msg.data[i].size();
        }
        return size;
    }
    int StartConsumer(){
        if (my_node_.id != Node::kEmpty){
            currConsumerTopic = Postoffice::IDtoTopic(my_node_.id);
            currConsumerPartition = Postoffice::IDtoPartition(my_node_.id);//rank+1 or 0
        }
        /* find RD */
        //check
        auto it = consumers_.find(TopicToConst(currConsumerTopic));
        if (it == consumers_.end()) {
            CHECK(0);
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;
        /* find topic partition */
        //check
        auto it_tp = consume_toppar_.find(TopicToConst(currConsumerTopic));
        if(it_tp == consume_toppar_.end() ||
           it_tp->second.find(currConsumerPartition) == it_tp->second.end()){
            //consume start
            if (rd_kafka_consume_start(rkt,
                                       currConsumerPartition,
                                       RD_KAFKA_OFFSET_END) == -1){
                CHECK(0);
            }
            consume_toppar_[TopicToConst(currConsumerTopic)].insert(currConsumerPartition);
        }
        return 0;
    }
    int RecvMsg(Message* msg) override {
        msg->data.clear();
        size_t recv_bytes = 0;
        /* RD */
        auto it = consumers_.find(TopicToConst(currConsumerTopic));
        if (it == consumers_.end()) {
            CHECK(0);
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;
        rd_kafka_message_t *rkmessage;
        rd_kafka_poll(rk, 0);
        while(1){
            rkmessage = rd_kafka_consume(rkt, currConsumerPartition, 1000);//block:not time out 1000ms
            if(!rkmessage){
                continue;
            } else if(rkmessage->err){
                if(rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF){
                    continue;
                }else{
                    CHECK(0)<<" consumer: "<<rd_kafka_err2str(rkmessage->err);
                }
            } else {
                break;
            }
        }
        CHECK_EQ(rkmessage->err,0);
        //void* buf = CHECK_NOTNULL(rkmessage->payload);
        msg->meta.recver = my_node_.id;
        void *buf = CHECK_NOTNULL(rkmessage->payload);
        UnpackMsg(buf, msg);
        size_t size = rkmessage->len;
        recv_bytes += size;
#ifdef DODEBUG
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<" "<<"recvmsg from "
                      <<Postoffice::IDtoRoleIDConst(msg->meta.sender) \
                            <<" :"<<msg->meta.control.DebugString() \
                            <<" size:"<<size;
        debug.Out();
#endif
        rd_kafka_message_destroy(rkmessage);
        return recv_bytes;
    }
    /* pack message into send buf by xiaoniu */
    int PackMsg(void* buf, const Message& msg) {
        /* buf construct: meta_size, data_number, meta, data[1]_size, data[1], data[2]_size, data[2], ...*/
        int n = msg.data.size();
        int meta_size; char* meta_buf = (char*)buf + 2 * sizeof(int);
        PackMeta(msg.meta, &meta_buf, &meta_size);
#ifdef XNDEBUG
        fprintf(stdout, "meta packed\n");
#endif
        *(int*)buf = meta_size;
        *((int*)(buf + sizeof(int))) = n;
        void* buf_tmp = buf + 2 * sizeof(int) + meta_size;
        /* total_bytes actually does not include data: only meta_size and two int */
        int total_bytes = meta_size + 2 * sizeof(int);
        for (int i = 0; i < n; i++) {
            int data_size = msg.data[i].size();
#ifdef XNDEBUG
            fprintf(stdout, "\t\tdata size required, is %d\n", data_size);
#endif
            memcpy(buf_tmp + sizeof(int), msg.data[i].data(), data_size);
#ifdef XNDEBUG
            fprintf(stdout, "\t\tdata %d copied\n", i);
#endif
            *(int*)buf_tmp = data_size;
            buf_tmp += data_size + sizeof(int);
        }
#ifdef XNDEBUG
        fprintf(stdout, "data in total %d packed\n", n);
#endif
        return total_bytes;
    }

    /* Unpack msg from recv buffer, return recv bytes by xiaoniu */
    int UnpackMsg(void *buf, Message* msg) {
        /* unpack Meta message */
        void* buf_tmp = buf;
        int meta_size = *(int*)buf_tmp;
        buf_tmp += sizeof(int);
        int data_num = *(int*)buf_tmp;
        buf_tmp += sizeof(int);
        UnpackMeta((char*)buf_tmp, meta_size, &(msg->meta));
        /* set msg meta recver */
        msg->meta.recver = my_node_.id;
        buf_tmp += meta_size;
        int total_bytes = meta_size + sizeof(int) * 2;
        /* reset msg data */
        for (int i = 0; i < data_num; i++) {
            int data_size = *(int*)buf_tmp;
            total_bytes += data_size + sizeof(int);
#ifdef XNDEBUG
            fprintf(stdout, "\t\t\tdata_size get %d\n", data_size);
#endif
            SArray<char> data;
            data.CopyFrom((char*)(buf_tmp + sizeof(int)), data_size);

#ifdef XNDEBUG
            fprintf(stdout, "\t\t\tdata %d cpyed\n", i);
#endif
            msg->data.push_back(data);
            buf_tmp += sizeof(int) + data_size;
        }
        return total_bytes;
    }
private:
    /**
     * \brief node_id to the socket for sending data to this node
     */
    std::unordered_map<const char*, RD> producers_;
    std::unordered_map<const char*, RD> consumers_;
    std::mutex mu_;
    std::unordered_map<const char*,std::set<int>> consume_toppar_;
    int partitions_cnt;
    Topic currConsumerTopic;
    int currConsumerPartition;
};
}  // namespace ps
#endif //PSLITE_KAFKA_VAN_H_
