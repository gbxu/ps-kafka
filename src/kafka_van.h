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
            return "TOSCHDULER";//to scheduler
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
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err)
        CHECK(0);
    /* The rkmessage is destroyed automatically by librdkafka */
}

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
            //while (rd_kafka_outq_len(rk) > 0) rd_kafka_poll(rk, 10);
            /* Destroy topic */
            rd_kafka_topic_destroy(rkt);
            /* Destroy handle */
            rd_kafka_destroy(rk);

        }
    }

    void Bind(const char *brokers, Topic topic) override {
        currConsumerTopic = topic;
        currConsumerPartition = 0;
        //consumer
        CHECK_NE(currConsumerTopic, NONE);//empty
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<" Bind "<<TopicToConst(currConsumerTopic);
        debug.Out();
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
        rd_kafka_conf_set(conf, "group.id", "0",
                          errstr, sizeof(errstr));// dont need the group.id now
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
        DebugOut tmp = DebugOut(my_node_);
        tmp.stream()<<" Connect: "<<TopicToConst(topic);
        tmp.Out();
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
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        //producer
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
            fprintf(stderr,
                    "%% Failed to create new producer: %s\n", errstr);
            return;
        }
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
            fprintf(stderr, "%% No valid brokers specified\n");
            exit(1);
        }
        //topic
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, TopicToConst(topic), NULL);
        if (!rkt) {
            fprintf(stderr, "%% Failed to create topic object: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return;
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
        //CHECK_NE(topic, NONE);
        //CHECK_NE(partition, kEmpty);
        auto it = producers_.find(TopicToConst(topic));
        if (it == producers_.end()) {
            DebugOut debug = DebugOut(my_node_);
            debug.stream()<<" scheduler:"<<Postoffice::Get()->is_scheduler()<<" "<<" cant find:"<<TopicToConst(topic)<<partition;
            debug.Out();
            return -1;
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;
        // send meta
        int meta_size; char* meta_buf;
        PackMeta(msg.meta, &meta_buf, &meta_size);
        int n = msg.data.size();
        if(n == 0){
            retry0:
            if(rd_kafka_produce(rkt,
                                partition,
                                RD_KAFKA_MSG_F_COPY,
                                meta_buf, meta_size,
                                "f", 1,/*no more*/
                                NULL) == -1){
                /* Poll to handle delivery reports */
                if (rd_kafka_last_error() ==
                    RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    /* If the internal queue is full, wait for
                     * messages to be delivered and then retry.
                     * The internal queue represents both
                     * messages to be sent and messages that have
                     * been sent or failed, awaiting their
                     * delivery report callback to be called.
                     *
                     * The internal queue is limited by the
                     * configuration property
                     * queue.buffering.max.messages */
                    rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                    goto retry0;
                }
            }

        }else{
            retry1:
            if(rd_kafka_produce(rkt,
                                partition,
                                RD_KAFKA_MSG_F_COPY,
                                meta_buf, meta_size,
                                "t", 1,/*more*/
                                NULL) == -1){
                if (rd_kafka_last_error() ==
                    RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                    goto retry1;
                }
            }
        }
        //Debug: send meta message
        DebugOut debug = DebugOut(my_node_);
        debug.stream()<<" sendmsg:"<<TopicToConst(topic)<<partition \
                        <<" :"<<msg.meta.control.DebugString() \
                        <<" size :"<<meta_size;
        debug.Out();

        rd_kafka_poll(rk, 0/*non-blocking*/);
        int send_bytes = meta_size;
        // send data
        for (int i = 0; i < n; ++i) {
            SArray<char>* data = new SArray<char>(msg.data[i]);
            int data_size = data->size();
            if (i == n - 1) {
                retry2:
                if(rd_kafka_produce(rkt,
                                    partition,
                                    RD_KAFKA_MSG_F_COPY,
                                    data, data_size,
                                    "f", 1,/*no more*/
                                    NULL) == -1){
                    if (rd_kafka_last_error() ==
                        RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                        //queue full
                        rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                        goto retry2;
                    }
                }
            } else{
                retry3:
                if(rd_kafka_produce(rkt,
                                    partition,
                                    RD_KAFKA_MSG_F_COPY,
                                    data, data_size,
                                    "t", 1,/*more*/
                                    NULL) == -1){
                    if (rd_kafka_last_error() ==
                        RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                        //queue full
                        rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                        goto retry3;
                    }
                }
            }
            rd_kafka_poll(rk, 0/*non-blocking*/);
            send_bytes += data_size;
        }
        return send_bytes;
    }
    int StartConsumer(){
        if (my_node_.id != Node::kEmpty){
            //currConsumerTopic = topic when Bind()
            //currConsumerPartition = 0 when Bind()

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
        /*---------------*/
        for (int i = 0; ; ++i) {
            rd_kafka_message_t *rkmessage;
            rd_kafka_resp_err_t err;

            /* Poll for errors, etc. */
            rd_kafka_poll(rk, 0);

            /* Consume single message.
             * See rdkafka_performance.c for high speed
             * consuming of messages. */
            while(1){
                rkmessage = rd_kafka_consume(rkt, currConsumerPartition, 1000);//block:not time out 1000ms
                if(!rkmessage){
                    continue;
                } else if(rkmessage->err){
                    //printf("debug:err:%s\n",rd_kafka_err2str(rkmessage->err));
                    continue;
                } else {
                    break;
                }
            }
            CHECK_EQ(rkmessage->err,0);
            char* buf = CHECK_NOTNULL((char *)rkmessage->payload);
            size_t size = rkmessage->len;
            recv_bytes += size;

            char * tmp = (char *)rkmessage->key;
            if (i == 0) {//meta
                // identify
                msg->meta.recver = my_node_.id;
                // task
                UnpackMeta(buf, size, &(msg->meta));//
                rd_kafka_message_destroy(rkmessage);

                DebugOut debug = DebugOut(my_node_);
                debug.stream()<<" recvmsg from "<<TopicToConst(currConsumerTopic) \
                            <<currConsumerPartition \
                            <<" :"<<msg->meta.control.DebugString() \
                            <<" size:"<<size;
                debug.Out();
                if (tmp[0] == 'f') break;
            } else {
                // zero-copy
                SArray<char> data;
                data.reset(buf, size, [rkmessage, size](char* buf) {
                    rd_kafka_message_destroy(rkmessage);
                });
                msg->data.push_back(data);

                DebugOut debug = DebugOut(my_node_);
                debug.stream()<<" 2recvmsg from "<<TopicToConst(currConsumerTopic) \
                            <<currConsumerPartition \
                            <<" :"<<msg->meta.control.DebugString() \
                            <<" size:"<<size;
                debug.Out();

                if (tmp[0] == 'f') break;
            }
        }
        return recv_bytes;
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
