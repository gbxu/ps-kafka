//
// DONE:Created by gbxu on 18-4-20.
//

#ifndef PSLITE_KAFKA_VAN_H
#define PSLITE_KAFKA_VAN_H
#include <stdlib.h>
#include <thread>
#include <string>
#include <stdio.h>
#include <signal.h>
#include "ps/internal/van.h"
#include <librdkafka/rdkafka.h>

#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {
/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */

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
        if(my_node_.role == Node::WORKER){
            partitions_cnt = Postoffice::Get()->num_workers();//consumer partitions
        } else if(my_node_.role = Node::SERVER){
            partitions_cnt = Postoffice::Get()->num_servers();
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

    void Bind(const char *brokers, Meta::Topic tp) override {
        //consumer
        CHECK_NE(tp, Meta::NONE);//empty

        auto it = consumers_.find((char *)tp);// null
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
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        //consumer
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
            fprintf(stderr,
                    "%% Failed to create new consumer: %s\n", errstr);
            return;
        }
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
            fprintf(stderr, "%% No valid brokers specified\n");
            exit(1);
        }
        //topic
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, (const char *)tp, NULL);
        if (!rkt) {
            fprintf(stderr, "%% Failed to create topic object: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return;
        }
        RD rd = {rk, rkt};
        consumers_[(char *)tp] = rd;

    }

    void Connect(const char *brokers, Meta::Topic tp) override {
        //producer
        // by gbxu:
        //brokers ip:port,ip:port,ip:port
        //brokers ip,ip:port,ip:port //default port is 9092
        CHECK_NE(tp, Meta::NONE);//empty

        auto it = producers_.find((char *)tp);// null
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
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, (const char *)tp, NULL);
        if (!rkt) {
            fprintf(stderr, "%% Failed to create topic object: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return;
        }
        RD rd = {rk, rkt};
        producers_[(char *)tp] = rd;
    }


    int SendMsg(Message& msg) override {

        std::lock_guard<std::mutex> lk(mu_);
        //topic partition
        msg.meta.sender = my_node_.id;
        msg.meta.topic = Postoffice::Get()->IDtoRoletoTopic(msg.meta.recver);
        msg.meta.partition = Postoffice::IDtoRank(msg.meta.recver);
        // find the producer
        CHECK_NE(msg.meta.topic, Meta::NONE);
        CHECK_NE(msg.meta.partition,Meta::kEmpty);
        Meta::Topic tp = {msg.meta.topic};
        auto it = producers_.find((char *)tp);
        if (it == producers_.end()) {
            return -1;
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;
        // send meta
        int meta_size; char* meta_buf;
        PackMeta(msg.meta, &meta_buf, &meta_size);
        int n = msg.data.size();
        int msg_more = 0;
        if(n != 0){
            msg_more = 1;
        }
        retry0:
        if(rd_kafka_produce(rkt,
                            msg.meta.partition,
                            RD_KAFKA_MSG_F_COPY,
                            meta_buf, meta_size,
                            (const void*)msg_more, 1,
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
        rd_kafka_poll(rk, 0/*non-blocking*/);
        int send_bytes = meta_size;
        // send data
        for (int i = 0; i < n; ++i) {
            SArray<char>* data = new SArray<char>(msg.data[i]);
            int data_size = data->size();
            if (i == n - 1) msg_more = 0;
            retry1:
            if(rd_kafka_produce(rkt,
                                msg.meta.recver,
                                RD_KAFKA_MSG_F_COPY,
                                data, data_size,
                                (const void*)msg_more, 1,
                                NULL) == -1){
                if (rd_kafka_last_error() ==
                    RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    //queue full
                    rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                    goto retry1;
                }
            }
            rd_kafka_poll(rk, 0/*non-blocking*/);
            send_bytes += data_size;
        }
        return send_bytes;
    }

    int RecvMsg(Message* msg) override {
        msg->data.clear();
        size_t recv_bytes = 0;
        // find the consumer
        auto it = consumers_.find((char *)Postoffice::IDtoRoletoTopic(my_node_.id));
        if (it == consumers_.end()) {
            CHECK(0);
        }
        RD rd = it->second;
        rd_kafka_t *rk = rd.rk;
        rd_kafka_topic_t *rkt = rd.rkt;

        /* Start consuming */
        if (rd_kafka_consume_start(rkt,
                                   Postoffice::IDtoRank(my_node_.id),
                                   RD_KAFKA_OFFSET_END) == -1){
            CHECK(0);
        }

        for (int i = 0; ; ++i) {
            rd_kafka_message_t *rkmessage;
            rd_kafka_resp_err_t err;

            /* Poll for errors, etc. */
            rd_kafka_poll(rk, 0);

            /* Consume single message.
             * See rdkafka_performance.c for high speed
             * consuming of messages. */
            rkmessage = rd_kafka_consume(rkt, Postoffice::IDtoRank(my_node_.id), 1000);// time out 1000ms
            CHECK_NE(rkmessage->err,0);
            char* buf = CHECK_NOTNULL((char *)rkmessage->payload);
            size_t size = rkmessage->len;
            recv_bytes += size;

            if (i == 0) {//data
                // identify
                msg->meta.recver = my_node_.id;
                // task
                UnpackMeta(buf, size, &(msg->meta));//
                rd_kafka_message_destroy(rkmessage);
                if (!rkmessage->key) break;
            } else {
                // zero-copy
                SArray<char> data;
                data.reset(buf, size, [rkmessage, size](char* buf) {
                    rd_kafka_message_destroy(rkmessage);
                });
                msg->data.push_back(data);
                if (!rkmessage->key) break;
            }
        }
        return recv_bytes;
    }

private:
    /**
     * \brief node_id to the socket for sending data to this node
     */
    std::unordered_map<char*, RD> producers_;
    std::unordered_map<char*, RD> consumers_;
    std::mutex mu_;
    int partitions_cnt;
};
}  // namespace ps
#endif //PSLITE_KAFKA_VAN_H
