#include "rdkafka.h"
#include <string.h>
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>

using namespace std;
const char* brokers;
const char* topic;
std::atomic<size_t> message_count;
std::atomic<size_t> message_bytes;

#define BATCH_SIZE 1024
struct TopicConsumer
{
    TopicConsumer(int partitions) : partition_count(partitions)
    {
        char errstr[0x200];
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
        rd_kafka_conf_set(conf, "session.timeout.ms", "6000", NULL, 0);
        rd_kafka_conf_set(conf, "group.id", "testgroup", NULL, 0);
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", NULL, 0);

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (rk == NULL)
        {
            cout << errstr << endl;
            exit(1);
        }
        rd_kafka_brokers_add(rk, brokers);

        rkt = rd_kafka_topic_new(rk, topic, 0);
        auto list = rd_kafka_topic_partition_list_new(partition_count);
        for (int i = 0; i < partition_count; i++)
        {
            auto p = rd_kafka_topic_partition_list_add(list, topic, i);
            p->offset = 0;
        }
        rd_kafka_subscribe(rk, list);
        rd_kafka_assign(rk, list);
    }
    ~TopicConsumer()
    {
        rd_kafka_assign(rk, NULL);
        rd_kafka_consumer_close(rk);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    void Consume()
    {
        while(finished < partition_count)
        {
            rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, 1000);
            if (msg == NULL)
                continue;
            if (msg->err)
            {
                if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
                {
                    finished++;
                }
                else
                {
                    cout << rd_kafka_err2str(msg->err) << endl;
                    break;
                }
            }
            else
            {
                message_bytes += msg->len;
                message_count += 1;
            }
            rd_kafka_message_destroy(msg);
        }
    }
    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    int partition_count;
    atomic<int> finished;
};
struct MetaConsumer
{
    MetaConsumer()
    {
        char errstr[0x200];
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
        rd_kafka_brokers_add(rk, brokers);
        rkt = rd_kafka_topic_new(rk, topic, 0);
        const rd_kafka_metadata_t* meta;
        rd_kafka_metadata(rk, 0, rkt, &meta, 2000);
        partition_count = meta->topics[0].partition_cnt;
        broker_count = meta->broker_cnt;
    }
    ~MetaConsumer()
    {
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    int partition_count;
    int broker_count;
};

int main(int argc, char** argv) {
    if (argc < 3) {
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];

    MetaConsumer meta;

    cout << "reading " << meta.partition_count << " partitions" << endl;
    cout << "create one consumer, shared by all workers" << endl;
    cout << "create " << meta.partition_count << " workers" << endl;
    TopicConsumer c(meta.partition_count);
    vector<thread> threads;

    for (int i = 0; i < meta.broker_count; i++)
    {
        threads.emplace_back([&c](int index){
            c.Consume();
        }, i);
    }
    for (auto& t : threads) {
         t.join();
    }

    cout << "Messages: " << message_count << endl;
    cout << "Bytes: " << message_bytes << endl;

    return 0;
}