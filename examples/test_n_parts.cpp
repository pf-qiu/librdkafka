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
    TopicConsumer()
    {
        char errstr[0x200];
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
        if (rk == NULL)
        {
            printf("%s\n", errstr);
        }
        rd_kafka_brokers_add(rk, brokers);
        rkt = rd_kafka_topic_new(rk, topic, 0);

    }
    ~TopicConsumer()
    {
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    void ConsumeWorker(int partition)
    {
        rd_kafka_message_t* messages[BATCH_SIZE];
        bool eof = false;
        while(!eof)
        {
            rd_kafka_poll(rk, 0);
            ssize_t count = rd_kafka_consume_batch(rkt, partition, 1000, messages, BATCH_SIZE);
            if (count < 0)
            {
                printf("%s\n", rd_kafka_err2str(rd_kafka_last_error()));
                return;
            }
            if (count == 0)
                continue;
            size_t bytes = 0;
            for (ssize_t i = 0; i < count; i++)
            {
                rd_kafka_message_t* msg = messages[i];
                if (msg->err)
                {
                    eof = true;
                    if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
                    {
                        count--;
                        break;
                    }
                    else
                    {
                        printf("message error: %d, %s\n", msg->err, rd_kafka_err2str(msg->err));
                        count = 0;
                        break;
                    }
                }
                bytes += msg->len;
                rd_kafka_message_destroy(msg);
            }
            message_count += count;
            message_bytes += bytes;
        }
    }

    void Consume(int partition)
    {
        rd_kafka_consume_start(rkt, partition, 0);
        ConsumeWorker(partition);
        rd_kafka_consume_stop(rkt, partition);
    }
    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    int partition;
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
        partition = meta->topics[0].partition_cnt;
    }
    ~MetaConsumer()
    {
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    int partition;
};

int GetPartition()
{
    MetaConsumer meta;
    return meta.partition;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];

    int partitions = GetPartition();
    printf("reading %d partitions\n", partitions);
    if (strstr(argv[0], "single"))
    {
        cout << "single" << endl;
        
        vector<thread> threads;
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back([](int part){
                TopicConsumer c;
                c.Consume(part);
            }, i);
        }
        
        for (auto& t : threads) {
             t.join();
        }
    }
    else if (strstr(argv[0], "multi"))
    {
        cout << "multi" << endl;
        TopicConsumer c;
        vector<thread> threads;
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back([&c](int part){
             c.Consume(part);
            }, i);
        }

        for (auto& t : threads) {
             t.join();
        }
    }

    cout << "Messages: " << message_count << endl;
    cout << "Bytes: " << message_bytes << endl;
    
    return 0;
}