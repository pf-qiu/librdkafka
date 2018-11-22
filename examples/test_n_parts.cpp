#include "rdkafka.h"
#include <string.h>
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include <chrono>

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
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
        rd_kafka_conf_set(conf, "session.timeout.ms", "6000", NULL, 0);

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (rk == NULL)
        {
            printf("%s\n", errstr);
            exit(1);
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

    struct ConsumeData
    {
        bool eof;
        size_t bytes;
        int messages;
    };
    void ConsumeCallbackWorker(int partition)
    {
        ConsumeData data;
        data.eof = false;

        while(!data.eof)
        {
            data.bytes = 0;
            data.messages = 0;
            int count = rd_kafka_consume_callback(rkt, partition, 0, 
            [](rd_kafka_message_t* msg, void* opaque){
                ConsumeData* cd = (ConsumeData*)opaque;
                if (msg->err)
                {
                    cd->eof = true;
                    if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
                    {
                        return;
                    }
                    else
                    {
                        printf("message error: %d, %s\n", msg->err, rd_kafka_err2str(msg->err));
                        return;
                    }
                }
                cd->messages++;
                cd->bytes += msg->len;
            }, &data);
            if (count < 0)
            {
                printf("%s\n", rd_kafka_err2str(rd_kafka_last_error()));
                return;
            }
            if (count == 0)
            {
                this_thread::sleep_for(chrono::milliseconds(1000));
                continue;
            }

            message_count += data.messages;
            message_bytes += data.bytes;
        }
    }
    void ConsumeCallback(int partition)
    {
        rd_kafka_consume_start(rkt, partition, 0);
        ConsumeCallbackWorker(partition);
        rd_kafka_consume_stop(rkt, partition);
    }
    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    int partition;

    vector<rd_kafka_queue_t*> queues;
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
                c.ConsumeCallback(part);
            }, i);
        }
        for (auto& t : threads)
        {
            t.join();
        }
    }

    cout << "Messages: " << message_count << endl;
    cout << "Bytes: " << message_bytes << endl;
    
    return 0;
}