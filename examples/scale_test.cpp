#include "rdkafka.h"
#include <string.h>
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>

using namespace std;
const char *brokers;
const char *topic;
std::atomic<size_t> message_count;
std::atomic<size_t> message_bytes;

#define BATCH_SIZE 1024
struct TopicConsumer
{
    TopicConsumer(bool ev = false)
    {
        char errstr[0x200];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
        rd_kafka_conf_set(conf, "session.timeout.ms", "6000", NULL, 0);
        rd_kafka_conf_set(conf, "enable.sparse.connections", "true", NULL, 0);

        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (rk == NULL)
        {
            cout << errstr << endl;
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
        rd_kafka_message_t *messages[BATCH_SIZE];
        bool eof = false;
        while (!eof)
        {
            rd_kafka_poll(rk, 0);
            ssize_t count = rd_kafka_consume_batch(rkt, partition, 1000, messages, BATCH_SIZE);
            if (count < 0)
            {
                cout << rd_kafka_err2str(rd_kafka_last_error()) << endl;
                return;
            }
            if (count == 0)
                continue;
            size_t bytes = 0;
            for (ssize_t i = 0; i < count; i++)
            {
                rd_kafka_message_t *msg = messages[i];
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
                        cout << rd_kafka_err2str(msg->err) << endl;
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

    void Consume(int index, int workers, int partitions)
    {
        auto q = rd_kafka_queue_new(rk);
        for (int i = index; i < partitions; i += workers)
        {
            rd_kafka_consume_start_queue(rkt, i, 0, q);
        }
        ConsumeQueueWorker(q);
        for (int i = index; i < partitions; i += workers)
        {
            rd_kafka_consume_stop(rkt, i);
        }
        rd_kafka_queue_destroy(q);
    }

    void ConsumeQueueWorker(rd_kafka_queue_t *rkq)
    {
        rd_kafka_message_t *messages[BATCH_SIZE];
        bool eof = false;
        while (!eof)
        {
            rd_kafka_poll(rk, 0);
            ssize_t count = rd_kafka_consume_batch_queue(rkq, 1000, messages, BATCH_SIZE);
            if (count < 0)
            {
                cout << rd_kafka_err2str(rd_kafka_last_error()) << endl;
                return;
            }
            if (count == 0)
                continue;
            size_t bytes = 0;
            for (ssize_t i = 0; i < count; i++)
            {
                rd_kafka_message_t *msg = messages[i];
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
                        cout << rd_kafka_err2str(msg->err) << endl;
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

    void ConsumeQueue(int partition)
    {
        auto q = rd_kafka_queue_new(rk);
        rd_kafka_consume_start_queue(rkt, partition, 0, q);
        ConsumeQueueWorker(q);
        rd_kafka_consume_stop(rkt, partition);
        rd_kafka_queue_destroy(q);
    }
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
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
        const rd_kafka_metadata_t *meta;
        rd_kafka_metadata(rk, 0, rkt, &meta, 2000);
        partition = meta->topics[0].partition_cnt;
    }
    ~MetaConsumer()
    {
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    int partition;
};

int GetTopicPartitions()
{
    MetaConsumer meta;
    return meta.partition;
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];

    int partitions = GetTopicPartitions();
    cout << "reading " << partitions << " partitions" << endl;
    if (strstr(argv[0], "kafka_1c1p"))
    {
        cout << "create one consumer for each partition" << endl;
        cout << "create " << partitions << " workers" << endl;

        vector<thread> threads;
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back([](int part) {
                TopicConsumer c;
                c.Consume(part);
            },
                                 i);
        }

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else if (strstr(argv[0], "kafka_1cnp"))
    {
        cout << "create one consumer for each partition" << endl;
        cout << "create " << partitions << " workers" << endl;

        vector<thread> threads;
        for (int i = 0; i < thread::hardware_concurrency(); i++)
        {
            threads.emplace_back([](int index, int partitions) {
                TopicConsumer c;
                c.Consume(index, thread::hardware_concurrency(), partitions);
            },
                                 i, partitions);
        }

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else if (strstr(argv[0], "kafka_1cap"))
    {
        cout << "create one consumer, shared by all workers" << endl;
        cout << "create " << partitions << " workers" << endl;

        TopicConsumer c;
        vector<thread> threads;
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back([&c](int part) {
                c.Consume(part);
            },
                                 i);
        }

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else if (strstr(argv[0], "kafka_1q1p"))
    {
        cout << "create one consumer, shared by all workers" << endl;
        cout << "create " << partitions << " queues" << endl;

        TopicConsumer c(true);
        vector<thread> threads;
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back([&c](int part) {
                c.ConsumeQueue(part);
            },
                                 i);
        }

        for (auto &t : threads)
        {
            t.join();
        }
    }

    cout << "Messages: " << message_count << endl;
    cout << "Bytes: " << message_bytes << endl;

    return 0;
}