#include "rdkafka.h"
#include <string.h>
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include <map>
#include <unordered_map>
#include <memory>

using namespace std;
const char *brokers;
const char *topic;
std::atomic<size_t> message_count;
std::atomic<size_t> message_bytes;

#define BATCH_SIZE 1024
struct TopicConsumer
{
    TopicConsumer() : rk(NULL), rkt(NULL)
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

    void ConsumeQueueWorker(rd_kafka_queue_t *rkq)
    {
        rd_kafka_message_t *messages[BATCH_SIZE];
        bool eof = false;
        while (!eof)
        {
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
        rd_kafka_metadata(rk, 0, rkt, &meta, 2000);
    }
    ~MetaConsumer()
    {
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    const rd_kafka_metadata_t *meta;
};

struct PartitionMeta
{
    int id;
    int leader;
};

struct BrokerMeta
{
    int id;
};

struct Meta
{
    Meta()
    {
        MetaConsumer mc;
        for (int i = 0; i < mc.meta->broker_cnt; i++)
        {
            broker_map.emplace_back(mc.meta->brokers[i].id);
        }

        for (int i = 0; i < mc.meta->topics->partition_cnt; i++)
        {
            auto p = mc.meta->topics->partitions + i;
            partition_map[p->id] = p->leader;
        }
    }
    vector<int> broker_map;
    map<int, int> partition_map;
};

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];
    Meta m;
    int partitions = m.partition_map.size();
    cout << "reading " << partitions << " partitions" << endl;
    if (strstr(argv[0], "kafka_1c1p"))
    {
        cout << "create one consumer for each partition" << endl;
        cout << "create " << partitions << " workers" << endl;

        vector<thread> threads;
        auto f = [](int partition) {
            TopicConsumer c;
            c.Consume(partition);
        };
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back(f, i);
        }

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else if (strstr(argv[0], "kafka_1cnp"))
    {
        cout << "create multiple consumers to match broker and partition" << endl;
        map<int, vector<int>> scheduler;
        for (int id : m.broker_map)
        {
            scheduler.emplace(id, vector<int>());
        }
        for (auto &kv : m.partition_map)
        {
            int partid = kv.first;
            int leaderid = kv.second;
            auto it = scheduler.find(leaderid);
            if (it == scheduler.end())
            {
                cout << "leader " << leaderid << " isnt available for partition " << partid << endl;
                exit(1);
            }
            it->second.emplace_back(partid);
        }

        vector<unique_ptr<TopicConsumer>> c;
        vector<thread> workers;
        auto f = [](TopicConsumer *c, int partition) { c->Consume(partition); };
        while (!scheduler.empty())
        {
            TopicConsumer *tc = new TopicConsumer();
            c.emplace_back(tc);

            for (auto it = scheduler.begin(); it != scheduler.end();)
            {
                workers.emplace_back(f, tc, it->second.back());
                it->second.pop_back();
                if (it->second.empty())
                {
                    it = scheduler.erase(it);
                }
                else
                {
                    it++;
                }
            }
        }

        for (auto &t : workers)
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
        auto f = [](TopicConsumer *c, int partition) {
            c->Consume(partition);
        };
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back(f, &c, i);
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

        TopicConsumer c;
        vector<thread> threads;
        auto f = [](TopicConsumer *c, int partition) {
            c->ConsumeQueue(partition);
        };
        for (int i = 0; i < partitions; i++)
        {
            threads.emplace_back(f, &c, i);
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