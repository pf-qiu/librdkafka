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
    TopicConsumer(int partitions) : partition_count(partitions)
    {
        char errstr[0x200];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
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
		rd_kafka_poll_set_consumer(rk);
        rd_kafka_brokers_add(rk, brokers);
		
		auto topic_conf = rd_kafka_topic_conf_new();
		rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "earliest", NULL, 0);

        rkt = rd_kafka_topic_new(rk, topic, topic_conf);
        auto list = rd_kafka_topic_partition_list_new(partition_count);
        for (int i = 0; i < partition_count; i++)
        {
            auto p = rd_kafka_topic_partition_list_add(list, topic, i);
            p->offset = 0;
        }
        //rd_kafka_subscribe(rk, list);
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
		rd_kafka_message_t* messages[BATCH_SIZE];
        while (finished < partition_count)
        {
			ssize_t count = rd_kafka_consumer_poll_batch(rk, 1000, messages, BATCH_SIZE);
			if (count < 0)
			{
				cout << rd_kafka_err2str(rd_kafka_last_error()) << endl;
				exit(1);
			}
			if (count == 0)
				continue;

			int msg_count = 0;
			size_t bytes = 0;
			for (ssize_t i = 0; i < count; i++)
			{
				rd_kafka_message_t *msg = messages[i];
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
					msg_count++;
					bytes += msg->len;
				}
				rd_kafka_message_destroy(msg);
			}
			message_count += msg_count;
			message_bytes += bytes;
        }
    }
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    int partition_count;
    atomic<int> finished;
};
struct MetaConsumer
{
    MetaConsumer() : rk(0), rkt(0), meta(0)
    {
        char errstr[0x200];
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
        rd_kafka_brokers_add(rk, brokers);
        rkt = rd_kafka_topic_new(rk, topic, 0);
        rd_kafka_metadata(rk, 0, rkt, &meta, 2000);
    }
    ~MetaConsumer()
    {
        rd_kafka_metadata_destroy(meta);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
    }

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    const rd_kafka_metadata_t *meta;
};

struct Meta
{
    Meta()
    {
        MetaConsumer consumer;
        if (consumer.meta)
        {
            partition_count = consumer.meta->topics[0].partition_cnt;
            broker_count = consumer.meta->broker_cnt;
        }
    }
    int partition_count;
    int broker_count;
};

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];

    Meta meta;

    cout << "reading " << meta.partition_count << " partitions" << endl;
    cout << "create one consumer, shared by all workers" << endl;
    cout << "create " << meta.partition_count << " workers" << endl;
    TopicConsumer c(meta.partition_count);
    vector<thread> threads;

    for (int i = 0; i < thread::hardware_concurrency(); i++)
    {
        threads.emplace_back([&c](int index) {
            c.Consume();
        },
                             i);
    }
    for (auto &t : threads)
    {
        t.join();
    }

    cout << "Messages: " << message_count << endl;
    cout << "Bytes: " << message_bytes << endl;

    return 0;
}