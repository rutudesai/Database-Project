from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka import Consumer
import app_constants as apc
import service_pgsql as pgsqlService
import sys
import json

class KafkaConsumerPGSql:

    def __init__(self):
        self.conf = {'bootstrap.servers': apc.KAFKA_BROKERS,
        'group.id': apc.KAFKA_PGSQL_CONSUMER,
        'enable.auto.commit': True,
#         'default.topic.config': {'auto.offset.reset': 'smallest'}
        'auto.offset.reset': 'earliest',
        'on_commit': self.commit_completed}
        self.consumer = Consumer(self.conf)
        cluster_metadata = self.consumer.list_topics()
        self.pgsqlDBService = pgsqlService.PGSQLService()
        print(cluster_metadata.topics)

    def commit_completed(self, err, partitions):
        if err:
            print(str(err))
        else:
            print("Committed partition offsets: " + str(partitions))

    def msg_process(self, msg):
        self.pgsqlDBService.process_tweet(json.loads(msg.value()))
        print(json.loads(msg.value()))
    
    def consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            msg_count = 0
            while True:

                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    self.msg_process(msg)
                    msg_count += 1
                    if msg_count % apc.KAFKA_MIN_COMMIT_COUNT == 0:
                        self.consumer.commit(asynchronous=True)
        except Exception as e:
            print("Oops! Error occured.", e)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

#Enable this code to run consumer
kafka_consumer = KafkaConsumerPGSql()
kafka_consumer.consume_loop([apc.KAFKA_TOPIC])