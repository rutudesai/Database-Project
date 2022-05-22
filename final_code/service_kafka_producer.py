import socket
import app_constants as apc
import service_datafile as dfservice
import json
# Using open source Apache Kafka library:
from kafka.admin import KafkaAdminClient, NewTopic
from confluent_kafka import Producer

class KafkaProducerService:

    def __init__(self):
        self.conf = {'bootstrap.servers': apc.KAFKA_BROKERS, 'client.id': socket.gethostname()}
        self.admin_client = KafkaAdminClient(
            bootstrap_servers = apc.KAFKA_BROKERS, 
            client_id='final-project-demo'
        )
        self.producer = Producer(self.conf)


    def test_connection(self):
             
        try:
            response = self.admin_client.list_topics()
            print(response)
        except Exception as e:
            print("Oops! unable to list topics on the broker.", e)

    def setup(self):
        topic_list = []
        topic_list.append(NewTopic(name="tweets", num_partitions=5, replication_factor=2))
        try:
            response = self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(response)
        except Exception as e:
            print("Oops! Error creating these topics.", e)

    # To receive notification of delivery success or failure, you can pass a callback parameter. 
    # This can be any callable, for example, a lambda, function, bound method, or callable object.

    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print(f"""Message produced: 
            TOPIC - {msg.topic()} 
            KEY - {msg.key().decode("utf-8")} 
            PARTITION - {msg.partition()}""")
                        # VALUE - {msg.value().decode("utf-8")} 
        
    def produce_tweet(self, tweet):
        try:
            self.producer.produce(topic=apc.KAFKA_TOPIC, key=str(tweet["id"]), value=json.dumps(tweet), callback=self.acked)
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            response = self.producer.poll(1)
            if response == None:
                return False
            else:
                return True
        except (Exception) as error:
                    print("Oops! Kafka process_tweet error occured.", error)
                
        return False        

