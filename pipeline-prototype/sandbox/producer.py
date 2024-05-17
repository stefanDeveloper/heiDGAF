from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9999")
admin_client.create_topics(new_topics=[NewTopic(name='test_topic', num_partitions=1, replication_factor=1)])

producer = KafkaProducer(bootstrap_servers=['localhost:9999'], api_version=(1, 3, 5))
print("Producer created")

producer.send('test_topic', b'some_message')
print("Message sent")
