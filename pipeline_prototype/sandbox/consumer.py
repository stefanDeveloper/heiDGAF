from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "my_group",
    'auto.offset.reset': 'earliest'
}

# utils.create_kafka_topic(
#     KAFKA_BROKER_HOST,
#     KAFKA_BROKER_PORT,
#     "Test",
# )

consumer = Consumer(conf)
consumer.subscribe(['192.168.0.0_24'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print('Received message: {}'.format(msg.value().decode('utf-8')))
finally:
    consumer.close()
