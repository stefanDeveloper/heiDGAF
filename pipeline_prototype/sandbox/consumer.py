from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': "localhost:8097,localhost:8098,localhost:8099",
    'group.id': "my_group",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['Inspect'])

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
