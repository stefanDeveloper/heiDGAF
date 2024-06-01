import json
import ast

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
consumer.subscribe(['Prefilter'])

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
        # print('Received message: {}'.format(msg.value().decode('utf-8')))

        decoded_msg = msg.value().decode('utf-8')
        liste = json.loads(decoded_msg)
        print(ast.literal_eval(liste[0])["client_ip"])
finally:
    consumer.close()
