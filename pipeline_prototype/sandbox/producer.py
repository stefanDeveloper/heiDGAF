from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)


def kafka_delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


for i in range(10):
    producer.produce('my_topic', key=str(i), value='my_value', callback=kafka_delivery_report)
    producer.poll(0)

producer.flush()
