from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9999'], api_version=(1, 3, 5))
print("Producer created")

future = producer.send('test_topic', b'some_message')
print("Message sent")
