from kafka import KafkaConsumer

consumer = KafkaConsumer('test_topic', bootstrap_servers=['localhost:9999'], api_version=(1, 3, 5))
print("Consumer created")

for msg in consumer:
    print(msg)
