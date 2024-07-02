from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic


def create_kafka_topic(
    broker_host, broker_port, topic_name, num_partitions=3, replication_factor=3
):
    admin_client = AdminClient({"bootstrap.servers": f"{broker_host}:{broker_port}"})

    topic_list = [
        NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
    ]

    try:
        fs = admin_client.create_topics(topic_list)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created successfully")
            except KafkaException as e:
                print(f"Failed to create topic {topic}: {e}")
    except Exception as e:
        print(f"Exception while creating topic: {e}")


create_kafka_topic("localhost", "9092", "Prefilter")
create_kafka_topic("localhost", "9093", "Prefilter")
create_kafka_topic("localhost", "9094", "Prefilter")
create_kafka_topic("localhost", "9092", "Inspect")
create_kafka_topic("localhost", "9093", "Inspect")
create_kafka_topic("localhost", "9094", "Inspect")
