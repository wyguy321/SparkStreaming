from kafka.admin import KafkaAdminClient, NewTopic

bootstrap_servers = ['XXXX.c2.kafka.us-east-1.amazonaws.com:9092','XXXX.c2.kafka.us-east-1.amazonaws.com:9092']
admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers, 
    client_id='test'
)

topic_list = []
topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)