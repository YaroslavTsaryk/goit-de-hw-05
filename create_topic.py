from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
my_name = "YT"
bs_topic_name = f'{my_name}_building_sensors'
ta_topic_name = f'{my_name}_temperature_alerts'
ha_topic_name = f'{my_name}_humidity_alerts'
num_partitions = 2
replication_factor = 1

bs_topic = NewTopic(name=bs_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
ta_topic = NewTopic(name=ta_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
ha_topic = NewTopic(name=ha_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[bs_topic], validate_only=False)
    print(f"Topic '{bs_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

try:
    admin_client.create_topics(new_topics=[ta_topic], validate_only=False)
    print(f"Topic '{ta_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")


try:
    admin_client.create_topics(new_topics=[ha_topic], validate_only=False)
    print(f"Topic '{ha_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
# print(admin_client.list_topics())

#fs = admin_client.delete_topics(['YT_topic_1'])

[print(topic) for topic in admin_client.list_topics() if "YT" in topic]

# Закриття зв'язку з клієнтом
admin_client.close()