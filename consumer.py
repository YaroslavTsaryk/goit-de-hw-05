from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)


# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Назва топіку
my_name = "YT"
bs_topic_name = f'{my_name}_building_sensors'
ta_topic_name = f'{my_name}_temperature_alerts'
ha_topic_name = f'{my_name}_humidity_alerts'


# Підписка на тему
consumer.subscribe([bs_topic_name])

print(f"Subscribed to topic '{bs_topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")

        if 20<int(message.value['humidity'])>80:
            print(f"ALERT HUMIDITY {message.value['humidity']}")
            producer.send(ha_topic_name, key=message.key, value=message.value)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"Message {message} sent to topic '{ha_topic_name}' successfully.")

        if int(message.value['temperature'])>40:
            print(f"ALERT TEMPERATURE {message.value['temperature']}")
            producer.send(ta_topic_name, key=message.key, value=message.value)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"Message {message} sent to topic '{ta_topic_name}' successfully.")


except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer
