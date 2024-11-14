import json
from kafka import KafkaConsumer

# Configuration de Kafka
KAFKA_TOPIC = 'tp-meteo'
KAFKA_SERVER = 'localhost:9092'

# Initialisation du consumer Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consommer et afficher les messages
for message in consumer:
    data = message.value       
    print(data)