import json
from confluent_kafka import Producer


config = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(config)

def send_to_kafka(topic, data):
    try:
        json_data = json.dumps(data).encode('utf-8')

        producer.produce(topic, json_data)
        producer.flush()

    except Exception as e:
        print(f'Lỗi khi đẩy vào Kafka: {e}')
