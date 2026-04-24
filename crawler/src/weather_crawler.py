import os
import json
import logging
from weather_fetcher import fetch_weather_data
from kafka_producer import send_to_kafka


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

KAFKA_TOPIC = 'raw_weather_data'

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    data = json.load(file)

LOCATIONS = [f'{item['latitude']},{item['longitude']}' for item in data]

def run_crawler():
    for location in LOCATIONS:
        logging.info(f'Đang lấy dữ liệu cho: {location}')
        weather_data = fetch_weather_data(location)
        
        if weather_data:
            send_to_kafka(KAFKA_TOPIC, weather_data)

        else:
            continue
        
    return True

if __name__ == "__main__":
    logging.info('Khởi động Weather Crawler')
    run_crawler()
    logging.info('Đã tắt Weather Crawler')
