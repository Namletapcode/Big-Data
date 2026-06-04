import os
import sys
import json
import logging
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from weather_fetcher import fetch_weather_data
from kafka_producer import send_to_kafka


logger = logging.getLogger('weather_crawler')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    locations = json.load(file)

def run_crawler(type):
    KAFKA_TOPIC = 'weather_data' if type == 'current' else 'weather_forecast_data'

    for location in locations:
        loc_name = f"{location['name']}, {location['country_code']}"

        logger.info(f'Đang lấy dữ liệu cho: {loc_name}')

        weather_data = fetch_weather_data(location['latitude'], location['longitude'], type=type)
        
        if weather_data:
            weather_data['address'] = loc_name
            
            send_to_kafka(KAFKA_TOPIC, weather_data)

        else:
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['current', 'fcst'], required=True)
    args = parser.parse_args()

    logger.info(f'Khởi động Weather Crawler (Chế độ: {args.type})')
    run_crawler(args.type)
    logger.info('Đã tắt Weather Crawler')
