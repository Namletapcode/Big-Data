import os
import json
import logging
from weather_fetcher import fetch_weather_data
from kafka_producer import send_to_kafka


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

KAFKA_TOPIC = 'weather_data'

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    locations = json.load(file)

def run_crawler():
    for location in locations:
        loc_name = f"{location['name']}, {location['country_code']}"

        logger.info(f'Đang lấy dữ liệu cho: {loc_name}')

        weather_data = fetch_weather_data(location['latitude'], location['longitude'])
        
        if weather_data:
            weather_data['address'] = loc_name
            
            send_to_kafka(KAFKA_TOPIC, weather_data)

        else:
            continue
        
    return True

if __name__ == "__main__":
    logger.info('Khởi động Weather Crawler')
    run_crawler()
    logger.info('Đã tắt Weather Crawler')
