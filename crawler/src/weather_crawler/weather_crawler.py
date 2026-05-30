import os
import sys
import json
import logging
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from weather_fetcher import fetch_weather_data, reset_key_cycle
from kafka_producer import send_to_kafka


logger = logging.getLogger('weather_crawler')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

KAFKA_TOPIC = 'weather_data'
CRAWL_INTERVAL_SECONDS = max(
    60,
    int(os.getenv('WEATHER_CRAWL_INTERVAL_SECONDS', '3600'))
)

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    locations = json.load(file)

def run_crawler():
    reset_key_cycle()
    sent_count = 0

    for location in locations:
        loc_name = f"{location['name']}, {location['country_code']}"

        logger.info(f'Đang lấy dữ liệu cho: {loc_name}')

        weather_data = fetch_weather_data(location['latitude'], location['longitude'])
        
        if weather_data:
            weather_data['address'] = loc_name
            
            send_to_kafka(KAFKA_TOPIC, weather_data)
            sent_count += 1

        else:
            continue

    return sent_count


def run_forever():
    while True:
        started_at = time.monotonic()
        sent_count = run_crawler()
        elapsed = time.monotonic() - started_at
        sleep_seconds = max(0, CRAWL_INTERVAL_SECONDS - elapsed)
        logger.info(
            'Hoàn tất chu kỳ: đã gửi %s/%s địa điểm. Chu kỳ tiếp theo sau %.0f giây',
            sent_count,
            len(locations),
            sleep_seconds,
        )
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    logger.info(
        'Khởi động Weather Crawler realtime, khoảng lặp %s giây',
        CRAWL_INTERVAL_SECONDS,
    )
    run_forever()
