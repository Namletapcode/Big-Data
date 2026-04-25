import os
import json
import logging
import time
from datetime import datetime
from pollution_fetcher_v1 import fetch_pollution_data_v1
from pollution_fetcher_v2 import fetch_pollution_data_v2
from kafka_producer import send_to_kafka


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

KAFKA_TOPIC = 'pollution_data'

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    locations = json.load(file)

def mean(a, b):
    if a is None and b is None:
        return 0
    
    if a is None:
        return b
    
    if b is None:
        return a
    
    return round((a + b) / 2, 2)

def run_crawler():
    for location in locations:
        loc_name = f"{location['name']}, {location['country_code']}"
        record_time = datetime.now().strftime('%Y-%m-%dT%H:00:00')
        lat = location['latitude']
        lon = location['longitude']

        logger.info(f'Đang lấy dữ liệu cho: {loc_name}')

        data_v1 = fetch_pollution_data_v1(lat, lon)
        data_v2 = fetch_pollution_data_v2(lat, lon)

        current_v1 = {}
        units_v1 = {}
        components_v2 = {}

        if not data_v1 or 'current' not in data_v1:
            logger.warning(f'Không có dữ liệu {loc_name} từ Open Meteo')

        else:
            current_v1 = data_v1['current']
            units_v1 = data_v1.get('current_units', {})

        if not data_v2 or 'list' not in data_v2 or len(data_v2['list']) == 0:
            logger.warning(f'Không có dữ liệu {loc_name} từ Open Weather')

        else:
            components_v2 = data_v2['list'][0].get('components', {})

        if not current_v1 and not components_v2:
            logger.error(f'Không có dữ liệu {loc_name}')
            continue

        pollution_data = {
            'location': loc_name,
            'latitude': lat,
            'longitude': lon,
            'time': record_time,
            'units': units_v1,
            'pm10': mean(current_v1.get('pm10'), components_v2.get('pm10')),
            'pm2_5': mean(current_v1.get('pm2_5'), components_v2.get('pm2_5')),
            'carbon_monoxide': mean(current_v1.get('carbon_monoxide'), components_v2.get('co')),
            'nitrogen_monoxide': components_v2.get('no'),
            'nitrogen_dioxide': mean(current_v1.get('nitrogen_dioxide'), components_v2.get('no2')),
            'sulphur_dioxide': mean(current_v1.get('sulphur_dioxide'), components_v2.get('so2')),
            'ozone': mean(current_v1.get('ozone'), components_v2.get('o3')),
            'amoniac': components_v2.get('nh3'),
            'aerosol_optical_depth': current_v1.get('aerosol_optical_depth'),
            'dust': current_v1.get('dust'),
            'uv_index': current_v1.get('uv_index'),
            'uv_index_clear_sky': current_v1.get('uv_index_clear_sky'),
            'us_aqi': current_v1.get('us_aqi'),
            'european_aqi': current_v1.get('european_aqi')
        }

        send_to_kafka(KAFKA_TOPIC, pollution_data)

        time.sleep(1)

if __name__ == "__main__":
    logger.info('Khởi động Pollution Crawler')
    run_crawler()
    logger.info('Đã tắt Pollution Crawler')
