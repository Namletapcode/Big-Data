import os
import requests
import json
import logging
import time
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

API_KEY_LIST = list(os.getenv('OPEN_WEATHER_API_KEY_LIST', '').split(','))
BASE_URL = 'http://api.openweathermap.org/data/2.5/air_pollution/history'
idx = 0

def fetch_pollution_data_v2(lat, lon, start_date, end_date, max_iter=10, interval=0.5):
    global idx
    params = {
        'lat': lat,
        'lon': lon,
        'start': int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()),
        'end': int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
    }

    for _ in range(max_iter):
        params['appid'] = API_KEY_LIST[idx]

        try:
            response = requests.get(url=BASE_URL, params=params, timeout=(5, 10))

            idx = (idx + 1) % len(API_KEY_LIST)
            response.raise_for_status()

            logger.info(f'Đã lấy thành công dữ liệu tại tọa độ {lat}, {lon}')

            return response.json()
        
        except Exception as e:
            logger.error(f'Lỗi khi lấy dữ liệu tại tọa độ {lat}, {lon}: {e}')

        time.sleep(interval)

    logger.warning(f'Không lấy được dữ liệu tại tọa độ {lat}, {lon}')


if __name__ == "__main__":
    lat = 21.0283334
    lon = 105.854041
    start_date = '2020-01-01'
    end_date = '2020-01-02'
    pollution_data = fetch_pollution_data_v2(lat, lon, start_date, end_date)

    print(json.dumps(pollution_data, ensure_ascii=False, indent=2))
