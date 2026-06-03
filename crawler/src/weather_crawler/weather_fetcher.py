import os
import sys
import requests
import json
import logging
import time
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(BASE_DIR, 'state', 'weather_crawler_state.json')
sys.path.append(BASE_DIR)

from utils import get_state, update_state


load_dotenv()

logger = logging.getLogger(__name__)

API_KEY_LIST = list(os.getenv('VISUAL_CROSSING_API_KEY_LIST', '').split(','))
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline'
idx = get_state(STATE_FILE, 'key_idx')

params = {
    'unitGroup': 'metric',
    'contentType': 'json',
}

def fetch_weather_data(lat, lon, type, max_iter=10, interval=0.5):
    global idx

    url_suffix = '/today' if type == 'current' else ''
    url = f'{BASE_URL}/{lat},{lon}{url_suffix}'

    if type == 'fcst':
        params['include'] = 'fcst,hours'
    else:
        params['include'] = type

    for _ in range(max_iter):
        try:
            while True:
                if idx >= len(API_KEY_LIST):
                    logger.critical('Đã sử dụng hết key')
                    return
                
                params['key'] = API_KEY_LIST[idx]
                response = requests.get(url=url, params=params, timeout=(5, 10))

                if response.status_code in [401, 429]:
                    idx += 1
                    logger.warning(f'Key thứ {idx} đã hết quota')
                    continue

                update_state(STATE_FILE, 'key_idx', idx)
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

    print('---DỮ LIỆU HIỆN TẠI---')
    weather_data = fetch_weather_data(lat, lon, 'current')
    print(json.dumps(weather_data, ensure_ascii=False, indent=2))

    print()

    print('---DỮ LIỆU DỰ ĐOÁN---')
    weather_data = fetch_weather_data(lat, lon, 'fcst')
    print(json.dumps(weather_data, ensure_ascii=False, indent=2))
