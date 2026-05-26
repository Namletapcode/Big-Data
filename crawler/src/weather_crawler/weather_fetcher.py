import os
import sys
import requests
import json
import logging
import time
from datetime import date, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(BASE_DIR, 'configs', 'weather_crawler_state.json')
sys.path.append(BASE_DIR)

from utils import get_state, update_state


load_dotenv()

logger = logging.getLogger(__name__)

API_KEY_LIST = [
    key.strip()
    for key in os.getenv('VISUAL_CROSSING_API_KEY_LIST', '').split(',')
    if key.strip()
]
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline'
idx = get_state(STATE_FILE, 'key_idx')

params = {
    'unitGroup': 'metric',
    'contentType': 'json',
    'include': 'current,days,hours'
}

def reset_key_cycle():
    """Allow keys rejected in a previous crawl cycle to be retried later."""
    global idx
    idx = 0
    update_state(STATE_FILE, 'key_idx', idx)


def fetch_weather_data(lat, lon, max_iter=10, interval=0.5):
    global idx
    if not API_KEY_LIST:
        logger.critical('Thiếu VISUAL_CROSSING_API_KEY_LIST trong biến môi trường')
        return

    start_date = date.today()
    end_date = start_date + timedelta(days=14)
    url = f'{BASE_URL}/{lat},{lon}/{start_date.isoformat()}/{end_date.isoformat()}'

    for _ in range(max_iter):
        try:
            while True:
                if idx >= len(API_KEY_LIST):
                    logger.critical('Không còn API key khả dụng trong chu kỳ thu thập hiện tại')
                    return
                
                params['key'] = API_KEY_LIST[idx]
                response = requests.get(url=url, params=params, timeout=(5, 10))

                if response.status_code in [401, 429]:
                    logger.warning(
                        'API key thứ %s bị từ chối (HTTP %s), chuyển sang key tiếp theo',
                        idx + 1,
                        response.status_code,
                    )
                    idx += 1
                    update_state(STATE_FILE, 'key_idx', idx)
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
    weather_data = fetch_weather_data(lat, lon)

    print(json.dumps(weather_data, ensure_ascii=False, indent=2))
