import os
import requests
import json
import logging
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, 'configs', 'crawler_state.json')

def get_used_idx():
    if not os.path.exists(STATE_FILE):
        return 0
    
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data['used_idx']
        
    except:
        return 0
    
def update_used_idx(idx):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    with open(STATE_FILE, 'w', encoding='utf-8') as file:
        json.dump({'used_idx': idx}, file, indent=4)

API_KEY_LIST = list(os.getenv('VISUAL_CROSSING_API_KEY_LIST', '').split(','))
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline'
idx = get_used_idx()

params = {
    'unitGroup': 'metric',
    'contentType': 'json',
    'include': 'current'
}

def fetch_weather_data(location):
    global idx
    url = f'{BASE_URL}/{location}'

    try:
        while True:
            if idx >= len(API_KEY_LIST):
                logging.critical('Đã sử dụng hết key')
                return
            
            params['key'] = API_KEY_LIST[idx]
            response = requests.get(url=url, params=params, timeout=(5, 10))

            if response.status_code in [401, 429]:
                idx += 1
                continue

            update_used_idx(idx)
            response.raise_for_status()
            return response.json()

    except Exception as e:
        logging.error(f'Lỗi khi lấy dữ liệu tại tọa độ {location}: {e}')


if __name__ == "__main__":
    location = '21.0283334,105.854041'
    weather_data = fetch_weather_data(location)

    for key, value in weather_data.items():
        print(f'{key}: {value}')
