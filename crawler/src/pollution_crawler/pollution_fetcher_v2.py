import os
import requests
import json
import logging
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv('OPEN_WEATHER_API_KEY')
BASE_URL = 'http://api.openweathermap.org/data/2.5/air_pollution'

params = {
    'appid': API_KEY
}

def fetch_pollution_data_v2(lat, lon):
    params['lat'] = lat
    params['lon'] = lon

    try:
        response = requests.get(url=BASE_URL, params=params, timeout=(5, 10))

        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        logger.error(f'Lỗi khi lấy dữ liệu tại tọa độ {lat}, {lon}: {e}')


if __name__ == "__main__":
    lat = 21.0283334
    lon = 105.854041
    pollution_data = fetch_pollution_data_v2(lat, lon)

    print(json.dumps(pollution_data, ensure_ascii=False, indent=2))
