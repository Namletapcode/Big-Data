import os
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY_LIST = iter(os.getenv('VISUAL_CROSSING_API_KEY_LIST').split(','))
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline'

params = {
    'unitGroup': 'metric',
    'key': next(API_KEY_LIST),
    'contentType': 'json'
}

def fetch_weather_data(location):
    url = f'{BASE_URL}/{location}'

    try:
        while True:
            response = requests.get(url=url, params=params, timeout=(3, 10))

            if response.status_code in [401, 429]:
                params['key'] = next(API_KEY_LIST)
                continue

            response.raise_for_status()
            return response.json()

    except StopIteration:
        print(f'Đã sử dụng hết key')

    except Exception as e:
        print(f'Lỗi khi lấy dữ liệu {location}: {e}')


if __name__ == "__main__":
    location = 'Hanoi, VN'
    weather_data = fetch_weather_data(location)

    for key, value in weather_data.items():
        print(f'{key}: {value}')
