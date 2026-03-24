import os
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv('VISUAL_CROSSING_API_KEY')
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline'

def fetch_weather_data(location):
    url = f'{BASE_URL}/{location}'
    params = {
        'unitGroup': 'metric',
        'key': API_KEY,
        'contentType': 'json'
    }

    try:
        response = requests.get(url=url, params=params, timeout=(3, 10))
        response.raise_for_status()

        return response.json()
    
    except Exception as e:
        print(f'Lỗi khi lấy dữ liệu: {e}')


if __name__ == "__main__":
    location = 'Hanoi, VN'
    weather_data = fetch_weather_data(location)

    for key, value in weather_data.items():
        print(f'{key}: {value}')
