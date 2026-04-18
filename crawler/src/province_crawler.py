import os
import requests
import json
import time
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

API_KEY = os.getenv('COUNTRY_STATE_CITY_API_KEY', '')
BASE_URL = 'https://api.countrystatecity.in/v1/countries'

headers = {
    'X-CSCAPI-KEY': API_KEY
}

def fetch_province_data(country_codes):
    locations_data = []
    
    for code in country_codes:
        response = None

        try:
            url = f'{BASE_URL}/{code}/states'

            response = requests.get(url=url, headers=headers)
            response.raise_for_status()

            provinces = response.json()

            for province in provinces:
                locations_data.append({
                    'country_code': code,
                    'name': province['name'],
                    'latitude': province['latitude'],
                    'longitude': province['longitude']
                })
                    
        except Exception as e:
            print(f'Lỗi khi lấy dữ liệu {code}: {e}')

        time.sleep(1)

    os.makedirs(os.path.dirname(LOCATIONS_FILE), exist_ok=True)

    with open(LOCATIONS_FILE, 'w', encoding='utf-8') as file:
        json.dump(locations_data, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    country_code = ['VN']

    print('Khởi động Province Crawler...')
    fetch_province_data(country_code)
    print('Đã tắt Province Crawler')
