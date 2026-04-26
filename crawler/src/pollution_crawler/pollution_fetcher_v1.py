import requests
import json
import logging


logger = logging.getLogger(__name__)

BASE_URL = 'https://air-quality-api.open-meteo.com/v1/air-quality'

params = {
    'current': 'pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index,uv_index_clear_sky,us_aqi,european_aqi',
    'timezone': 'Asia/Bangkok'
}

def fetch_pollution_data_v1(lat, lon):
    params['latitude'] = lat
    params['longitude'] = lon

    try:
        response = requests.get(url=BASE_URL, params=params, timeout=(5, 10))

        response.raise_for_status()
        return response.json()

    except Exception as e:
        logger.error(f'Lỗi khi lấy dữ liệu tại tọa độ {lat}, {lon}: {e}')


if __name__ == "__main__":
    lat = 21.0283334
    lon = 105.854041
    pollution_data = fetch_pollution_data_v1(lat, lon)

    print(json.dumps(pollution_data, ensure_ascii=False, indent=2))
