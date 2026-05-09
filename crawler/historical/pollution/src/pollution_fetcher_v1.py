import requests
import json
import logging
import time


logger = logging.getLogger(__name__)

BASE_URL = 'https://air-quality-api.open-meteo.com/v1/air-quality'

params = {
    'hourly': 'pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index,uv_index_clear_sky,us_aqi,european_aqi',
    'timezone': 'Asia/Bangkok'
}

def fetch_pollution_data_v1(lat, lon, start_date, end_date, max_iter=10, interval=0.5):
    params['latitude'] = lat
    params['longitude'] = lon
    params['start_date'] = start_date
    params['end_date'] = end_date

    for _ in range(max_iter):
        try:
            response = requests.get(url=BASE_URL, params=params, timeout=(5, 10))

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
    start_date = '2023-01-01'
    end_date = '2023-01-02'
    pollution_data = fetch_pollution_data_v1(lat, lon, start_date, end_date)

    print(json.dumps(pollution_data, ensure_ascii=False, indent=2))
