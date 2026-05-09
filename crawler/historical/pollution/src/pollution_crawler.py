import os
import json
import logging
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType, IntegerType
from pollution_fetcher_v1 import fetch_pollution_data_v1
from pollution_fetcher_v2 import fetch_pollution_data_v2


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger('pollution_crawler')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'src', 'configs', 'pollution_crawler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

LOCATIONS_34_FILE = os.path.join(BASE_DIR, 'src', 'configs', 'locations_34.json')
LOCATIONS_63_FILE = os.path.join(BASE_DIR, 'src', 'configs', 'locations_63.json')

spark = SparkSession.builder.appName('Historical Pollution Crawler').getOrCreate()

pollution_schema = StructType([
    StructField("location", StringType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("time", StringType(), nullable=False),
    StructField("units", MapType(StringType(), StringType()), nullable=True),
    StructField("pm10", DoubleType(), nullable=True),
    StructField("pm2_5", DoubleType(), nullable=True),
    StructField("carbon_monoxide", DoubleType(), nullable=True),
    StructField("nitrogen_monoxide", DoubleType(), nullable=True),
    StructField("nitrogen_dioxide", DoubleType(), nullable=True),
    StructField("sulphur_dioxide", DoubleType(), nullable=True),
    StructField("ozone", DoubleType(), nullable=True),
    StructField("amoniac", DoubleType(), nullable=True),
    StructField("aerosol_optical_depth", DoubleType(), nullable=True),
    StructField("dust", DoubleType(), nullable=True),
    StructField("uv_index", DoubleType(), nullable=True),
    StructField("uv_index_clear_sky", DoubleType(), nullable=True),
    StructField("us_aqi", IntegerType(), nullable=True),
    StructField("european_aqi", IntegerType(), nullable=True)
])

with open(LOCATIONS_63_FILE, 'r', encoding='utf-8') as file:
    locations = json.load(file)

def mean(a, b):
    if a is None and b is None:
        return
    
    if a is None:
        return float(b)
    
    if b is None:
        return float(a)
    
    return round((a + b) / 2, 2)

def to_float(x):
    if x is None:
        return None
    
    return float(x)

def get(data, idx):
    if not data or idx >= len(data):
        return None
    
    return data[idx]

def run_crawler():
    global locations

    for year in range(2025, 2027):
        for month in range(1, 13):
            logger.info(f'Đang lấy dữ liệu của tháng {month} năm {year}')

            if year == 2025 and month == 7:
                with open(LOCATIONS_34_FILE, 'r', encoding='utf-8') as file:
                    locations = json.load(file)

            if year == 2026 and month == 5:
                return

            data_of_month = []

            for location in locations:
                loc_name = f"{location['name']}, {location['country_code']}"
                
                lat = location['latitude']
                lon = location['longitude']

                start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
                end_date = (datetime(year, month + 1, 1) - timedelta(days=1) if month < 12 else datetime(year, month, 31)).strftime('%Y-%m-%d')

                logger.info(f'Đang lấy dữ liệu cho: {loc_name}')

                data_v1 = fetch_pollution_data_v1(lat, lon, start_date, end_date)
                data_v2 = fetch_pollution_data_v2(lat, lon, start_date, end_date)

                hourly_v1 = {}
                units_v1 = {}
                list_v2 = []

                if not data_v1 or 'hourly' not in data_v1:
                    logger.warning(f'Không có dữ liệu {loc_name} từ Open Meteo')

                else:
                    hourly_v1 = data_v1['hourly']
                    units_v1 = data_v1.get('hourly_units', {})

                if not data_v2 or 'list' not in data_v2 or len(data_v2['list']) == 0:
                    logger.warning(f'Không có dữ liệu {loc_name} từ Open Weather')

                else:
                    list_v2 = data_v2['list']

                if not hourly_v1 and not list_v2:
                    logger.error(f'Không có dữ liệu {loc_name}')
                    continue
                
                for hour in range(0, max(len(hourly_v1.get('time')) if hourly_v1.get('time') else 0, len(list_v2)), 6):
                    record_time = datetime(year, month, int(hour / 24) + 1, hour % 24).strftime('%Y-%m-%dT%H:00:00')
                    main_v2 = get(list_v2, hour)
                    components_v2 = {}

                    if main_v2:
                        components_v2 = main_v2.get('components')

                    pollution_data = {
                        'location': loc_name,
                        'latitude': lat,
                        'longitude': lon,
                        'time': record_time,
                        'units': units_v1,
                        'pm10': mean(get(hourly_v1.get('pm10'), hour), components_v2.get('pm10')),
                        'pm2_5': mean(get(hourly_v1.get('pm2_5'), hour), components_v2.get('pm2_5')),
                        'carbon_monoxide': mean(get(hourly_v1.get('carbon_monoxide'), hour), components_v2.get('co')),
                        'nitrogen_monoxide': to_float(components_v2.get('no')),
                        'nitrogen_dioxide': mean(get(hourly_v1.get('nitrogen_dioxide'), hour), components_v2.get('no2')),
                        'sulphur_dioxide': mean(get(hourly_v1.get('sulphur_dioxide'), hour), components_v2.get('so2')),
                        'ozone': mean(get(hourly_v1.get('ozone'), hour), components_v2.get('o3')),
                        'amoniac': to_float(components_v2.get('nh3')),
                        'aerosol_optical_depth': to_float(get(hourly_v1.get('aerosol_optical_depth'), hour)),
                        'dust': to_float(get(hourly_v1.get('dust'), hour)),
                        'uv_index': to_float(get(hourly_v1.get('uv_index'), hour)),
                        'uv_index_clear_sky': to_float(get(hourly_v1.get('uv_index_clear_sky'), hour)),
                        'us_aqi': get(hourly_v1.get('us_aqi'), hour),
                        'european_aqi': get(hourly_v1.get('european_aqi'), hour)
                    }

                    data_of_month.append(pollution_data)

                time.sleep(0.2)

            if data_of_month:
                df = spark.createDataFrame(data_of_month, schema=pollution_schema)
                df.write.parquet(os.path.join(BASE_DIR, 'data', str(year), f'{year}-{month:02d}'), mode='overwrite')

                logger.info(f'Đã lưu thành công dữ liệu của tháng {month} năm {year}')

            else:
                logger.warning(f'Đã bỏ qua tháng {month} năm {year} vì không có dữ liệu')


if __name__ == "__main__":
    logger.info('Khởi động Historical Pollution Crawler')

    try:
        run_crawler()
    
    except Exception as e:
        logger.error(f'Lỗi: {e}')

    finally:
        spark.stop()
        logger.info('Đã tắt Historical Pollution Crawler')
