import os
import json
from weather_fetcher import fetch_weather_data
from kafka_producer import send_to_kafka


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_FILE = os.path.join(BASE_DIR, 'configs', 'locations.json')

KAFKA_TOPIC = 'raw_weather_data'

with open(LOCATIONS_FILE, 'r', encoding='utf-8') as file:
    data = json.load(file)

LOCATIONS = [f'{item['latitude']},{item['longitude']}' for item in data]

def run_crawler():
    for location in LOCATIONS:
        print(f'Đang lấy dữ liệu cho: {location}...')
        weather_data = fetch_weather_data(location)
        
        if weather_data:
            # Gửi vào Kafka (Cho Speed Layer)
            send_to_kafka(KAFKA_TOPIC, weather_data)
            
            # Ghi vào Data Lake / Master Dataset (Cho Batch Layer)
            lake_dir = '/data_lake/realtime'
            os.makedirs(lake_dir, exist_ok=True)
            file_path = os.path.join(lake_dir, f'raw_{location.replace(",", "_")}.jsonl')
            try:
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(weather_data, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f"Lỗi ghi Data Lake: {e}")
        else:
            continue
        
    return True

if __name__ == "__main__":
    print('Khởi động Weather Crawler...')
    run_crawler()
    print('Đã tắt Weather Crawler')
