from api_client import fetch_weather_data
from kafka_producer import send_to_kafka


KAFKA_TOPIC = 'raw_weather_data'
LOCATIONS = ['Hanoi, VN', 'HoChiMinh, VN']

def run_crawler():
    for location in LOCATIONS:
        print(f'Đang lấy dữ liệu cho: {location}...')
        weather_data = fetch_weather_data(location)
        
        if weather_data:
            send_to_kafka(KAFKA_TOPIC, weather_data)

        else:
            return False
        
    return True

if __name__ == "__main__":
    print('Khởi động Weather Crawler...')
    run_crawler()
