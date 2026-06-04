Các bước cấu hình Kafka Connect sau khi docker build:

B1: cd vào thư mục batch này

B2: mở cmd và run script đăng ký 3 connector:

```bash
CONNECT_URL=http://localhost:8083 ./register_connectors.sh
```

Hoặc đăng ký thủ công từng connector:

```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-weather-realtime/config -H "Content-Type: application/json" -d @config.json
curl -X PUT http://localhost:8083/connectors/s3-sink-weather-forecast/config -H "Content-Type: application/json" -d @config_forecast.json
curl -X PUT http://localhost:8083/connectors/s3-sink-pollution/config -H "Content-Type: application/json" -d @config_pollution.json
```

Khi dùng `docker compose up`, service `kafka_connect_init` sẽ tự chờ Kafka Connect sẵn sàng và đăng ký 3 connector trên.

Lưu ý: 
- bacth_layer đang để restart là: No nên khi docker-compose up -d thì batch chỉ chạy 1 lần r tắt (có thể lỗi nếu minio chưa khởi tạo hoặc minio chưa có bucket hoặc là kafkaconnect chưa chạy) nên nếu nó bị lỗi lần đầu chạy thì k sao. Chạy lại bằng lệnh: 
docker start spark_batch_layer 
