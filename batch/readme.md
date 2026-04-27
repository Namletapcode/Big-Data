Các bước cấu hình kafka connect sau khi docker build :
B1: cd vào thư mục batch này

B2: mở cmd và run: 
curl -X POST http://localhost:8083/connectors ^
  -H "Content-Type: application/json" ^
  -d "{ \"name\": \"minio-s3-sink-weather\", \"config\": { \"connector.class\": \"io.confluent.connect.s3.S3SinkConnector\", \"tasks.max\": \"1\", \"topics\": \"raw_weather_data\", \"s3.region\": \"us-east-1\", \"s3.bucket.name\": \"raw-weather-data\", \"store.url\": \"http://minio:9000\", \"storage.class\": \"io.confluent.connect.s3.storage.S3Storage\", \"format.class\": \"io.confluent.connect.s3.format.json.JsonFormat\", \"flush.size\": \"20\", \"rotate.schedule.interval.ms\": \"60000\", \"schema.compatibility\": \"NONE\" } }"

B3: run: 
curl -X PUT http://localhost:8083/connectors/minio-s3-sink-weather/config -H "Content-Type: application/json" -d @config.json

Lưu ý: 
- bacth_layer đang để restart là: No nên khi docker-compose up -d thì batch chỉ chạy 1 lần r tắt (có thể lỗi nếu minio chưa khởi tạo hoặc minio chưa có bucket hoặc là kafkaconnect chưa chạy) nên nếu nó bị lỗi lần đầu chạy thì k sao. Chạy lại bằng lệnh: 
docker start spark_batch_layer