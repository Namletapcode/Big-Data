Các bước cấu hình kafka connect sau khi docker build :
B1: cd vào thư mục batch này

B2: mở cmd và run: 
curl -X PUT http://localhost:8083/connectors/minio-s3-sink-weather/config -H "Content-Type: application/json" -d @config.json

Lưu ý: 
- bacth_layer đang để restart là: No nên khi docker-compose up -d thì batch chỉ chạy 1 lần r tắt (có thể lỗi nếu minio chưa khởi tạo hoặc minio chưa có bucket hoặc là kafkaconnect chưa chạy) nên nếu nó bị lỗi lần đầu chạy thì k sao. Chạy lại bằng lệnh: 
docker start spark_batch_layer 