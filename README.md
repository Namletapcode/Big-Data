# 🌦️ Weather Forecase - Lambda Architecture

Dự án thu thập, xử lý và trực quan hóa dữ liệu thời tiết & môi trường cho các tỉnh/thành phố tại Việt Nam. Hệ thống được thiết kế theo **Kiến trúc Lambda** kết hợp cơ chế tự động hóa quy trình, triển khai hoàn toàn trên nền tảng Docker.

## 📊 Kiến trúc Hệ thống

Hệ thống được chia thành 5 thành phần lõi, vận hành liền mạch qua mạng nội bộ `bigdata-net`:

1. **Orchestration (Điều phối tự động)**
   - **Apache Airflow**: Bộ não điều phối trung tâm. Quản lý lịch trình chạy của các luồng thu thập dữ liệu (DAGs) thông qua `DockerOperator`, đồng thời quản trị trạng thái an toàn qua shared volume.
2. **Data Ingestion (Thu thập dữ liệu)**
   - **Weather & Pollution Crawler**: Các tác vụ được Airflow kích hoạt định kỳ. Thu thập dữ liệu từ Visual Crossing, Open Meteo, Open Weather Map và đẩy trực tiếp các bản ghi vào Kafka topic.
3. **Speed Layer (Xử lý thời gian thực)**
   - **Spark Streaming**: Tiêu thụ dữ liệu liên tục từ Kafka, bóc tách JSON và ghi trực tiếp vào Elasticsearch (Index: `weather_realtime`). Cung cấp độ trễ siêu thấp cho Dashboard.
4. **Batch Layer (Xử lý lô định kỳ)**
   - **Kafka Connect (S3 Sink)**: Lắng nghe Kafka và đồng bộ hóa dữ liệu thô liên tục xuống kho lưu trữ MinIO (Data Lake).
   - **Spark Batch**: Chạy định kỳ để đọc dữ liệu từ MinIO, thực hiện tính toán tổng hợp (Aggregation) theo ngày và phát hiện các hiện tượng cực đoan, sau đó ghi kết quả vào Elasticsearch (Index: `weather_batch_daily` & `weather_batch_stats`).
5. **Serving Layer (Truy xuất & Trực quan hóa)**
   - **FastAPI**: Lớp giao tiếp (REST API) truy vấn dữ liệu từ Elasticsearch.
   - **Web Dashboard**: Giao diện theo dõi thời tiết realtime và hiển thị biểu đồ thống kê các kỷ lục thời tiết.

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy

### 1. Cấu hình biến môi trường (.env)
Hệ thống sử dụng các khóa API để thu thập dữ liệu. Trong thư mục `crawler`, bạn cần tạo/chỉnh sửa file `.env` và điền các API Key tương ứng:

```env
# crawler/.env

# Đăng ký tại: [https://www.visualcrossing.com/](https://www.visualcrossing.com/)
VISUAL_CROSSING_API_KEY_LIST=key1,key2,key3...

# Đăng ký tại: [https://openweathermap.org/](https://openweathermap.org/)
OPEN_WEATHER_API_KEY_LIST=key1,key2,key3,...

# Đăng ký tại: [https://countrystatecity.in/](https://countrystatecity.in/)
COUNTRY_STATE_CITY_API_KEY=key
```

> **Lưu ý:** Crawler hỗ trợ cơ chế luân phiên khóa (API Key Rotation). Bạn có thể cung cấp nhiều key cùng lúc (ngăn cách bởi dấu phẩy, không khoảng trắng) để tận dụng tối đa Quota miễn phí.

### 2. Khởi động Cụm hệ thống (Docker Compose)
Di chuyển vào thư mục `docker_deployment` và chạy tập lệnh khởi tạo tương ứng với hệ điều hành của bạn:

**🔹 Đối với Windows:**
```cmd
cd docker_deployment
deploy.bat
```

**🔹 Đối với macOS / Linux:**
```bash
cd docker_deployment
chmod +x deploy.sh  # Cấp quyền thực thi (chỉ cần chạy lần đầu)
./deploy.sh
```

### 3. Kích hoạt Kafka Connect (S3 Sink)
Sau khi các dịch vụ đạt trạng thái `Healthy` (kiểm tra bằng `docker ps`), tiến hành đăng ký Connector để tự động đẩy dữ liệu từ Kafka xuống MinIO Data Lake:

```bash
curl -X PUT http://localhost:8083/connectors/minio-s3-sink-weather/config \
  -H "Content-Type: application/json" \
  -d @../batch/config.json
```

Kiểm tra trạng thái connector (Dữ liệu trả về trạng thái `RUNNING` là thành công):
```bash
curl -s http://localhost:8083/connectors/minio-s3-sink-weather/status
```

### 4. Vận hành Spark Batch (Cập nhật thủ công)
Container `spark_batch_layer` được cấu hình để chạy một lần rồi tắt (`restart: no`). Để tổng hợp dữ liệu lịch sử mới nhất từ MinIO, bạn chỉ cần gọi lệnh tái khởi động container này:

```bash
docker start spark_batch_layer
```

---

## 🌐 Danh sách Cổng Dịch Vụ (Ports)

Sau khi hệ thống khởi động thành công, toàn bộ các bảng điều khiển quản trị đều được bộc lộ (expose) để tiện theo dõi:

| Công cụ | URL Truy cập cục bộ | Chức năng chính |
| :--- | :--- | :--- |
| Web Dashboard | http://localhost:8000 | Giao diện hiển thị biểu đồ & dữ liệu người dùng |
| Airflow UI | http://localhost:8080 | Quản lý, theo dõi lịch sử chạy và kích hoạt DAGs |
| Kibana | http://localhost:5601 | Quản trị Elasticsearch, khám phá dữ liệu thô (Dev Tools) |
| MinIO Console | http://localhost:9001 | Quản trị S3 Buckets, xem file JSON đã lưu trữ |
