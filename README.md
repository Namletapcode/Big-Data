# 🌦️ Big Data Weather Pipeline - Lambda Architecture

Dự án thu thập, xử lý và trực quan hóa dữ liệu thời tiết cho 34 tỉnh/thành phố tại Việt Nam sử dụng **Kiến trúc Lambda** (Lambda Architecture).

## 📊 Kiến trúc Hệ thống

Hệ thống được chia thành 4 lớp (layers) chính, chạy hoàn toàn trên Docker:

1. **Data Ingestion (Thu thập)**
   - **Weather Crawler**: Lấy dữ liệu quan trắc/dự đoán từ Visual Crossing  → Đẩy lên Kafka.
   - **Pollution Crawler**: Tổng hợp dữ liệu từ Open Meteo và Open Weather Map → Đẩy lên Kafka.
2. **Speed Layer (Xử lý Real-time)**
   - **Spark Streaming**: Đọc dữ liệu từ Kafka (`weather_data`), parse JSON và ghi trực tiếp vào Elasticsearch (Index: `weather_realtime`). Dữ liệu xuất hiện ngay lập tức trên Dashboard.
3. **Batch Layer (Xử lý Lô)**
   - **Kafka Connect**: Sink dữ liệu liên tục từ Kafka xuống MinIO (S3 Data Lake).
   - **Spark Batch**: Định kỳ đọc dữ liệu từ MinIO, tổng hợp (Aggregation) theo ngày, phát hiện hiện tượng thời tiết cực đoan (nắng nóng kéo dài, lạnh nhất, nóng nhất) → Ghi vào Elasticsearch (Index: `weather_batch_daily` & `weather_batch_stats`).
4. **Serving Layer (Trực quan hóa)**
   - **FastAPI**: Cung cấp các REST API lấy dữ liệu từ Elasticsearch.
   - **Dashboard (HTML/JS + Chart.js)**: Giao diện theo dõi thời tiết realtime, hiển thị biểu đồ và thống kê batch cực đoan.

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy

### 1. Cấu hình biến môi trường
Trong thư mục `crawler`, tạo/chỉnh sửa file `.env`. Điền các API Key cần thiết (đặc biệt là Visual Crossing để lấy dữ liệu thời tiết và Open Weather Map để lấy dữ liệu ô nhiễm):

```env
# crawler/.env

# Đăng ký tại: https://www.visualcrossing.com/
VISUAL_CROSSING_API_KEY_LIST=key1,key2,key3...

# Đăng ký tại: https://openweathermap.org/
OPEN_WEATHER_API_KEY_LIST=key1,key2,key3,...

# Đăng ký tại: https://countrystatecity.in/
COUNTRY_STATE_CITY_API_KEY=key
```
*Lưu ý: Weather crawler và Pollution crawler hỗ trợ cơ chế xoay vòng API key. Bạn có thể cung cấp nhiều key cùng lúc (tối thiểu 5 key đối với Open Weather Map), ngăn cách bởi dấu phẩy (không chứa khoảng trắng) để tránh hết quota.*

### 2. Khởi động các Services bằng Docker Compose
Di chuyển vào thư mục `docker_deployment` và build các container:

```bash
cd docker_deployment
docker compose up -d --build
```

Lệnh này sẽ khởi động toàn bộ **12 containers** bao gồm: Zookeeper, Kafka, MinIO, Elasticsearch, Kibana, Crawlers, Spark Speed, Spark Batch, Kafka Connect và Serving API.

### 3. Đăng ký Kafka Connect S3 Sink
Sau khi các container báo `Healthy` (đặc biệt là `kafka` và `kafka_connect`), bạn cần đăng ký connector để dữ liệu được tự động chảy từ Kafka xuống MinIO.

Chạy lệnh sau (vẫn đang đứng ở thư mục `docker_deployment`):

```bash
curl -X PUT http://localhost:8083/connectors/minio-s3-sink-weather/config \
  -H "Content-Type: application/json" \
  -d @../batch/config.json
```

Kiểm tra trạng thái connector (Phải trả về `RUNNING`):
```bash
curl -s http://localhost:8083/connectors/minio-s3-sink-weather/status
```

### 4. Xử lý Batch (Tùy chọn thủ công)
Container `spark_batch_layer` được cấu hình tự động chạy 1 lần khi docker-compose up. Nếu lúc đó MinIO chưa có data, batch sẽ rỗng. 
Khi MinIO đã thu thập đủ dữ liệu và bạn muốn cập nhật bảng thống kê Batch mới nhất, hãy kích hoạt lại container này:

```bash
docker start spark_batch_layer
```

---

## 🌐 Các Cổng Dịch Vụ (Ports)

Sau khi hệ thống chạy, bạn có thể truy cập các thành phần qua trình duyệt:

| Tên Dịch Vụ | URL Truy Cập | Mục Đích |
| :--- | :--- | :--- |
| **Web Dashboard** | [http://localhost:8000](http://localhost:8000) | Xem biểu đồ, dữ liệu thời tiết và thống kê |
| **FastAPI Swagger** | [http://localhost:8000/docs](http://localhost:8000/docs) | Xem tài liệu cấu trúc các REST API |
| **Kibana (ES)** | [http://localhost:5601](http://localhost:5601) | Khám phá dữ liệu thô trong Elasticsearch |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Xem Data Lake S3 (`admin` / `password123`) |

---

## 🛠️ Khắc phục sự cố (Troubleshooting)

- **Crawlers không lấy được dữ liệu:** Xem log của container weather_crawler và pollution_crawler để biết chi tiết (thường là do API key hết hạn, sai format, hoặc lỗi mạng):
  `docker logs weather_crawler`, `docker logs pollution_crawler`
- **Dashboard không có biểu đồ:** Dữ liệu batch chưa được tính toán. Hãy đảm bảo MinIO đã có file JSON (`http://localhost:9001` > raw-weather-data) và chạy lại lệnh `docker start spark_batch_layer`.
- **Dữ liệu không chảy vào MinIO:** Do Kafka Connect chưa được gửi request kích hoạt `config.json` hoặc config sai endpoint. Cần đảm bảo `store.url` trong `batch/config.json` là `http://minio:9000` khi chạy trên máy cá nhân.
