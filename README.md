# MLOps Monitoring & Anomaly Detection System

## Tổng quan

Hệ thống hiện tại là một pipeline giám sát metrics hạ tầng theo hướng realtime, gồm các thành phần chính sau:

- thu thập metrics từ các nguồn phát dữ liệu và đưa vào Kafka;
- consumer đọc dữ liệu từ Kafka, chuẩn hóa và lưu vào TimescaleDB;
- service inference chạy các mô hình phát hiện bất thường trên từng metric hoặc nhóm metric;
- Grafana dùng để trực quan hóa dữ liệu và cảnh báo từ cơ sở dữ liệu.

Hệ thống đang tập trung vào hai luồng hoạt động chính:

1. Luồng vận hành realtime
   - nhận metrics từ Kafka;
   - lưu lịch sử vào database;
   - sinh cảnh báo theo quy tắc hoặc bằng mô hình phát hiện bất thường.

2. Luồng inference cho dự đoán bất thường
   - expose API để nhận snapshot metrics từ client;
   - trả về kết quả phân loại bình thường/bất thường cho từng nhóm metric.

## Kiến trúc hiện tại

- Kafka: trung tâm hàng đợi tin nhắn cho dữ liệu metrics realtime.
- TimescaleDB: lưu trữ dữ liệu metrics và alerts.
- Consumer: đọc message từ Kafka, parse payload và ghi vào DB.
- Inference Service: cung cấp API dự đoán bất thường bằng các mô hình đã được đóng gói sẵn.
- Grafana: giao diện dashboard giám sát và quan sát metrics.

## Cấu trúc thư mục chính

- [docker-compose.yml](docker-compose.yml): khởi động Kafka, TimescaleDB và Grafana.
- [db/init.sql](db/init.sql): định nghĩa bảng metrics và alerts.
- [consumer/consumer.py](consumer/consumer.py): consumer đọc Kafka, lưu DB và tạo alert theo ngưỡng.
- [inference/app.py](inference/app.py): FastAPI service cung cấp endpoint health và predict.
- [model/models](model/models): thư mục chứa các file mô hình và scaler dùng cho inference.

## Luồng dữ liệu

1. Metrics được đẩy vào Kafka topic `metrics-raw`.
2. Consumer nhận message, parse thông tin và chuyển thành bản ghi phù hợp.
3. Dữ liệu được lưu vào bảng `metrics` trong TimescaleDB.
4. Nếu giá trị vượt ngưỡng quy tắc, consumer ghi thêm bản ghi vào bảng `alerts`.
5. Service inference nhận payload metrics qua API `/predict` và trả về kết quả bất thường cùng điểm độ bất thường.

## Chạy hệ thống local

### 1. Khởi động hạ tầng

```bash
docker compose up -d
```

Các dịch vụ sẽ chạy ở:

- Kafka: `localhost:9092`
- TimescaleDB: `localhost:5432`
- Grafana: `http://localhost:3000`

### 2. Chạy consumer

```bash
python consumer/consumer.py
```

### 3. Chạy inference service

```bash
uvicorn inference.app:app --reload --port 8000
```

### 4. Kiểm tra trạng thái service

```bash
curl http://localhost:8000/health
```

### 5. Gửi request dự đoán

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "host_id": "intern-metrics-server-1",
    "cpu_usage_active": 5,
    "mem_used_percent": 14,
    "disk_used_percent": 9,
    "net_bytes_recv": 1000000,
    "net_bytes_sent": 800000,
    "net_err_in": 0,
    "net_err_out": 0
  }'
```

## API hiện tại

### GET `/health`
Trả về trạng thái service và các mô hình đã được load hay chưa.

### POST `/predict`
Nhận một snapshot metrics của một host, chạy inference cho các nhóm:

- CPU
- Memory
- Disk
- Network

Trả về:

- `results`: kết quả phát hiện bất thường cho từng nhóm
- `anomalous_count`: số nhóm bị đánh dấu bất thường
- `severity`: mức độ nguy hiểm `normal`, `warning` hoặc `critical`

## Ghi chú về trạng thái hiện tại

- Consumer hiện tại vẫn dùng quy tắc ngưỡng cứng để tạo alert, phù hợp cho kiểm thử luồng realtime đầu tiên.
- Inference service hiện dùng mô hình đã được huấn luyện sẵn và lưu trong thư mục [model/models](model/models).
- Hệ thống đang ở trạng thái tích hợp cơ bản giữa streaming data, lưu trữ và dự đoán bất thường, chưa đi tới toàn bộ vòng đời MLOps tự động hoàn chỉnh.
