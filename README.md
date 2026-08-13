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

# Mlflow, Labeling

## Install Python pip manager

Trình thông dịch python có sẵn → cài pip
```
sudo apt install python3-pip -y
pip3 install psycopg2-binary --break-system-packages
```
Cài stress next generation
```
apt install stress-ng -y
```
## sql to define table that contains labeled data
đăng nhập postgresql qua giao diện dòng lệnh (trên server) (`-U mlops` - chỉ định tài khoản đăng nhập) (`-d metrics_db` - chỉ định cơ sở dữ liệu cần kết nối):
```
docker exec -it timescaledb psql -U mlops -d metrics_db
```

trong psql shell nhập (`\dt` to confirm and `\q` to quit): 
```
CREATE TABLE labeled_events (
    id BIGSERIAL PRIMARY KEY,
    host_id TEXT NOT NULL,
    metric_group TEXT NOT NULL,
    label TEXT NOT NULL,
    scenario TEXT,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    notes TEXT
);
```
psql shell báo:
```
metrics_db=# \dt
            List of relations
 Schema |      Name      | Type  | Owner
--------+----------------+-------+-------
 public | alerts         | table | mlops
 public | labeled_events | table | mlops
 public | metrics        | table | mlops
(3 rows)
```
## tạo file script trên server
tạo thư mục:
```
mkdir -p ~/mlops-kafka/scripts
vim ~/mlops-kafka/scripts/collect_labeled_data.py
```
nhập `collect_labeled_data.py`:
```
# scripts/collect_labeled_data.py
import subprocess
import time
import psycopg2
from psycopg2.extensions import cursor
from datetime import datetime, timezone

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "metrics_db", "user": "mlops", "password": "mlops123",
}
HOST_ID = "intern-metrics-server-1"

def log_event(
        cur: cursor,
        metric_group: str,
        label: str,
        scenario: str,
        start_ts: datetime,
        end_ts: datetime,
        notes=""
    ):
    """
    Ghi lại dữ liệu vào database

    Args:
        cur: cursor object
        metric_group: `cpu`, `disk`, `mem`, `net`
        label: `normal, anomaly`
        scenario:
            - `stress-cpu-100pct`
            - `stress-mem-fill`
            - `stress-disk-io`
            - `network-flood
        start_ts: well duh it's start time
        end_ts: well duh it's end time
        notes
    Returns:


    """
    cur.execute(
        """INSERT INTO labeled_events (host_id, metric_group, label, scenario, start_ts, end_ts, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (HOST_ID, metric_group, label, scenario, start_ts, end_ts, notes),
    )

def run_scenario(cur: cursor, metric_group: str, scenario_name: str, cmd: str, duration_sec, cooldown_sec=30):
    """
    Chạy các kịch bản stress test

    Args:
        cur: đối tượng cursor của Psycopg2
        metric_group: `cpu`, `disk`, `mem`, `net`
        scenario_name:
            - `stress-cpu-100pct`: sử dụng stress next generation ép cpu lên 100%
            - `stress-mem-fill`: chiếm 80% ram
            - `stress-disk-io`: tạo 2 tiến trình, mỗi cái liên tục đọc-ghi trên một file tạm dung lượng 1gb
            - `network-flood`: tạo 4 tiến trình, 2 chủ 2 khách, mở kết nối socket, gửi dữ liệu qua lại với tốc độ lớn. repeat this process many times trong 60s
        cmd:
            - `stress-ng --cpu 4 --cpu-load 100 --timeout 60s`
            - `stress-ng --vm 2 --vm-bytes 80% --timeout 60s`
            - `stress-ng --hdd 2 --hdd-bytes 1G --timeout 60s`
            - `stress-ng --sock 4 --timeout 60s`
        duration_sec: duration time in second
        cooldown_sec: cooldown time in second
            
    Returns:

    """
    print(f"\n=== Scenario: {scenario_name} ===")
    # Ghi 1 đoạn "normal" trước khi bắt đầu (baseline ngay trước anomaly)
    normal_start = datetime.now(timezone.utc)
    time.sleep(cooldown_sec)
    normal_end = datetime.now(timezone.utc)
    log_event(cur, metric_group, "normal", scenario_name, normal_start, normal_end, "baseline before anomaly")

    # Chạy anomaly
    anomaly_start = datetime.now(timezone.utc)
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, timeout=duration_sec + 10)
    anomaly_end = datetime.now(timezone.utc)
    log_event(cur, metric_group, "anomaly", scenario_name, anomaly_start, anomaly_end)

    # Cooldown sau anomaly (để hệ thống về bình thường trước kịch bản tiếp theo)
    time.sleep(cooldown_sec)

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    scenarios = [
        ("cpu", "stress-cpu-100pct", "stress-ng --cpu 4 --cpu-load 100 --timeout 60s", 60),
        ("mem", "stress-mem-fill", "stress-ng --vm 2 --vm-bytes 80% --timeout 60s", 60),
        ("disk", "stress-disk-io", "stress-ng --hdd 2 --hdd-bytes 1G --timeout 60s", 60),
        ("net", "network-flood", "stress-ng --sock 4 --timeout 60s", 60),  # hoặc dùng iperf3 nếu có
    ]

    for metric_group, scenario_name, cmd, duration in scenarios:
        run_scenario(cur, metric_group, scenario_name, cmd, duration)
        conn.commit()

    cur.close()
    conn.close()
    print("\nDone collecting labeled events.")

if __name__ == "__main__":
    main()
```

chạy script:
```
python3 scripts/collect_labeled_data.py
```

## verify data được ghi

```
docker exec -it timescaledb psql -U mlops -d metrics_db
```
chạy lệnh:
```
SELECT * FROM labeled_events ORDER BY start_ts;
```
## check whether metrics match labeled events' time
```
docker exec -it timescaledb psql -U mlops -d metrics_db
```
chạy lệnh:
```
SELECT le.scenario, le.label, le.start_ts, le.end_ts, count(m.*) as n_metric_points
FROM labeled_events le
LEFT JOIN metrics m ON m.host_id = le.host_id
  AND m.metric_name = 'cpu_usage_active'
  AND m.ts BETWEEN le.start_ts AND le.end_ts
GROUP BY le.id, le.scenario, le.label, le.start_ts, le.end_ts
ORDER BY le.start_ts;
```