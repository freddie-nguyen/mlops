# consumer/consumer.py
import json
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "123.30.48.173:9092"
TOPIC = "metrics-raw"
GROUP_ID = "metrics-consumer-group"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "mlops",
    "password": "mlops123",
}

# rule ngưỡng cứng - sẽ thay bằng model ở giai đoạn sau
THRESHOLDS = {
    "usage_active": 90,     # cpu
    "used_percent": 90,     # mem, disk
}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_rule(field_name, value):
    threshold = THRESHOLDS.get(field_name)
    if threshold is not None and value > threshold:
        return True, f"{field_name}={value} vượt ngưỡng {threshold}"
    return False, None

# Chỉ giữ đúng những gì cần, mọi thứ khác bị lọc ngay khi parse
ALLOWED_METRICS = {
    "cpu": {"usage_idle"},              # sẽ tự tính usage_active = 100 - usage_idle
    "mem": {"used_percent"},
    "disk": {"used_percent"},
    "net": {"bytes_recv", "bytes_sent", "err_in", "err_out"},
}

def parse_telegraf_message(raw_value):
    data = json.loads(raw_value)
    metric_group = data.get("name", "unknown")

    # Lọc ngay từ đầu: bỏ qua toàn bộ group không nằm trong danh sách cần
    if metric_group not in ALLOWED_METRICS:
        return []

    host_id = data.get("tags", {}).get("host", "unknown-host")
    ts_unix = data.get("timestamp")
    ts = datetime.fromtimestamp(ts_unix, tz=timezone.utc) if ts_unix else datetime.now(timezone.utc)

    rows = []
    allowed_fields = ALLOWED_METRICS[metric_group]
    for field_name, value in data.get("fields", {}).items():
        if field_name not in allowed_fields:
            continue
        if not isinstance(value, (int, float)):
            continue

        # đặc biệt: đổi usage_idle thành usage_active cho dễ hiểu (CPU đang bận, không phải rảnh)
        if metric_group == "cpu" and field_name == "usage_idle":
            field_name = "usage_active"
            value = 100 - value

        metric_name = f"{metric_group}_{field_name}"
        rows.append({
            "ts": ts,
            "host_id": host_id,
            "metric_name": metric_name,
            "value": float(value),
        })
    return rows

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    conn = get_db_conn()
    cur = conn.cursor()

    print("Consumer started, listening...")
    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                rows = parse_telegraf_message(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Skip malformed message: {e}")
                continue

            for row in rows:
                # 1. lưu vào bảng metrics
                cur.execute(
                    "INSERT INTO metrics (ts, host_id, metric_name, value) VALUES (%s, %s, %s, %s)",
                    (row["ts"], row["host_id"], row["metric_name"], row["value"]),
                )

                # 2. check rule cứng -> alert
                is_alert, reason = check_rule(row["metric_name"], row["value"])
                if is_alert:
                    cur.execute(
                        """INSERT INTO alerts (ts, host_id, metric_name, value, reason, source)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (row["ts"], row["host_id"], row["metric_name"],
                         row["value"], reason, "rule"),
                    )
                    print(f"[ALERT] {reason} on {row['host_id']}")

            conn.commit()
            count += 1
            if count % 100 == 0:
                print(f"Consumed {count} kafka messages")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()