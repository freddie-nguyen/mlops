import json
import psycopg2
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:9092"
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
    "cpu": 90,
    "ram": 90,
    "disk": 95,
    "latency": 500,
    "error_rate": 5,
}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_rule(metric_name, value):
    threshold = THRESHOLDS.get(metric_name)
    if threshold is not None and value > threshold:
        return True, f"{metric_name}={value} vượt ngưỡng {threshold}"
    return False, None

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

            data = json.loads(msg.value().decode("utf-8"))

            # 1. lưu vào bảng metrics
            cur.execute(
                "INSERT INTO metrics (ts, host_id, metric_name, value) VALUES (%s, %s, %s, %s)",
                (data["timestamp"], data["host_id"], data["metric_name"], data["value"]),
            )

            # 2. check rule cứng -> alert
            is_alert, reason = check_rule(data["metric_name"], data["value"])
            if is_alert:
                cur.execute(
                    """INSERT INTO alerts (ts, host_id, metric_name, value, reason, source)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (data["timestamp"], data["host_id"], data["metric_name"],
                     data["value"], reason, "rule"),
                )
                print(f"[ALERT] {reason} on {data['host_id']}")

            conn.commit()
            count += 1
            if count % 500 == 0:
                print(f"Consumed {count} messages")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()