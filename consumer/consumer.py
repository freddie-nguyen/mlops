import json
import psycopg2
import requests
import os
# from dotenv import load_dotenv
from confluent_kafka import Consumer
from datetime import datetime, timezone
from collections import defaultdict

# load_dotenv()

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
TOPIC = os.getenv('TOPIC')
GROUP_ID = os.getenv('GROUP_ID')
INFERENCE_URL = os.getenv('INFERENCE_URL')

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

REQUIRED_FIELDS = {
    "cpu_usage_active",
    "mem_used_percent",
    "disk_used_percent",
    "net_bytes_recv",
    "net_bytes_sent",
    "net_err_in",
    "net_err_out",
}

BUCKET_SECONDS = 10  # khớp với interval Telegraf đang set

ALLOWED_METRICS = {
    "cpu": {"usage_idle"},
    "mem": {"used_percent"},
    "disk": {"used_percent"},
    "net": {"bytes_recv", "bytes_sent", "err_in", "err_out"},
}

# Buffer gom field theo (host_id, time_bucket) -> {metric_name: value}
# {
#     # Lớp ngoài: key là tuple (host_id, time_bucket)

#     ("server-01", datetime(2026, 8, 4, 9, 50, 10)): {
#         "cpu_usage_active": 45.5,
#         "mem_used_percent": 60.2,
#         "disk_used_percent": 12.0
#     },
# 
#     ("server-02", datetime(2026, 8, 4, 9, 50, 10)): {
#         "cpu_usage_active": 80.1,
#         ...
#     }
# }

snapshot_buffer = defaultdict(dict)

def floor_timestamp(ts: datetime, seconds: int = BUCKET_SECONDS) -> datetime:
    """
    Giúp gom các điểm dữ liệu rải rác trong một khoảng thời gian về cùng một mốc để dễ dàng xử lý. Ví dụ, nếu seconds=10:

    10:05:13 -> làm tròn về 10:05:10

    10:05:17 -> làm tròn về 10:05:10

    10:05:19 -> làm tròn về 10:05:10
    """
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def parse_telegraf_message(raw_value: str) -> list[dict[str, any]]:
    """
    Parse string sent from Telegraf/Kafka

    Return:
        example:
        ```
        [
            {
                'ts': datetime.datetime(2026, 8, 4, 1, 55, tzinfo=datetime.timezone.utc),
                'host_id': 'intern-metrics-server-1',
                'metric_name': 'net_bytes_recv',
                'value': 229896602.0
            },
            {
                'ts': datetime.datetime(2026, 8, 4, 1, 55, tzinfo=datetime.timezone.utc),
                'host_id': 'intern-metrics-server-1',
                'metric_name': 'net_bytes_sent',
                'value': 268119960.0
            },
            ...
        ]
        ```
    """
    
    data: dict = json.loads(raw_value)
    metric_group: str = data.get('name')        # metric_group → cpu, mem, disk, net

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

def call_inference(host_id: str, fields: dict):
    payload = {
        "host_id": host_id,
        "cpu_usage_active": fields["cpu_usage_active"],
        "mem_used_percent": fields["mem_used_percent"],
        "disk_used_percent": fields["disk_used_percent"],
        "net_bytes_recv": fields["net_bytes_recv"],
        "net_bytes_sent": fields["net_bytes_sent"],
        "net_err_in": fields["net_err_in"],
        "net_err_out": fields["net_err_out"],
    }
    try:
        response = requests.post(
            INFERENCE_URL,
            json=payload,
            timeout=3,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Inference error {e}')
        return None

def save_alert(cur, ts, host_id, result):

    cur.execute(
        """INSERT INTO alerts (ts, host_id, metric_name, value, score, reason, source)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (
            ts, host_id, result["group"], None,
            result.get("anomaly_score"),
            f"{result['group']} anomaly detected by model",
            "model",
        ),
    )

def main():
    # ==========================================================
    # test env
    REQUIRED_ENV_VARS = [
        'KAFKA_BOOTSTRAP', 'TOPIC', 'GROUP_ID', 'INFERENCE_URL',
        'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
    ]

    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {missing}")
    # ==========================================================

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print('Consumer started\nListening...')
    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            try:
                rows = parse_telegraf_message(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, KeyError) as e:
                print(f'Skip: {e}')
                continue

            for row in rows:
                # lưu vào trong database
                cur.execute(
                    "INSERT INTO metrics (ts, host_id, metric_name, value) VALUES (%s, %s, %s, %s)",
                    (row['ts'], row['host_id'], row['metric_name'], row['value']),
                )

                # gom vào buffer theo (host_id, time_bucket)
                bucket = floor_timestamp(row['ts'])
                key: tuple[str, datetime] = (row['host_id'], bucket) 
                snapshot_buffer[key][row['metric_name']] = row['value']

                # kiểm tra xem gom đủ 7 fields chưa
                if REQUIRED_FIELDS.issubset(snapshot_buffer[key].keys()):
                    fields = snapshot_buffer.pop(key)
                    result = call_inference(row["host_id"], fields)
                    # print(f"[DEBUG] Inference result: {result}")


                    if result:
                        print(f"[{row['host_id']}] severity={result['severity']} "
                              f"anomalous_count={result['anomalous_count']}")

                        for group_result in result["results"]:
                            if group_result["is_anomaly"]:
                                save_alert(cur, bucket, row["host_id"], group_result)
                                print(f"  [ALERT] {group_result['group']} "
                                      f"score={group_result['anomaly_score']:.3f}")

            conn.commit()
            count += 1
            if count % 100 == 0:
                print(f"Consumed {count} kafka messages, buffer size={len(snapshot_buffer)}")
 
                
    except KeyboardInterrupt:
        print('Stop consuming')
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()