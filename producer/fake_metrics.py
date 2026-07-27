import json
import time
import random
import numpy as np
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "metrics-raw"
HOSTS = [f"host-{i:02d}" for i in range(1, 6)]  # 5 host giả

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# baseline "bình thường" cho mỗi loại metric
BASELINE = {
    "cpu":     {"mean": 40, "std": 8,  "min": 0, "max": 100},
    "ram":     {"mean": 55, "std": 10, "min": 0, "max": 100},
    "disk":    {"mean": 60, "std": 5,  "min": 0, "max": 100},
    "latency": {"mean": 120, "std": 20, "min": 0, "max": None},   # ms
    "error_rate": {"mean": 0.5, "std": 0.3, "min": 0, "max": None}, # %
}

ANOMALY_PROB = 0.01  # 1% message là bất thường

def gen_value(metric):
    cfg = BASELINE[metric]
    is_anomaly = random.random() < ANOMALY_PROB
    if is_anomaly:
        # spike mạnh: 3-6 std so với baseline
        multiplier = random.choice([1, -1]) * random.uniform(3, 6)
        val = cfg["mean"] + multiplier * cfg["std"]
    else:
        val = np.random.normal(cfg["mean"], cfg["std"])
    val = max(val, cfg["min"])
    if cfg["max"] is not None:
        val = min(val, cfg["max"])
    return round(val, 2), is_anomaly

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    print(f"Producing to {TOPIC} ...")
    msg_count = 0
    while True:
        for host in HOSTS:
            for metric in BASELINE.keys():
                value, is_anomaly = gen_value(metric)
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "host_id": host,
                    "metric_name": metric,
                    "value": value,
                    "_injected_anomaly": is_anomaly,  # field debug, model sẽ không thấy field này khi consume thật
                }
                producer.produce(
                    TOPIC,
                    key=host.encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    callback=delivery_report,
                )
                msg_count += 1
        producer.poll(0)
        if msg_count % 500 == 0:
            print(f"Sent {msg_count} messages")
        time.sleep(1)  # mỗi giây 1 batch (5 host x 5 metric = 25 msg/s)

if __name__ == "__main__":
    main()