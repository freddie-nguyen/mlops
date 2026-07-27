# import json
# import time
# from datetime import datetime, timezone
# from confluent_kafka import Producer

# KAFKA_BOOTSTRAP = "localhost:9092"
# TOPIC = "metrics-raw"

# producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# host = "host_01"

# payload = {
#     "timestamp": datetime.now(timezone.utc).isoformat(),
#     "host_id": host,
#     "metric_name": "cpu",
#     "value": 67,
#     "_injected_anomaly": True,
# }

# producer.produce(
#     TOPIC,
#     key=host.encode("utf-8"),
#     value=json.dumps(payload).encode("utf-8"),
# )

# producer.plush()

# print('message sent')