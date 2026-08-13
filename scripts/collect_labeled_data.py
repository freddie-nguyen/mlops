# scripts/collect_labeled_data.py
import subprocess
import time
import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "metrics_db", "user": "mlops", "password": "mlops123",
}
HOST_ID = "intern-metrics-server-1"

def log_event(cur, metric_group, label, scenario, start_ts, end_ts, notes=""):
    cur.execute(
        """INSERT INTO labeled_events (host_id, metric_group, label, scenario, start_ts, end_ts, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (HOST_ID, metric_group, label, scenario, start_ts, end_ts, notes),
    )

def run_scenario(cur, metric_group, scenario_name, cmd, duration_sec, cooldown_sec=30):
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