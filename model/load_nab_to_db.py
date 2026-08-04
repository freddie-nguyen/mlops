import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

NAB_ROOT = "NAB"
DATA_DIR =  os.path.join(NAB_ROOT, "data", "realAWSCloudwatch")
WINDOWS_FILE = os.path.join(NAB_ROOT, "labels", "combined_windows.json")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "mlops",
    "password": "mlops123",
}

def load_windows():
    with open(WINDOWS_FILE) as f:
        return json.load(f)

def is_in_any_window(ts, windows):
    for start, end in windows:
        if pd.Timestamp(start) <= ts <= pd.Timestamp(end):
            return True
    return False

def process_file(filepath, windows):
    df = pd.read_csv(filepath)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["is_anomaly"] = [is_in_any_window(t, windows) for t in df["timestamp"]]
    return df

def main():
    all_windows = load_windows()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    total_rows = 0
    total_anomalies = 0

    for filename in sorted(os.listdir(DATA_DIR)):
        if not filename.endswith(".csv"):
            continue

        key = f"realAWSCloudwatch/{filename}"
        windows = all_windows.get(key, [])

        filepath = os.path.join(DATA_DIR, filename)
        df = process_file(filepath, windows)

        rows = [
            (row.timestamp, filename, row.value, bool(row.is_anomaly))
            for row in df.itertuples()
        ]

        execute_values(
            cur,
            "INSERT INTO metrics_labeled (ts, source_file, value, is_anomaly) VALUES %s",
            rows,
        )
        conn.commit()

        n_anomaly = df["is_anomaly"].sum()
        total_rows += len(df)
        total_anomalies += n_anomaly
        print(f"{filename}: {len(df)} rows, {n_anomaly} anomalies")

    print(f"\nTOTAL: {total_rows} rows loaded, {total_anomalies} anomalies ({total_anomalies/total_rows*100:.2f}%)")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()