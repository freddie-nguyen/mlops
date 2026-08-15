import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg2
import pickle
import tempfile
import os
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "metrics_db", "user": "mlops", "password": "mlops123",
}
MLFLOW_URI = "http://localhost:5000"

NET_FIELDS = ["net_bytes_recv", "net_bytes_sent", "net_err_in", "net_err_out"]
METRIC_GROUP = "net"
REGISTRY_NAME = "anomaly-detector-net"


def load_metrics():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(
        "SELECT ts, metric_name, value FROM metrics WHERE metric_name = ANY(%(fields)s) ORDER BY ts",
        conn, params={"fields": NET_FIELDS},
    )
    conn.close()
    return df


def load_labels():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(
        f"SELECT start_ts, end_ts, label FROM labeled_events WHERE metric_group='{METRIC_GROUP}'",
        conn,
    )
    conn.close()
    return df


def build_wide_table(df):
    wide = df.pivot_table(index="ts", columns="metric_name", values="value", aggfunc="mean").reset_index()
    wide = wide.dropna()
    return wide


def compute_diffs(wide):
    """Cumulative counter -> diff giữa các lần đo liên tiếp"""
    for field in NET_FIELDS:
        wide[f"{field}_diff"] = wide[field].diff().fillna(0)
        wide.loc[wide[f"{field}_diff"] < 0, f"{field}_diff"] = 0  # counter reset -> clip về 0
    return wide


def build_features(wide, window=6):
    feature_cols = []
    for field in NET_FIELDS:
        diff_col = f"{field}_diff"
        roll_col = f"{field}_roll_mean"
        wide[roll_col] = wide[diff_col].rolling(window, min_periods=1).mean()
        feature_cols.append(diff_col)
        feature_cols.append(roll_col)
    return wide, feature_cols


def attach_labels(wide, labels_df):
    wide["true_label"] = "unlabeled"
    for _, row in labels_df.iterrows():
        mask = (wide["ts"] >= row["start_ts"]) & (wide["ts"] <= row["end_ts"])
        wide.loc[mask, "true_label"] = row["label"]
    return wide


def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(f"anomaly-detection-{METRIC_GROUP}")

    df = load_metrics()
    wide = build_wide_table(df)
    wide = compute_diffs(wide)
    wide, feature_cols = build_features(wide)

    labels_df = load_labels()
    wide = attach_labels(wide, labels_df)

    labeled_df = wide[wide["true_label"].isin(["normal", "anomaly"])]

    if labeled_df.empty:
        print("Chưa có dữ liệu labeled cho net, dừng train")
        return

    actual_anomaly_rate = (labeled_df["true_label"] == "anomaly").mean()
    contamination = float(min(max(actual_anomaly_rate, 0.01), 0.5))
    print(f"Tỷ lệ anomaly thật: {actual_anomaly_rate:.2%}, dùng contamination={contamination:.3f}")

    scaler = StandardScaler()
    X_all_scaled = scaler.fit_transform(wide[feature_cols])

    with mlflow.start_run():
        model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42)
        model.fit(X_all_scaled)

        X_labeled_scaled = scaler.transform(labeled_df[feature_cols])
        y_true = (labeled_df["true_label"] == "anomaly").astype(int)
        y_pred = (model.predict(X_labeled_scaled) == -1).astype(int)

        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)

        print(f"Precision={precision:.3f}, Recall={recall:.3f}, F1={f1:.3f}")

        mlflow.log_param("contamination", contamination)
        mlflow.log_param("window", 6)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("n_labeled_samples", len(labeled_df))

        mlflow.sklearn.log_model(model, "model", registered_model_name=REGISTRY_NAME)

        # Log thêm scaler như artifact riêng - inference service cần tải cả 2
        with tempfile.TemporaryDirectory() as tmpdir:
            scaler_path = os.path.join(tmpdir, "scaler.pkl")
            with open(scaler_path, "wb") as f:
                pickle.dump(scaler, f)
            mlflow.log_artifact(scaler_path, artifact_path="scaler")

        print(f"Registered {REGISTRY_NAME} (kèm scaler)")


if __name__ == "__main__":
    main()