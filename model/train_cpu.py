# train/train_cpu.py
import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg2
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score

# Chạy TRÊN SERVER -> dùng localhost
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "mlops",
    "password": "mlops123",
}

MLFLOW_URI = "http://localhost:5000"

METRIC_NAME = "cpu_usage_active"
METRIC_GROUP = "cpu"
REGISTRY_NAME = "anomaly-detector-cpu"


def load_metrics():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(
        f"SELECT ts, value FROM metrics WHERE metric_name='{METRIC_NAME}' ORDER BY ts",
        conn,
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


def attach_labels(metrics_df, labels_df):
    metrics_df["true_label"] = "unlabeled"
    for _, row in labels_df.iterrows():
        mask = (metrics_df["ts"] >= row["start_ts"]) & (metrics_df["ts"] <= row["end_ts"])
        metrics_df.loc[mask, "true_label"] = row["label"]
    return metrics_df


def build_features(df, window=6):
    df["roll_mean"] = df["value"].rolling(window, min_periods=1).mean()
    df["roll_std"] = df["value"].rolling(window, min_periods=1).std().fillna(0)
    df["diff"] = df["value"].diff().fillna(0)
    return df


def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(f"anomaly-detection-{METRIC_GROUP}")

    metrics_df = load_metrics()
    labels_df = load_labels()
    metrics_df = attach_labels(metrics_df, labels_df)
    metrics_df = build_features(metrics_df)

    feature_cols = ["value", "roll_mean", "roll_std", "diff"]
    labeled_df = metrics_df[metrics_df["true_label"].isin(["normal", "anomaly"])]

    actual_anomaly_rate = (labeled_df["true_label"] == "anomaly").mean()
    contamination = float(min(max(actual_anomaly_rate, 0.01), 0.5))
    print(f"Tỷ lệ anomaly thật: {actual_anomaly_rate:.2%}, dùng contamination={contamination:.3f}")

    with mlflow.start_run():
        model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42)
        model.fit(metrics_df[feature_cols])

        X_labeled = labeled_df[feature_cols]
        y_true = (labeled_df["true_label"] == "anomaly").astype(int)
        y_pred = (model.predict(X_labeled) == -1).astype(int)

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

        # ĐÓNG GÓI + ĐĂNG KÝ MODEL (Bước 2 gộp vào đây)
        mlflow.sklearn.log_model(
            model, "model",
            registered_model_name=REGISTRY_NAME,
        )
        print(f"Registered {REGISTRY_NAME}")


if __name__ == "__main__":
    main()