from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import mlflow
import numpy as np
import os
import time
import pickle
from collections import defaultdict, deque

app = FastAPI(title='Anomaly Detection Inference Service')

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
CACHE_DIR = '/app/model_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# MODEL_DIR = os.getenv('MODEL_DIR', 'models')

MODEL_REGISTRY_NAMES = {
    "cpu": "anomaly-detector-cpu",
    "mem": "anomaly-detector-mem",
    "disk": "anomaly-detector-disk",
    "net": "anomaly-detector-net",
}

WINDOW = 12
NET_FIELDS = [
    "net_bytes_recv_diff",
    "net_bytes_sent_diff",
    "net_err_in_diff",
    "net_err_out_diff",
]

# def load_model(name: str):
#     """
#     Load model được đóng gói
#     """
#     path = os.path.join(MODEL_DIR, f'{name}_model.pkl')
#     return joblib.load(path)

# def load_scaler():
#     path = os.path.join(MODEL_DIR, 'scaler.pkl')
#     return joblib.load(path)

# models = {
#     'cpu': load_model('cpu'),
#     'mem': load_model('mem'),
#     'disk': load_model('disk'),
#     'net': load_model('net'),
# }

# scalers = {
#     'scaler': load_scaler(),
# }

def load_model_from_mlflow(group, registry_name, max_retries=10, retry_delay=5):
    """
    Load model từ MLflow registry (stage production),
    fallback về cache local nếu gặp lỗi
    """
    cache_path = os.path.join(CACHE_DIR, f"{group}.pkl")
    for attempt in range(max_retries):
        try:
            model = mlflow.sklearn.load_model(
                f"models:/{registry_name}/Production"
            )
            # cache lại để load nếu mlflow bị down ở lần tiếp theo
            with open(cache_path, 'wb') as f:
                pickle.dump(model, f)
            print(f'[{group}] Loaded from MLflow Registry (Production)')
            return model
        except Exception as e:
            print(f'[{group}] Attempt {attempt + 1}/{max_retries} failed: {e}')
            time.sleep(retry_delay)

    # fallback: dùng model từ cache cũ
    if os.path.exists(cache_path):
        print(f'[{group}] WARNING: MLFlow unreachable, falling back to cached model')
        with open(cache_path, 'rb') as f:
            return pickle.load(f)

    print(f'[{group}] ERROR: No model available')
    return None

def load_scaler_from_mlflow(registry_name):
    """
    Net model cần thêm scaler
    """
    try:
        client = mlflow.MlflowClient()
        versions = client.get_latest_versions(registry_name, stages=['Production'])
        if not versions:
            return None
        run_id = versions[0].run_id
        local_path = mlflow.artifacts.download_artifacts(
            run_id=run_id,
            artifact_path='scaler',
        )
        with open(os.path.join(local_path, 'scaler.pkl'), 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print(f'WARNING: could not loa scaler')
        return None

# load model
models = {}
scalers = {}

for group, registry_name in MODEL_REGISTRY_NAMES.items():
    models[group] = load_model_from_mlflow(group, registry_name)
    if group == 'net':
        scalers['net'] = load_scaler_from_mlflow(registry_name)


# In memory state
def create_limited_queue():
    """
    Tạo một deque chứa 12 phần tử
    """
    return deque(maxlen=WINDOW)

# khi một máy chủ (host_id) gửi một dữ liệu đến, dictionary tự động tạo
# riêng một hàng đợi chứa 12 giá trị
rolling_history = defaultdict(create_limited_queue)

last_net_value = {}
net_rolling_history = defaultdict(lambda: deque(maxlen=WINDOW))


class MetricsSnapshot(BaseModel):
    """
    BaseModel là class cốt lõi của Pydantic, được sử dụng để định nghĩa cấu
    trúc dữ liệu, tự động xác thực vầ ép kiểu (parse) dữ liệu dựa trên
    Type Hints (type hints là bắt buộc) của Python.
    """
    host_id: str
    cpu_usage_active: float
    mem_used_percent: float
    disk_used_percent: float
    net_bytes_recv: float
    net_bytes_sent: float
    net_err_in: float
    net_err_out: float

class GroupResult(BaseModel):
    group: str
    is_anomaly: bool
    anomaly_score: float | None
    note: str | None = None

class PredictionResponse(BaseModel):
    host_id: str
    results: list[GroupResult]
    anomalous_count: int
    severity: str

def predict_univariate(group: str, host_id: str, value: float) -> GroupResult:
    """
    Biến hist lưu giá trị:
    ```
    rolling_hist:
    {
        ("server-1", "cpu"): deque([15.2, 16.5, 14.8, 18.2, 11.4], maxlen=12), <- đây là hist
        ("server-1", "mem"): deque([80.0, 80.1, 80.5], maxlen=12),
        ("server-2", "cpu"): deque([5.0, 5.2], maxlen=12)
    }
    ```
    Args:
        group (str): group tương ứng với mô hình (cpu, disk, mem)
        host_id (str): current value (that is appended at the end of the deque)
        value (float):
        
    Returns:

    """
    model = models.get(group)
    if model is None:
        return GroupResult(
            group=group,
            is_anomaly=False,
            anomaly_score=None,
            note='model unavailable',
        )

    hist = rolling_history[(host_id, group)]
    pre_values = list(hist)
    roll_mean = float(np.mean(pre_values)) if pre_values else value
    roll_std  = float(np.std(pre_values)) if len(pre_values) > 1 else 0.0
    diff = value - pre_values[-1] if pre_values else 0.0
    hist.append(value)

    X = np.array([[value, roll_mean, roll_std, diff]])
    pred = model.predict(X)[0]
    score = float(model.decision_function(X)[0])

    return GroupResult(group=group, is_anomaly=bool(pred == -1), anomaly_score=score)


def predict_net(host_id, raw_values: dict):
    """raw_values: {"net_bytes_recv": ..., "net_bytes_sent": ..., "net_err_in": ..., "net_err_out": ...}"""
    model = models.get("net")
    scaler = scalers.get("net")
    if model is None or scaler is None:
        return GroupResult(group="net", is_anomaly=False, anomaly_score=None, note="model/scaler missing")

    feature_vector = []
    for base_field in ["net_bytes_recv", "net_bytes_sent", "net_err_in", "net_err_out"]:
        raw_value = raw_values[base_field]
        diff_field = f"{base_field}_diff"
        key = (host_id, diff_field)

        prev_raw = last_net_value.get(key)
        last_net_value[key] = raw_value

        if prev_raw is None:
            diff_value = 0.0  # lần đầu, chưa có gì để tính diff
        else:
            diff_value = raw_value - prev_raw
            if diff_value < 0:
                diff_value = 0.0  # counter bị reset (reboot/interface restart)

        hist = net_rolling_history[key]
        roll_mean = float(np.mean(hist)) if hist else diff_value
        hist.append(diff_value)

        feature_vector.append(diff_value)
        feature_vector.append(roll_mean)

    X = np.array([feature_vector])
    X_scaled = scaler.transform(X)
    pred = model.predict(X_scaled)[0]
    score = float(model.decision_function(X_scaled)[0])

    return GroupResult(group="net", is_anomaly=bool(pred == -1), anomaly_score=score)

@app.get('/health')
def health():
    loaded = {
        key: (value is not None) for key, value in models.items()
    }
    return {
        'status': 'ok',
        'models_loaded': loaded,
    }

@app.post('/reload-models')
def reload_models():
    for group, registry_name in MODEL_REGISTRY_NAMES.items():
        models[group] = load_model_from_mlflow(group, registry_name, max_retries=1)
        if group == "net":
            scalers["scaler"] = load_scaler_from_mlflow(registry_name)
    return {"status": "reloaded", "models_loaded": {k: (v is not None) for k, v in models.items()}}


@app.post('/predict', response_model=PredictionResponse)
def predict(payload: MetricsSnapshot):
    results = []

    results.append(predict_univariate('cpu', payload.host_id, payload.cpu_usage_active))
    results.append(predict_univariate('mem', payload.host_id, payload.mem_used_percent))
    results.append(predict_univariate('disk', payload.host_id, payload.disk_used_percent))
    results.append(predict_net(
        payload.host_id,
        {
            'net_bytes_recv': payload.net_bytes_recv,
            'net_bytes_sent': payload.net_bytes_sent,
            'net_err_in': payload.net_err_in,
            'net_err_out': payload.net_err_out,
        }
    ))

    anomalous_count = sum(1 for r in results if r.is_anomaly == True)

    if anomalous_count == 0:
        severity = "normal"
    elif anomalous_count == 1:
        severity = "warning"
    else:
        severity = "critical"

    return PredictionResponse(
        host_id=payload.host_id,
        results=results,
        anomalous_count=anomalous_count,
        severity=severity,
    )


if __name__ == "__main__":
    import uvicorn
    # chạy web server bằng uvicorn, truyền app FastAPI ở cổng 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)