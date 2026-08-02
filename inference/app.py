from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import os
from collections import defaultdict, deque

app = FastAPI(title='Anomaly Detection Inference Service')

MODEL_DIR = os.getenv('MODEL_DIR', 'model/models')
WINDOW = 12
NET_FIELDS = [
    "net_bytes_recv_diff",
    "net_bytes_sent_diff",
    "net_err_in_diff",
    "net_err_out_diff",
]

def load_model(name: str):
    """
    Load model được đóng gói
    """
    path = os.path.join(MODEL_DIR, f'{name}_model.pkl')
    return joblib.load(path)

def load_scaler():
    path = os.path.join(MODEL_DIR, 'scaler.pkl')
    return joblib.load(path)

models = {
    'cpu': load_model('cpu'),
    'mem': load_model('mem'),
    'disk': load_model('disk'),
    'net': load_model('net'),
}

scalers = {
    'scaler': load_scaler(),
}

# In-memory state
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

def predict_univariate(group: str, host_id: str, value):
    model = models.get(group)

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
    scaler = scalers.get("scaler")
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

    anomalous_count = sum(1 for r in results if r.is_anomaly)

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
    uvicorn.run(app, host="0.0.0.0", port=8000)