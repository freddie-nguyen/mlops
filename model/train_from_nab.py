import pandas as pd
import psycopg2
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score
import joblib

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "mlops",
    "password": "mlops123",
}

def load_data(source_file):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(
        "SELECT ts, value, is_anomaly FROM metrics_labeled WHERE source_file = %(f)s ORDER BY ts",
        conn, params={"f": source_file},
    )
    conn.close()
    return df

def build_features(df, window=12):
    '''
    Rolling mean/std làm feature, giúp model detect các Contextual Anomaly
    '''
    df["roll_mean"] = df["value"].rolling(window, min_periods=1).mean()
    df["roll_std"]  = df["value"].rolling(window, min_periods=1).std()
    df["diff"]      = df["value"].diff().fillna(0)
    return df

def train_and_eval(source_file):
    df = load_data(source_file=source_file)
    if len(df) < 50:
        print(f'Skip {source_file}: too few rooms')
        return None

    df = build_features(df=df)
    feature_cols = ["value", "roll_mean", "roll_std", "diff"]
    X = df[feature_cols]
    y_true = df["is_anomaly"].astype(int)

    contamination = max(y_true.mean(), 0.001)
    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
    )
    model.fit(X)

    predictions = model.predict(X)
    y_pred = (predictions == -1).astype(int)

    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)

    print(f"{source_file}: precision: {precision:.3f} recall: {recall:.3f} f1: {f1:.3f}")
    print(f'(n_anomaly={y_true.sum()}/{len(y_true)})')

    return model, {
        "precision": precision,
        "recall": recall,
        "f1": f1
    }

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    files = pd.read_sql("SELECT DISTINCT source_file FROM metrics_labeled", conn)["source_file"].tolist()
    conn.close()

    results = []
    best_model = None
    best_f1 = -1
    best_file = None

    for f in files:
        result = train_and_eval(f)
        if result is None:
            continue
        model, metrics = result
        results.append({
            'file': f,
            **metrics
        })
        if metrics['f1'] > best_f1:
            best_f1 = metrics['f1']
            best_model = model
            best_file = f

        results_df = pd.DataFrame(results)
        print('\n')
        print('SUMMARY: \n')
        print(results_df.describe())

        joblib.dump(best_model, "models/isolation_forest_nab_v1.pkl")
        print(f"\nBest model (from {best_file}, f1={best_f1:.3f}) saved to models/isolation_forest_nab_v1.pkl")

if __name__ == "__main__":
    main()