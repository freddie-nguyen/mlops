# scripts/promote_model.py
import mlflow
import sys

mlflow.set_tracking_uri("http://localhost:5000")
client = mlflow.MlflowClient()

MODEL_NAMES = ["anomaly-detector-cpu", "anomaly-detector-mem",
               "anomaly-detector-disk", "anomaly-detector-net"]

MIN_RECALL = 0.40      # ngưỡng tối thiểu để coi là "đủ tốt" cho Staging
MIN_PRECISION = 0.40


def promote_best_version(model_name):
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        print(f"[{model_name}] Không có version nào")
        return

    best_version = None
    best_f1 = -1

    for v in versions:
        run = client.get_run(v.run_id)
        precision = run.data.metrics.get("precision", 0)
        recall = run.data.metrics.get("recall", 0)
        f1 = run.data.metrics.get("f1_score", 0)

        print(f"  v{v.version}: precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")

        if precision >= MIN_PRECISION and recall >= MIN_RECALL and f1 > best_f1:
            best_f1 = f1
            best_version = v.version

    if best_version is None:
        print(f"[{model_name}] Không có version nào đạt ngưỡng tối thiểu, KHÔNG promote")
        return

    # Bước trung gian: Staging (để "duyệt" trước khi lên Production)
    client.transition_model_version_stage(
        name=model_name, version=best_version, stage="Staging",
    )
    print(f"[{model_name}] v{best_version} (f1={best_f1:.3f}) -> Staging")

    # Promote thẳng lên Production (trong đồ án, có thể tự động luôn;
    # trong thực tế công ty thường cần người duyệt thủ công ở bước Staging trước)
    client.transition_model_version_stage(
        name=model_name, version=best_version, stage="Production",
        archive_existing_versions=True,   # version Production cũ tự chuyển sang Archived
    )
    print(f"[{model_name}] v{best_version} -> Production")


def main():
    for model_name in MODEL_NAMES:
        print(f"\n=== {model_name} ===")
        promote_best_version(model_name)


if __name__ == "__main__":
    main()