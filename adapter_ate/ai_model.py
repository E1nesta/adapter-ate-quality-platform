import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split


@dataclass(frozen=True)
class QualityDataset:
    features: pd.DataFrame
    labels: pd.Series
    feature_names: list[str]


def build_dataset(processed_dir):
    processed_dir = Path(processed_dir)
    products = pd.read_csv(processed_dir / "processed_results.csv")
    items = pd.read_csv(processed_dir / "item_results.csv")

    item_features = items.pivot_table(
        index="run_id",
        columns="item_name",
        values="measured_value",
        aggfunc="first",
    )
    labels = products.set_index("run_id")["final_result"]
    dataset = item_features.join(labels, how="inner").dropna()
    feature_names = [str(name) for name in item_features.columns]
    features = dataset[feature_names].astype(float)
    return QualityDataset(features=features, labels=dataset["final_result"], feature_names=feature_names)


def train_model(processed_dir, model_path, metrics_path, random_state=42):
    dataset = build_dataset(processed_dir)
    stratify = dataset.labels if dataset.labels.nunique() > 1 and len(dataset.labels) >= 10 else None
    x_train, x_test, y_train, y_test = train_test_split(
        dataset.features,
        dataset.labels,
        test_size=0.3,
        random_state=random_state,
        stratify=stratify,
    )
    model = RandomForestClassifier(n_estimators=80, random_state=random_state)
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    metrics = {
        "accuracy": round(float(accuracy_score(y_test, predictions)), 4),
        "precision": round(float(precision_score(y_test, predictions, pos_label="FAIL", zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, predictions, pos_label="FAIL", zero_division=0)), 4),
        "f1_score": round(float(f1_score(y_test, predictions, pos_label="FAIL", zero_division=0)), 4),
        "confusion_matrix": confusion_matrix(y_test, predictions, labels=["PASS", "FAIL"]).tolist(),
        "feature_names": dataset.feature_names,
        "model_name": Path(model_path).name,
    }

    artifact = {
        "model": model,
        "feature_names": dataset.feature_names,
    }
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, model_path)
    Path(metrics_path).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return metrics


def predict_quality(model_path, features):
    artifact = joblib.load(model_path)
    model = artifact["model"]
    feature_names = artifact["feature_names"]
    row = pd.DataFrame([{name: float(features[name]) for name in feature_names}])
    predicted_result = str(model.predict(row)[0])

    confidence = 1.0
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(row)[0]
        confidence = float(max(probabilities))

    return {
        "predicted_result": predicted_result,
        "confidence": round(confidence, 4),
        "model_name": Path(model_path).name,
    }


def predict_if_available(model_path, features):
    model_path = Path(model_path)
    if not model_path.exists():
        return {
            "available": False,
            "reason": "model_unavailable",
        }
    prediction = predict_quality(model_path, features)
    return {
        "available": True,
        **prediction,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Train adapter ATE quality model")
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--model", default="models/quality_model.joblib")
    parser.add_argument("--metrics", default="reports/model_metrics.json")
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def main():
    args = parse_args()
    metrics = train_model(args.processed_dir, args.model, args.metrics, args.random_state)
    print(
        "trained "
        f"accuracy={metrics['accuracy']} f1={metrics['f1_score']} "
        f"model={metrics['model_name']}"
    )


if __name__ == "__main__":
    main()
