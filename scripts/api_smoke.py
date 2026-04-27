import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapter_ate.api import create_app


def main():
    app = create_app(
        processed_dir="data/processed",
        reports_dir="reports",
        model_path="models/quality_model.joblib",
    )
    client = app.test_client()

    checks = [
        ("POST", "/api/process", {}, 200),
        ("POST", "/api/reports/generate", {}, 200),
        ("GET", "/api/health", None, 200),
        ("GET", "/api/batches/B20260425/yield", None, 200),
        ("GET", "/api/defects", None, 200),
        ("GET", "/api/stations/PERFORMANCE/summary", None, 200),
    ]
    for method, path, payload, expected_status in checks:
        if method == "GET":
            response = client.get(path)
        else:
            response = client.post(path, json=payload)
        if response.status_code != expected_status:
            raise SystemExit(f"{method} {path} returned {response.status_code}, expected {expected_status}")

    first_product = client.get("/api/batches/B20260425/yield").get_json()
    if first_product["total_count"] <= 0:
        raise SystemExit("batch yield returned no products")

    prediction = client.post(
        "/api/predict",
        json={
            "efficiency": 90.0,
            "hipot_ac_withstand": 3750.0,
            "hipot_insulation_resistance": 200.0,
            "hipot_leakage_current": 0.2,
            "ocp_trip_current": 2.8,
            "output_current": 2.0,
            "output_voltage": 12.0,
            "ripple": 75.0,
            "scp_response_time": 20.0,
            "temperature": 45.0,
        },
    )
    if prediction.status_code != 200:
        raise SystemExit(f"POST /api/predict returned {prediction.status_code}")

    print("API smoke checks passed")


if __name__ == "__main__":
    main()
