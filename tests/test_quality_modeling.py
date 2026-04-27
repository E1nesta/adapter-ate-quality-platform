import csv
import json
import tempfile
import unittest
from pathlib import Path

from adapter_ate.ai_model import build_dataset, predict_if_available, predict_quality, train_model


def write_csv(path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def create_ai_fixture(processed_dir):
    products = []
    items = []
    for index in range(20):
        run_id = f"RUN-{index + 1:03d}"
        sn = f"SN{index + 1:03d}"
        is_fail = index % 4 == 0
        products.append({
            "run_id": run_id,
            "sn": sn,
            "batch_no": "B01",
            "product_model": "ADP-65W",
            "line_id": "LINE-01",
            "equipment_id": "ATE-01",
            "start_time": "2026-04-25 10:00:00",
            "end_time": "2026-04-25 10:00:10",
            "final_result": "FAIL" if is_fail else "PASS",
            "failure_codes": "OUTPUT_VOLTAGE_HIGH" if is_fail else "",
            "defect_count": "1" if is_fail else "0",
            "simulated_sort_result": "FAIL" if is_fail else "PASS",
        })
        values = {
            "hipot_leakage_current": 0.2,
            "hipot_insulation_resistance": 200.0,
            "output_voltage": 12.5 if is_fail else 12.0,
            "output_current": 2.0,
            "efficiency": 84.0 if is_fail else 90.0,
            "ripple": 80.0,
            "temperature": 45.0,
        }
        for item_name, value in values.items():
            items.append({
                "item_id": f"ITEM-{len(items) + 1:03d}",
                "run_id": run_id,
                "sn": sn,
                "station_id": "PERFORMANCE",
                "item_name": item_name,
                "measured_value": str(value),
                "lower_limit": "0",
                "upper_limit": "100",
                "unit": "",
                "test_time": "2026-04-25 10:00:08",
                "required": "True",
                "item_result": "FAIL" if is_fail and item_name in {"output_voltage", "efficiency"} else "PASS",
                "failure_code": "FAIL_CODE" if is_fail and item_name in {"output_voltage", "efficiency"} else "",
                "failure_message": "",
            })

    write_csv(
        processed_dir / "processed_results.csv",
        [
            "run_id",
            "sn",
            "batch_no",
            "product_model",
            "line_id",
            "equipment_id",
            "start_time",
            "end_time",
            "final_result",
            "failure_codes",
            "defect_count",
            "simulated_sort_result",
        ],
        products,
    )
    write_csv(
        processed_dir / "item_results.csv",
        [
            "item_id",
            "run_id",
            "sn",
            "station_id",
            "item_name",
            "measured_value",
            "lower_limit",
            "upper_limit",
            "unit",
            "test_time",
            "required",
            "item_result",
            "failure_code",
            "failure_message",
        ],
        items,
    )


class QualityModelingTests(unittest.TestCase):
    def test_build_dataset_uses_numeric_measurements_and_excludes_identifiers(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed_dir = Path(tmp)
            create_ai_fixture(processed_dir)

            dataset = build_dataset(processed_dir)

            self.assertIn("output_voltage", dataset.feature_names)
            self.assertIn("efficiency", dataset.feature_names)
            self.assertNotIn("sn", dataset.feature_names)
            self.assertNotIn("run_id", dataset.feature_names)
            self.assertEqual(20, len(dataset.labels))

    def test_train_model_writes_model_and_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_dir = base / "processed"
            processed_dir.mkdir()
            create_ai_fixture(processed_dir)
            model_path = base / "quality_model.joblib"
            metrics_path = base / "model_metrics.json"

            metrics = train_model(processed_dir, model_path, metrics_path, random_state=42)

            self.assertTrue(model_path.exists())
            self.assertTrue(metrics_path.exists())
            saved_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertIn("accuracy", saved_metrics)
            self.assertIn("f1_score", saved_metrics)
            self.assertEqual(metrics["feature_names"], saved_metrics["feature_names"])

    def test_prediction_returns_result_confidence_and_model_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_dir = base / "processed"
            processed_dir.mkdir()
            create_ai_fixture(processed_dir)
            model_path = base / "quality_model.joblib"
            metrics_path = base / "model_metrics.json"
            train_model(processed_dir, model_path, metrics_path, random_state=42)

            prediction = predict_quality(
                model_path,
                {
                    "hipot_leakage_current": 0.2,
                    "hipot_insulation_resistance": 200.0,
                    "output_voltage": 12.0,
                    "output_current": 2.0,
                    "efficiency": 90.0,
                    "ripple": 80.0,
                    "temperature": 45.0,
                },
            )

            self.assertIn(prediction["predicted_result"], {"PASS", "FAIL"})
            self.assertIn("confidence", prediction)
            self.assertEqual("quality_model.joblib", prediction["model_name"])

    def test_missing_model_reports_unavailable(self):
        result = predict_if_available(Path("missing-model.joblib"), {"output_voltage": 12.0})

        self.assertFalse(result["available"])
        self.assertEqual("model_unavailable", result["reason"])


if __name__ == "__main__":
    unittest.main()
