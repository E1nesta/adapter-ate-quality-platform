import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adapter_ate.api import create_app, main
from tests.test_processor_core import write_csv, write_rules
from tests.test_storage_and_reports import create_processed_fixture


class ApiFakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def create_raw_fixture(raw_dir):
    write_csv(
        raw_dir / "raw_products.csv",
        [
            "run_id",
            "sn",
            "batch_no",
            "product_model",
            "line_id",
            "equipment_id",
            "start_time",
            "end_time",
            "simulated_sort_result",
        ],
        [
            {
                "run_id": "RUN-001",
                "sn": "SN001",
                "batch_no": "B01",
                "product_model": "ADP-65W",
                "line_id": "LINE-01",
                "equipment_id": "ATE-01",
                "start_time": "2026-04-25 10:00:00",
                "end_time": "2026-04-25 10:00:10",
                "simulated_sort_result": "PASS",
            }
        ],
    )
    write_csv(
        raw_dir / "raw_test_items.csv",
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
        ],
        [
            {
                "item_id": "ITEM-001",
                "run_id": "RUN-001",
                "sn": "SN001",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "measured_value": "12.00",
                "lower_limit": "11.8",
                "upper_limit": "12.2",
                "unit": "V",
                "test_time": "2026-04-25 10:00:08",
            }
        ],
    )
    write_csv(
        raw_dir / "raw_station_events.csv",
        ["event_id", "run_id", "sn", "station_id", "event_type", "event_message", "event_time"],
        [
            {
                "event_id": "EVT-001",
                "run_id": "RUN-001",
                "sn": "SN001",
                "station_id": "SCAN",
                "event_type": "SCAN_OK",
                "event_message": "SN scanned",
                "event_time": "2026-04-25 10:00:00",
            }
        ],
    )


class AdapterApiTests(unittest.TestCase):
    def make_client(self):
        tmp = tempfile.TemporaryDirectory()
        base = Path(tmp.name)
        processed_dir = base / "processed"
        reports_dir = base / "reports"
        processed_dir.mkdir()
        create_processed_fixture(processed_dir)
        app = create_app(processed_dir=processed_dir, reports_dir=reports_dir, data_source="csv")
        self.addCleanup(tmp.cleanup)
        return app.test_client()

    def test_health_endpoint_returns_service_and_database_status(self):
        client = self.make_client()

        response = client.get("/api/health")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual("ok", data["status"])
        self.assertIn("database", data)
        self.assertIn("data_source", data)

    def test_health_endpoint_reports_data_source_mode_and_mysql_availability(self):
        connection = ApiFakeConnection()
        client = create_app(data_source="mysql").test_client()

        with patch("adapter_ate.api.connect", return_value=connection):
            response = client.get("/api/health")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual("mysql", data["data_source"]["mode"])
        self.assertTrue(data["data_source"]["mysql_available"])
        self.assertTrue(connection.closed)

    def test_product_endpoint_returns_trace(self):
        client = self.make_client()

        response = client.get("/api/products/SN002")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual("SN002", data["product"]["sn"])
        self.assertEqual("FAIL", data["product"]["final_result"])
        self.assertEqual("OUTPUT_VOLTAGE_HIGH", data["defects"][0]["failure_code"])

    def test_csv_data_source_mode_preserves_product_trace_query(self):
        client = self.make_client()

        with patch("adapter_ate.api.connect", side_effect=AssertionError("mysql should not be used")):
            response = client.get("/api/products/SN002")

        self.assertEqual(200, response.status_code)
        self.assertEqual("SN002", response.get_json()["product"]["sn"])

    def test_auto_data_source_uses_mysql_when_query_succeeds(self):
        connection = ApiFakeConnection()
        db_trace = {
            "product": {"sn": "DB001", "final_result": "PASS"},
            "items": [],
            "events": [],
            "defects": [],
        }
        client = create_app(data_source="auto").test_client()

        with (
            patch("adapter_ate.api.connect", return_value=connection),
            patch("adapter_ate.api.query_product_trace", return_value=db_trace, create=True) as query,
        ):
            response = client.get("/api/products/DB001")

        self.assertEqual(200, response.status_code)
        self.assertEqual("DB001", response.get_json()["product"]["sn"])
        query.assert_called_once_with(connection, "DB001")
        self.assertTrue(connection.closed)

    def test_auto_data_source_falls_back_to_csv_when_mysql_fails(self):
        client = self.make_client()

        with patch("adapter_ate.api.connect", side_effect=OSError("connection refused")):
            response = client.get("/api/products/SN002")

        self.assertEqual(200, response.status_code)
        self.assertEqual("SN002", response.get_json()["product"]["sn"])

    def test_mysql_data_source_returns_error_when_mysql_fails(self):
        client = create_app(data_source="mysql").test_client()

        with patch("adapter_ate.api.connect", side_effect=OSError("connection refused")):
            response = client.get("/api/products/SN002")

        self.assertEqual(503, response.status_code)
        self.assertEqual("data_source_error", response.get_json()["error"])

    def test_product_endpoint_returns_404_for_missing_sn(self):
        client = self.make_client()

        response = client.get("/api/products/NOPE")

        self.assertEqual(404, response.status_code)
        self.assertEqual("not_found", response.get_json()["error"])

    def test_batch_yield_endpoint_returns_counts(self):
        client = self.make_client()

        response = client.get("/api/batches/B01/yield")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual(2, data["total_count"])
        self.assertEqual(1, data["pass_count"])
        self.assertEqual(50.0, data["yield_rate"])

    def test_defects_endpoint_groups_failures(self):
        client = self.make_client()

        response = client.get("/api/defects")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual("OUTPUT_VOLTAGE_HIGH", data["defects"][0]["failure_code"])
        self.assertEqual(1, data["defects"][0]["defect_count"])

    def test_station_summary_endpoint_returns_station_yield(self):
        client = self.make_client()

        response = client.get("/api/stations/PERFORMANCE/summary")

        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertEqual("PERFORMANCE", data["station_id"])
        self.assertEqual(1, data["defect_count"])

    def test_invalid_json_error_shape_for_predict_placeholder(self):
        client = self.make_client()

        response = client.post("/api/predict", data="not-json")

        self.assertEqual(400, response.status_code)
        self.assertEqual("invalid_json", response.get_json()["error"])

    def test_process_endpoint_processes_raw_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_dir = base / "raw"
            output_dir = base / "processed"
            raw_dir.mkdir()
            config_path = base / "rules.json"
            log_path = base / "reports" / "process.log"
            create_raw_fixture(raw_dir)
            write_rules(config_path)
            client = create_app().test_client()

            response = client.post(
                "/api/process",
                json={
                    "raw_dir": str(raw_dir),
                    "config": str(config_path),
                    "output_dir": str(output_dir),
                    "log": str(log_path),
                },
            )

            self.assertEqual(200, response.status_code)
            data = response.get_json()
            self.assertEqual(1, data["total_count"])
            self.assertEqual(1, data["pass_count"])
            self.assertTrue((output_dir / "processed_results.csv").exists())
            self.assertTrue(log_path.exists())

    def test_process_endpoint_returns_400_for_missing_raw_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_dir = base / "raw"
            output_dir = base / "processed"
            raw_dir.mkdir()
            config_path = base / "rules.json"
            write_rules(config_path)
            client = create_app().test_client()

            response = client.post(
                "/api/process",
                json={
                    "raw_dir": str(raw_dir),
                    "config": str(config_path),
                    "output_dir": str(output_dir),
                },
            )

            self.assertEqual(400, response.status_code)
            self.assertEqual("raw_data_error", response.get_json()["error"])

    def test_generate_reports_endpoint_writes_report_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_dir = base / "processed"
            reports_dir = base / "reports"
            processed_dir.mkdir()
            create_processed_fixture(processed_dir)
            client = create_app(processed_dir=processed_dir, reports_dir=reports_dir).test_client()

            response = client.post("/api/reports/generate")

            self.assertEqual(200, response.status_code)
            data = response.get_json()
            self.assertEqual(2, data["summary"]["total_count"])
            self.assertTrue((reports_dir / "daily_summary.csv").exists())
            self.assertIn("daily_summary", data["reports"])

    def test_generate_reports_endpoint_returns_400_for_missing_processed_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_dir = base / "processed"
            reports_dir = base / "reports"
            processed_dir.mkdir()
            client = create_app(processed_dir=processed_dir, reports_dir=reports_dir).test_client()

            response = client.post("/api/reports/generate")

            self.assertEqual(400, response.status_code)
            self.assertEqual("report_generation_failed", response.get_json()["error"])

    def test_storage_import_endpoint_returns_clear_error_when_mysql_unreachable(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed_dir = Path(tmp) / "processed"
            processed_dir.mkdir()
            create_processed_fixture(processed_dir)
            client = create_app(processed_dir=processed_dir).test_client()

            with patch("adapter_ate.api.connect", side_effect=OSError("connection refused")):
                response = client.post("/api/storage/import")

            self.assertEqual(503, response.status_code)
            self.assertEqual("storage_import_failed", response.get_json()["error"])

    def test_main_wires_cli_paths_to_matching_app_config(self):
        captured = {}

        def fake_run(self, host=None, port=None):
            captured["host"] = host
            captured["port"] = port
            captured["raw_dir"] = self.config["RAW_DIR"]
            captured["config_path"] = self.config["CONFIG_PATH"]
            captured["processed_dir"] = self.config["PROCESSED_DIR"]
            captured["reports_dir"] = self.config["REPORTS_DIR"]
            captured["model_path"] = self.config["MODEL_PATH"]

        argv = [
            "adapter_ate.api",
            "--processed-dir",
            "custom/processed",
            "--reports-dir",
            "custom/reports",
            "--model",
            "custom/model.joblib",
            "--host",
            "0.0.0.0",
            "--port",
            "5050",
        ]

        with patch.object(sys, "argv", argv), patch("flask.Flask.run", fake_run):
            main()

        self.assertEqual("data/raw", captured["raw_dir"])
        self.assertEqual("config/test_rules.json", captured["config_path"])
        self.assertEqual("custom/processed", captured["processed_dir"])
        self.assertEqual("custom/reports", captured["reports_dir"])
        self.assertEqual("custom/model.joblib", captured["model_path"])
        self.assertEqual("0.0.0.0", captured["host"])
        self.assertEqual(5050, captured["port"])


if __name__ == "__main__":
    unittest.main()
