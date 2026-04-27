import csv
import os
import tempfile
import unittest
from pathlib import Path

from adapter_ate.reports import generate_reports
from adapter_ate.storage import (
    CREATE_TABLES_SQL,
    db_config_from_env,
    query_batch_yield,
    query_defect_summary,
    query_product_trace,
    query_station_summary,
    upsert_processed_dir,
)


def write_csv(path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.commit_count = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_count += 1


class QueryCursor:
    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.current_result = []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.current_result = self.result_sets.pop(0) if self.result_sets else []

    def fetchone(self):
        if not self.current_result:
            return None
        return self.current_result[0]

    def fetchall(self):
        return self.current_result


class QueryConnection:
    def __init__(self, result_sets):
        self.cursor_instance = QueryCursor(result_sets)

    def cursor(self, *args, **kwargs):
        return self.cursor_instance


def create_processed_fixture(base):
    write_csv(
        base / "processed_results.csv",
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
                "final_result": "PASS",
                "failure_codes": "",
                "defect_count": "0",
                "simulated_sort_result": "PASS",
            },
            {
                "run_id": "RUN-002",
                "sn": "SN002",
                "batch_no": "B01",
                "product_model": "ADP-65W",
                "line_id": "LINE-01",
                "equipment_id": "ATE-01",
                "start_time": "2026-04-25 10:01:00",
                "end_time": "2026-04-25 10:01:10",
                "final_result": "FAIL",
                "failure_codes": "OUTPUT_VOLTAGE_HIGH",
                "defect_count": "1",
                "simulated_sort_result": "FAIL",
            },
        ],
    )
    write_csv(
        base / "item_results.csv",
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
        [
            {
                "item_id": "ITEM-001",
                "run_id": "RUN-001",
                "sn": "SN001",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "measured_value": "12.0",
                "lower_limit": "11.8",
                "upper_limit": "12.2",
                "unit": "V",
                "test_time": "2026-04-25 10:00:08",
                "required": "True",
                "item_result": "PASS",
                "failure_code": "",
                "failure_message": "",
            }
        ],
    )
    write_csv(
        base / "station_events.csv",
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
    write_csv(
        base / "defects.csv",
        [
            "item_id",
            "run_id",
            "sn",
            "station_id",
            "item_name",
            "failure_code",
            "failure_message",
            "measured_value",
            "lower_limit",
            "upper_limit",
            "unit",
            "test_time",
        ],
        [
            {
                "item_id": "ITEM-002",
                "run_id": "RUN-002",
                "sn": "SN002",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "failure_code": "OUTPUT_VOLTAGE_HIGH",
                "failure_message": "output_voltage above lower limit",
                "measured_value": "12.5",
                "lower_limit": "11.8",
                "upper_limit": "12.2",
                "unit": "V",
                "test_time": "2026-04-25 10:01:08",
            }
        ],
    )


class StorageAndReportTests(unittest.TestCase):
    def test_schema_defines_required_tables_and_idempotency_keys(self):
        for table_name in (
            "product_test_runs",
            "test_item_results",
            "station_events",
            "defect_reasons",
            "ai_predictions",
        ):
            self.assertIn(table_name, CREATE_TABLES_SQL)
        self.assertIn("PRIMARY KEY (run_id)", CREATE_TABLES_SQL)
        self.assertIn("PRIMARY KEY (item_id)", CREATE_TABLES_SQL)
        self.assertIn("PRIMARY KEY (event_id)", CREATE_TABLES_SQL)

    def test_db_config_reads_environment(self):
        env = {
            "MYSQL_HOST": "127.0.0.1",
            "MYSQL_PORT": "13306",
            "MYSQL_USER": "root",
            "MYSQL_PASSWORD": "rootpass",
            "MYSQL_DATABASE": "adapter_ate",
        }

        config = db_config_from_env(env)

        self.assertEqual("127.0.0.1", config["host"])
        self.assertEqual(13306, config["port"])
        self.assertEqual("adapter_ate", config["database"])

    def test_upsert_processed_dir_uses_on_duplicate_key_update(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed_dir = Path(tmp)
            create_processed_fixture(processed_dir)
            connection = FakeConnection()

            counts = upsert_processed_dir(processed_dir, connection)

            executed_sql = "\n".join(sql for sql, _ in connection.cursor_instance.executed)
            self.assertEqual(2, counts["product_test_runs"])
            self.assertEqual(1, counts["test_item_results"])
            self.assertEqual(1, counts["station_events"])
            self.assertEqual(1, counts["defect_reasons"])
            self.assertIn("ON DUPLICATE KEY UPDATE", executed_sql)
            item_params = [
                params for sql, params in connection.cursor_instance.executed
                if "INSERT INTO test_item_results" in sql
            ][0]
            self.assertEqual(1, item_params[10])
            self.assertEqual(1, connection.commit_count)

    def test_generate_reports_writes_yield_and_defect_csv_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed_dir = Path(tmp) / "processed"
            reports_dir = Path(tmp) / "reports"
            processed_dir.mkdir()
            create_processed_fixture(processed_dir)

            summary = generate_reports(processed_dir, reports_dir)

            self.assertEqual(2, summary["total_count"])
            self.assertEqual(50.0, summary["yield_rate"])
            self.assertTrue((reports_dir / "daily_summary.csv").exists())
            self.assertTrue((reports_dir / "batch_summary.csv").exists())
            self.assertTrue((reports_dir / "defect_summary.csv").exists())

            with (reports_dir / "defect_summary.csv").open("r", encoding="utf-8-sig", newline="") as file:
                defect_rows = list(csv.DictReader(file))
            self.assertEqual("PERFORMANCE", defect_rows[0]["station_id"])
            self.assertEqual("output_voltage", defect_rows[0]["item_name"])

    def test_query_product_trace_reads_related_mysql_rows(self):
        connection = QueryConnection([
            [{
                "run_id": "RUN-DB-001",
                "sn": "DB001",
                "batch_no": "BDB",
                "product_model": "ADP-65W",
                "line_id": "LINE-01",
                "equipment_id": "ATE-01",
                "start_time": "2026-04-25 10:00:00",
                "end_time": "2026-04-25 10:00:10",
                "final_result": "FAIL",
                "failure_codes": "OUTPUT_VOLTAGE_HIGH",
                "defect_count": 1,
                "simulated_sort_result": "FAIL",
            }],
            [{
                "item_id": "ITEM-DB-001",
                "run_id": "RUN-DB-001",
                "sn": "DB001",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "measured_value": 12.5,
                "lower_limit": 11.8,
                "upper_limit": 12.2,
                "unit": "V",
                "test_time": "2026-04-25 10:00:08",
                "required": 1,
                "item_result": "FAIL",
                "failure_code": "OUTPUT_VOLTAGE_HIGH",
                "failure_message": "output_voltage above upper limit",
            }],
            [{
                "event_id": "EVT-DB-001",
                "run_id": "RUN-DB-001",
                "sn": "DB001",
                "station_id": "SCAN",
                "event_type": "SCAN_OK",
                "event_message": "SN scanned",
                "event_time": "2026-04-25 10:00:00",
            }],
            [{
                "item_id": "ITEM-DB-001",
                "run_id": "RUN-DB-001",
                "sn": "DB001",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "failure_code": "OUTPUT_VOLTAGE_HIGH",
                "failure_message": "output_voltage above upper limit",
                "measured_value": 12.5,
                "lower_limit": 11.8,
                "upper_limit": 12.2,
                "unit": "V",
                "test_time": "2026-04-25 10:00:08",
            }],
        ])

        trace = query_product_trace(connection, "DB001")

        self.assertEqual("DB001", trace["product"]["sn"])
        self.assertEqual("FAIL", trace["product"]["final_result"])
        self.assertEqual("True", trace["items"][0]["required"])
        self.assertEqual("OUTPUT_VOLTAGE_HIGH", trace["defects"][0]["failure_code"])
        self.assertEqual(("DB001",), connection.cursor_instance.executed[0][1])

    def test_query_batch_yield_calculates_counts_from_mysql(self):
        connection = QueryConnection([[{"total_count": 2, "pass_count": 1, "fail_count": 1}]])

        result = query_batch_yield(connection, "BDB")

        self.assertEqual("BDB", result["batch_no"])
        self.assertEqual(2, result["total_count"])
        self.assertEqual(1, result["pass_count"])
        self.assertEqual(50.0, result["yield_rate"])
        self.assertEqual(("BDB",), connection.cursor_instance.executed[0][1])

    def test_query_defect_summary_groups_mysql_defects(self):
        connection = QueryConnection([[
            {
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "failure_code": "OUTPUT_VOLTAGE_HIGH",
                "defect_count": 2,
            }
        ]])

        result = query_defect_summary(connection)

        self.assertEqual("PERFORMANCE", result[0]["station_id"])
        self.assertEqual("OUTPUT_VOLTAGE_HIGH", result[0]["failure_code"])
        self.assertEqual(2, result[0]["defect_count"])

    def test_query_station_summary_calculates_mysql_station_counts(self):
        connection = QueryConnection([
            [{"tested_product_count": 2, "fail_item_count": 1}],
            [{"pass_count": 1}],
            [{"defect_count": 1}],
        ])

        result = query_station_summary(connection, "PERFORMANCE")

        self.assertEqual("PERFORMANCE", result["station_id"])
        self.assertEqual(2, result["tested_product_count"])
        self.assertEqual(1, result["pass_count"])
        self.assertEqual(1, result["fail_item_count"])
        self.assertEqual(1, result["defect_count"])
        self.assertEqual(50.0, result["yield_rate"])


if __name__ == "__main__":
    unittest.main()
