import csv
import json
import tempfile
import unittest
from pathlib import Path

from adapter_ate.processor import (
    RawDataError,
    build_traceability_index,
    judge_item,
    load_rules,
    process_raw_files,
)


def write_csv(path, fieldnames, rows):
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_rules(path):
    rules = {
        "items": {
            "hipot_leakage_current": {
                "station_id": "HIPOT",
                "lower_limit": 0.0,
                "upper_limit": 0.5,
                "unit": "mA",
                "required": True,
                "failure_code_low": "HIPOT_LEAKAGE_CURRENT_LOW",
                "failure_code_high": "HIPOT_LEAKAGE_CURRENT_HIGH",
            },
            "output_voltage": {
                "station_id": "PERFORMANCE",
                "lower_limit": 11.8,
                "upper_limit": 12.2,
                "unit": "V",
                "required": True,
                "failure_code_low": "OUTPUT_VOLTAGE_LOW",
                "failure_code_high": "OUTPUT_VOLTAGE_HIGH",
            },
        }
    }
    path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")


class ProcessorCoreTests(unittest.TestCase):
    def test_judge_item_uses_configured_limits_and_failure_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            rules_path = Path(tmp) / "rules.json"
            write_rules(rules_path)
            rules = load_rules(rules_path)

            item = {
                "item_id": "ITEM-001",
                "run_id": "RUN-001",
                "sn": "SN001",
                "station_id": "PERFORMANCE",
                "item_name": "output_voltage",
                "measured_value": "12.50",
            }

            judged = judge_item(item, rules)

            self.assertEqual("FAIL", judged["item_result"])
            self.assertEqual("OUTPUT_VOLTAGE_HIGH", judged["failure_code"])

    def test_missing_required_raw_file_raises_clear_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp) / "raw"
            raw_dir.mkdir()
            config_path = Path(tmp) / "rules.json"
            output_dir = Path(tmp) / "processed"
            write_rules(config_path)
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
                [],
            )

            with self.assertRaisesRegex(RawDataError, "raw_test_items.csv"):
                process_raw_files(raw_dir, config_path, output_dir)

    def test_process_raw_files_writes_product_results_and_defects(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw_dir = base / "raw"
            output_dir = base / "processed"
            raw_dir.mkdir()
            config_path = base / "rules.json"
            write_rules(config_path)

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
                        "batch_no": "B20260425",
                        "product_model": "ADP-65W",
                        "line_id": "LINE-01",
                        "equipment_id": "ATE-01",
                        "start_time": "2026-04-25 10:00:00",
                        "end_time": "2026-04-25 10:00:10",
                        "simulated_sort_result": "PASS",
                    },
                    {
                        "run_id": "RUN-002",
                        "sn": "SN002",
                        "batch_no": "B20260425",
                        "product_model": "ADP-65W",
                        "line_id": "LINE-01",
                        "equipment_id": "ATE-01",
                        "start_time": "2026-04-25 10:01:00",
                        "end_time": "2026-04-25 10:01:10",
                        "simulated_sort_result": "FAIL",
                    },
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
                        "station_id": "HIPOT",
                        "item_name": "hipot_leakage_current",
                        "measured_value": "0.20",
                        "lower_limit": "0",
                        "upper_limit": "0.5",
                        "unit": "mA",
                        "test_time": "2026-04-25 10:00:03",
                    },
                    {
                        "item_id": "ITEM-002",
                        "run_id": "RUN-001",
                        "sn": "SN001",
                        "station_id": "PERFORMANCE",
                        "item_name": "output_voltage",
                        "measured_value": "12.00",
                        "lower_limit": "11.8",
                        "upper_limit": "12.2",
                        "unit": "V",
                        "test_time": "2026-04-25 10:00:08",
                    },
                    {
                        "item_id": "ITEM-003",
                        "run_id": "RUN-002",
                        "sn": "SN002",
                        "station_id": "HIPOT",
                        "item_name": "hipot_leakage_current",
                        "measured_value": "0.20",
                        "lower_limit": "0",
                        "upper_limit": "0.5",
                        "unit": "mA",
                        "test_time": "2026-04-25 10:01:03",
                    },
                    {
                        "item_id": "ITEM-004",
                        "run_id": "RUN-002",
                        "sn": "SN002",
                        "station_id": "PERFORMANCE",
                        "item_name": "output_voltage",
                        "measured_value": "12.50",
                        "lower_limit": "11.8",
                        "upper_limit": "12.2",
                        "unit": "V",
                        "test_time": "2026-04-25 10:01:08",
                    },
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

            summary = process_raw_files(raw_dir, config_path, output_dir)

            self.assertEqual(2, summary.total_count)
            self.assertEqual(1, summary.pass_count)
            self.assertEqual(1, summary.fail_count)

            with (output_dir / "processed_results.csv").open("r", encoding="utf-8-sig", newline="") as file:
                results = list(csv.DictReader(file))
            with (output_dir / "defects.csv").open("r", encoding="utf-8-sig", newline="") as file:
                defects = list(csv.DictReader(file))

            self.assertEqual(["PASS", "FAIL"], [row["final_result"] for row in results])
            self.assertEqual(1, len(defects))
            self.assertEqual("OUTPUT_VOLTAGE_HIGH", defects[0]["failure_code"])

    def test_traceability_index_links_product_items_events_and_defects(self):
        products = [{"run_id": "RUN-001", "sn": "SN001", "final_result": "FAIL"}]
        items = [{"item_id": "ITEM-001", "run_id": "RUN-001", "sn": "SN001"}]
        events = [{"event_id": "EVT-001", "run_id": "RUN-001", "sn": "SN001"}]
        defects = [{"item_id": "ITEM-001", "run_id": "RUN-001", "sn": "SN001"}]

        index = build_traceability_index(products, items, events, defects)

        self.assertEqual("FAIL", index["SN001"]["product"]["final_result"])
        self.assertEqual(items, index["SN001"]["items"])
        self.assertEqual(events, index["SN001"]["events"])
        self.assertEqual(defects, index["SN001"]["defects"])


if __name__ == "__main__":
    unittest.main()
