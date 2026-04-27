import csv
import subprocess
import tempfile
import unittest
from pathlib import Path


class CppSimulatorTests(unittest.TestCase):
    def test_simulator_generates_three_stable_raw_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            binary = base / "ate_line_simulator"
            source = Path("cpp_ate_simulator/ate_line_simulator.cpp")

            subprocess.run(["g++", "-std=c++17", "-Wall", "-Wextra", str(source), "-o", str(binary)], check=True)

            first = base / "first"
            second = base / "second"
            for output_dir in (first, second):
                subprocess.run(
                    [
                        str(binary),
                        "--count",
                        "5",
                        "--output-dir",
                        str(output_dir),
                        "--seed",
                        "42",
                        "--abnormal-rate",
                        "0.2",
                        "--batch-no",
                        "B20260425",
                        "--product-model",
                        "ADP-65W",
                        "--line-id",
                        "LINE-01",
                    ],
                    check=True,
                )

            for name in ("raw_products.csv", "raw_test_items.csv", "raw_station_events.csv"):
                self.assertTrue((first / name).exists(), name)
                self.assertEqual((first / name).read_text(), (second / name).read_text())

            with (first / "raw_products.csv").open("r", encoding="utf-8", newline="") as file:
                products = list(csv.DictReader(file))
            with (first / "raw_test_items.csv").open("r", encoding="utf-8", newline="") as file:
                items = list(csv.DictReader(file))
            with (first / "raw_station_events.csv").open("r", encoding="utf-8", newline="") as file:
                events = list(csv.DictReader(file))

            self.assertEqual(5, len(products))
            self.assertIn("item_id", items[0])
            self.assertIn("event_id", events[0])
            self.assertTrue({"SCAN", "HIPOT", "PERFORMANCE", "SORT"}.issubset({row["station_id"] for row in events}))


if __name__ == "__main__":
    unittest.main()
