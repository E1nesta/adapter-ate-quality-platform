import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .contracts import (
    DEFECT_FIELDS,
    ITEM_RESULT_FIELDS,
    PROCESSED_PRODUCT_FIELDS,
    RAW_PRODUCTS_FIELDS,
    RAW_STATION_EVENTS_FIELDS,
    RAW_TEST_ITEMS_FIELDS,
)


class RawDataError(ValueError):
    """Raised when raw ATE files are missing or malformed."""


class ConfigError(ValueError):
    """Raised when judgement configuration is missing or malformed."""


@dataclass(frozen=True)
class ProcessSummary:
    total_count: int
    pass_count: int
    fail_count: int
    defect_count: int
    output_dir: Path


def load_rules(config_path):
    path = Path(config_path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"rule config not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid rule config JSON: {path}") from exc

    items = data.get("items")
    if not isinstance(items, dict) or not items:
        raise ConfigError("rule config must contain a non-empty 'items' object")

    for item_name, rule in items.items():
        for field in ("lower_limit", "upper_limit", "unit", "required"):
            if field not in rule:
                raise ConfigError(f"rule '{item_name}' missing field: {field}")
        if "failure_code_low" not in rule or "failure_code_high" not in rule:
            raise ConfigError(f"rule '{item_name}' missing failure code fields")

    return data


def validate_csv(path, required_fields):
    if not path.exists():
        raise RawDataError(f"required raw file missing: {path.name}")

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        actual_fields = reader.fieldnames or []

    missing = [field for field in required_fields if field not in actual_fields]
    if missing:
        raise RawDataError(f"{path.name} missing columns: {', '.join(missing)}")


def read_csv(path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def write_csv(path, fieldnames, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def judge_item(item_row, rules):
    item_name = item_row["item_name"]
    rule = rules["items"].get(item_name)
    if rule is None:
        raise RawDataError(f"no judgement rule for item: {item_name}")

    measured_value = float(item_row["measured_value"])
    lower_limit = float(rule["lower_limit"])
    upper_limit = float(rule["upper_limit"])
    required = bool(rule.get("required", True))

    result = "PASS"
    failure_code = ""
    failure_message = ""
    if measured_value < lower_limit:
        result = "FAIL"
        failure_code = rule["failure_code_low"]
        failure_message = f"{item_name} below lower limit"
    elif measured_value > upper_limit:
        result = "FAIL"
        failure_code = rule["failure_code_high"]
        failure_message = f"{item_name} above upper limit"

    return {
        **item_row,
        "lower_limit": lower_limit,
        "upper_limit": upper_limit,
        "unit": rule["unit"],
        "required": required,
        "item_result": result,
        "failure_code": failure_code,
        "failure_message": failure_message,
    }


def build_defect_row(judged_item):
    return {
        "item_id": judged_item["item_id"],
        "run_id": judged_item["run_id"],
        "sn": judged_item["sn"],
        "station_id": judged_item["station_id"],
        "item_name": judged_item["item_name"],
        "failure_code": judged_item["failure_code"],
        "failure_message": judged_item["failure_message"],
        "measured_value": judged_item["measured_value"],
        "lower_limit": judged_item["lower_limit"],
        "upper_limit": judged_item["upper_limit"],
        "unit": judged_item["unit"],
        "test_time": judged_item["test_time"],
    }


def build_product_result(product_row, judged_items):
    required_failures = [
        item for item in judged_items
        if item["required"] and item["item_result"] == "FAIL"
    ]
    final_result = "FAIL" if required_failures else "PASS"
    failure_codes = ",".join(item["failure_code"] for item in required_failures)

    return {
        "run_id": product_row["run_id"],
        "sn": product_row["sn"],
        "batch_no": product_row["batch_no"],
        "product_model": product_row["product_model"],
        "line_id": product_row["line_id"],
        "equipment_id": product_row["equipment_id"],
        "start_time": product_row["start_time"],
        "end_time": product_row["end_time"],
        "final_result": final_result,
        "failure_codes": failure_codes,
        "defect_count": len(required_failures),
        "simulated_sort_result": product_row.get("simulated_sort_result", ""),
    }


def build_traceability_index(products, items, events, defects):
    traces = {}
    for product in products:
        traces[product["sn"]] = {
            "product": product,
            "items": [],
            "events": [],
            "defects": [],
        }

    for collection_name, rows in (
        ("items", items),
        ("events", events),
        ("defects", defects),
    ):
        for row in rows:
            trace = traces.setdefault(
                row["sn"],
                {
                    "product": None,
                    "items": [],
                    "events": [],
                    "defects": [],
                },
            )
            trace[collection_name].append(row)

    return traces


def process_raw_files(raw_dir, config_path, output_dir, log_path=None):
    raw_dir = Path(raw_dir)
    output_dir = Path(output_dir)
    rules = load_rules(config_path)

    products_path = raw_dir / "raw_products.csv"
    items_path = raw_dir / "raw_test_items.csv"
    events_path = raw_dir / "raw_station_events.csv"

    validate_csv(products_path, RAW_PRODUCTS_FIELDS)
    validate_csv(items_path, RAW_TEST_ITEMS_FIELDS)
    validate_csv(events_path, RAW_STATION_EVENTS_FIELDS)

    products = read_csv(products_path)
    raw_items = read_csv(items_path)
    events = read_csv(events_path)

    judged_items = [judge_item(item, rules) for item in raw_items]
    items_by_run = defaultdict(list)
    for item in judged_items:
        items_by_run[item["run_id"]].append(item)

    product_results = [
        build_product_result(product, items_by_run[product["run_id"]])
        for product in products
    ]
    defects = [
        build_defect_row(item)
        for item in judged_items
        if item["required"] and item["item_result"] == "FAIL"
    ]

    write_csv(output_dir / "processed_results.csv", PROCESSED_PRODUCT_FIELDS, product_results)
    write_csv(output_dir / "item_results.csv", ITEM_RESULT_FIELDS, judged_items)
    write_csv(output_dir / "defects.csv", DEFECT_FIELDS, defects)
    write_csv(output_dir / "station_events.csv", RAW_STATION_EVENTS_FIELDS, events)
    traceability = build_traceability_index(product_results, judged_items, events, defects)
    (output_dir / "traceability.json").write_text(
        json.dumps(traceability, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    pass_count = sum(1 for row in product_results if row["final_result"] == "PASS")
    fail_count = sum(1 for row in product_results if row["final_result"] == "FAIL")
    summary = ProcessSummary(
        total_count=len(product_results),
        pass_count=pass_count,
        fail_count=fail_count,
        defect_count=len(defects),
        output_dir=output_dir,
    )

    if log_path is not None:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        Path(log_path).write_text(
            "\n".join([
                f"raw_dir={raw_dir}",
                f"output_dir={output_dir}",
                f"total_count={summary.total_count}",
                f"pass_count={summary.pass_count}",
                f"fail_count={summary.fail_count}",
                f"defect_count={summary.defect_count}",
            ]) + "\n",
            encoding="utf-8",
        )

    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="Process adapter ATE raw CSV files")
    parser.add_argument("--raw-dir", default="data/raw", help="Directory containing raw CSV files")
    parser.add_argument("--config", default="config/test_rules.json", help="Judgement rule JSON file")
    parser.add_argument("--output-dir", default="data/processed", help="Directory for processed CSV files")
    parser.add_argument("--log", default="reports/process.log", help="Processing log file")
    return parser.parse_args()


def main():
    args = parse_args()
    summary = process_raw_files(args.raw_dir, args.config, args.output_dir, args.log)
    print(
        "processed "
        f"total={summary.total_count} pass={summary.pass_count} "
        f"fail={summary.fail_count} defects={summary.defect_count}"
    )


if __name__ == "__main__":
    main()
