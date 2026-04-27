import argparse
import csv
from pathlib import Path

import pandas as pd

from .contracts import BATCH_SUMMARY_FIELDS, DAILY_SUMMARY_FIELDS, DEFECT_SUMMARY_FIELDS


def write_csv(path, fieldnames, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def yield_rate(pass_count, total_count):
    if total_count == 0:
        return 0.0
    return round(pass_count / total_count * 100, 2)


def generate_reports(processed_dir, reports_dir):
    processed_dir = Path(processed_dir)
    reports_dir = Path(reports_dir)
    products = pd.read_csv(processed_dir / "processed_results.csv", keep_default_na=False)
    defects = pd.read_csv(processed_dir / "defects.csv", keep_default_na=False)

    total_count = int(len(products))
    pass_count = int((products["final_result"] == "PASS").sum())
    fail_count = int((products["final_result"] == "FAIL").sum())
    summary = {
        "total_count": total_count,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "yield_rate": yield_rate(pass_count, total_count),
    }

    products = products.assign(
        date=pd.to_datetime(products["start_time"]).dt.date.astype(str),
        pass_flag=(products["final_result"] == "PASS").astype(int),
        fail_flag=(products["final_result"] == "FAIL").astype(int),
    )

    daily = (
        products.groupby("date", as_index=False)
        .agg(total_count=("run_id", "count"), pass_count=("pass_flag", "sum"), fail_count=("fail_flag", "sum"))
        .sort_values("date")
    )
    daily["yield_rate"] = (daily["pass_count"] / daily["total_count"] * 100).round(2)

    batch = (
        products.groupby(["batch_no", "product_model"], as_index=False)
        .agg(total_count=("run_id", "count"), pass_count=("pass_flag", "sum"), fail_count=("fail_flag", "sum"))
        .sort_values(["batch_no", "product_model"])
    )
    batch["yield_rate"] = (batch["pass_count"] / batch["total_count"] * 100).round(2)

    if defects.empty:
        defect = pd.DataFrame(columns=DEFECT_SUMMARY_FIELDS)
    else:
        defect = (
            defects.groupby(["station_id", "item_name", "failure_code"], as_index=False)
            .size()
            .rename(columns={"size": "defect_count"})
            .sort_values(["station_id", "item_name", "failure_code"])
        )

    write_csv(reports_dir / "daily_summary.csv", DAILY_SUMMARY_FIELDS, daily[DAILY_SUMMARY_FIELDS].to_dict("records"))
    write_csv(reports_dir / "batch_summary.csv", BATCH_SUMMARY_FIELDS, batch[BATCH_SUMMARY_FIELDS].to_dict("records"))
    write_csv(reports_dir / "defect_summary.csv", DEFECT_SUMMARY_FIELDS, defect[DEFECT_SUMMARY_FIELDS].to_dict("records"))
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="Generate adapter ATE yield reports")
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--reports-dir", default="reports")
    return parser.parse_args()


def main():
    args = parse_args()
    summary = generate_reports(args.processed_dir, args.reports_dir)
    print(
        "reported "
        f"total={summary['total_count']} pass={summary['pass_count']} "
        f"fail={summary['fail_count']} yield={summary['yield_rate']}%"
    )


if __name__ == "__main__":
    main()
