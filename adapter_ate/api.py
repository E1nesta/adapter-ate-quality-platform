import argparse
import csv
import os
from collections import Counter
from pathlib import Path

from flask import Flask, jsonify, request

from .ai_model import predict_if_available
from .processor import ConfigError, RawDataError, process_raw_files
from .reports import generate_reports, yield_rate
from .storage import (
    connect,
    create_schema,
    db_config_from_env,
    query_batch_yield,
    query_defect_summary,
    query_product_trace,
    query_station_summary,
    upsert_processed_dir,
)


VALID_DATA_SOURCES = {"auto", "mysql", "csv"}


class DataSourceError(RuntimeError):
    pass


def read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def load_dataset(processed_dir):
    processed_dir = Path(processed_dir)
    return {
        "products": read_csv(processed_dir / "processed_results.csv"),
        "items": read_csv(processed_dir / "item_results.csv"),
        "events": read_csv(processed_dir / "station_events.csv"),
        "defects": read_csv(processed_dir / "defects.csv"),
    }


def json_error(error, message, status_code):
    response = jsonify({"error": error, "message": message})
    response.status_code = status_code
    return response


def request_payload():
    if not request.data:
        return {}
    if not request.is_json:
        return None
    return request.get_json(silent=True) or {}


def normalize_data_source(value):
    data_source = (value or "auto").strip().lower()
    if data_source not in VALID_DATA_SOURCES:
        return "auto"
    return data_source


def check_database_available():
    connection = None
    try:
        connection = connect()
        return True
    except Exception:
        return False
    finally:
        if connection is not None:
            connection.close()


def int_value(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def csv_product_trace(processed_dir, sn):
    data = load_dataset(processed_dir)
    product = next((row for row in data["products"] if row["sn"] == sn), None)
    return {
        "product": product,
        "items": [row for row in data["items"] if row["sn"] == sn],
        "events": [row for row in data["events"] if row["sn"] == sn],
        "defects": [row for row in data["defects"] if row["sn"] == sn],
    }


def csv_batch_yield(processed_dir, batch_no):
    products = [
        row for row in load_dataset(processed_dir)["products"]
        if row["batch_no"] == batch_no
    ]
    if not products:
        return None

    pass_count = sum(1 for row in products if row["final_result"] == "PASS")
    fail_count = sum(1 for row in products if row["final_result"] == "FAIL")
    return {
        "batch_no": batch_no,
        "total_count": len(products),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "yield_rate": yield_rate(pass_count, len(products)),
    }


def csv_defect_summary(processed_dir):
    defect_rows = load_dataset(processed_dir)["defects"]
    counter = Counter(
        (row["station_id"], row["item_name"], row["failure_code"])
        for row in defect_rows
    )
    return [
        {
            "station_id": station_id,
            "item_name": item_name,
            "failure_code": failure_code,
            "defect_count": count,
        }
        for (station_id, item_name, failure_code), count in sorted(counter.items())
    ]


def csv_station_summary(processed_dir, station_id):
    data = load_dataset(processed_dir)
    station_items = [row for row in data["items"] if row["station_id"] == station_id]
    if not station_items:
        return None

    fail_items = [row for row in station_items if row["item_result"] == "FAIL"]
    affected_runs = {row["run_id"] for row in station_items}
    affected_products = [row for row in data["products"] if row["run_id"] in affected_runs]
    pass_count = sum(1 for row in affected_products if row["final_result"] == "PASS")
    station_defects = [row for row in data["defects"] if row["station_id"] == station_id]
    return {
        "station_id": station_id,
        "tested_product_count": len(affected_products),
        "pass_count": pass_count,
        "fail_item_count": len(fail_items),
        "defect_count": len(station_defects),
        "yield_rate": yield_rate(pass_count, len(affected_products)),
    }


def mysql_query(query_func, *args):
    connection = None
    try:
        connection = connect()
        return query_func(connection, *args)
    finally:
        if connection is not None:
            connection.close()


def query_with_data_source(app, mysql_func, csv_func, *args):
    data_source = app.config["DATA_SOURCE_MODE"]
    if data_source == "csv":
        return csv_func(app.config["PROCESSED_DIR"], *args)

    try:
        return mysql_query(mysql_func, *args)
    except Exception as exc:
        if data_source == "auto":
            return csv_func(app.config["PROCESSED_DIR"], *args)
        raise DataSourceError(str(exc)) from exc


def create_app(
    raw_dir="data/raw",
    config_path="config/test_rules.json",
    processed_dir="data/processed",
    reports_dir="reports",
    model_path="models/quality_model.joblib",
    log_path="reports/process.log",
    data_source=None,
):
    app = Flask(__name__)
    app.config["RAW_DIR"] = str(raw_dir)
    app.config["CONFIG_PATH"] = str(config_path)
    app.config["PROCESSED_DIR"] = str(processed_dir)
    app.config["REPORTS_DIR"] = str(reports_dir)
    app.config["MODEL_PATH"] = str(model_path)
    app.config["LOG_PATH"] = str(log_path)
    app.config["DATA_SOURCE_MODE"] = normalize_data_source(
        data_source or os.environ.get("ATE_DATA_SOURCE", "auto")
    )

    @app.errorhandler(404)
    def not_found(_error):
        return json_error("not_found", "resource not found", 404)

    @app.errorhandler(500)
    def server_error(_error):
        return json_error("server_error", "internal server error", 500)

    @app.get("/api/health")
    def health():
        config = db_config_from_env()
        data_source_mode = app.config["DATA_SOURCE_MODE"]
        mysql_available = False if data_source_mode == "csv" else check_database_available()
        return jsonify({
            "status": "ok",
            "database": {
                "host": config["host"],
                "port": config["port"],
                "database": config["database"],
                "configured": bool(config["user"] and config["database"]),
            },
            "data_source": {
                "mode": data_source_mode,
                "mysql_available": mysql_available,
            },
        })

    @app.post("/api/process")
    def process_batch():
        payload = request_payload()
        if payload is None:
            return json_error("invalid_json", "request body must be JSON", 400)

        try:
            summary = process_raw_files(
                payload.get("raw_dir", app.config["RAW_DIR"]),
                payload.get("config", app.config["CONFIG_PATH"]),
                payload.get("output_dir", app.config["PROCESSED_DIR"]),
                payload.get("log", app.config["LOG_PATH"]),
            )
        except RawDataError as exc:
            return json_error("raw_data_error", str(exc), 400)
        except ConfigError as exc:
            return json_error("config_error", str(exc), 400)

        return jsonify({
            "total_count": summary.total_count,
            "pass_count": summary.pass_count,
            "fail_count": summary.fail_count,
            "defect_count": summary.defect_count,
            "output_dir": str(summary.output_dir),
        })

    @app.post("/api/reports/generate")
    def reports_generate():
        payload = request_payload()
        if payload is None:
            return json_error("invalid_json", "request body must be JSON", 400)

        processed_path = Path(payload.get("processed_dir", app.config["PROCESSED_DIR"]))
        reports_path = Path(payload.get("reports_dir", app.config["REPORTS_DIR"]))
        try:
            summary = generate_reports(processed_path, reports_path)
        except (FileNotFoundError, KeyError, ValueError) as exc:
            return json_error("report_generation_failed", str(exc), 400)

        return jsonify({
            "summary": summary,
            "reports": {
                "daily_summary": str(reports_path / "daily_summary.csv"),
                "batch_summary": str(reports_path / "batch_summary.csv"),
                "defect_summary": str(reports_path / "defect_summary.csv"),
            },
        })

    @app.post("/api/storage/import")
    def storage_import():
        payload = request_payload()
        if payload is None:
            return json_error("invalid_json", "request body must be JSON", 400)

        processed_path = payload.get("processed_dir", app.config["PROCESSED_DIR"])
        should_create_schema = bool(payload.get("create_schema", True))
        connection = None
        try:
            connection = connect()
            if should_create_schema:
                create_schema(connection)
            counts = upsert_processed_dir(processed_path, connection)
        except Exception as exc:
            return json_error("storage_import_failed", str(exc), 503)
        finally:
            if connection is not None:
                connection.close()

        return jsonify({"counts": counts})

    @app.get("/api/products/<sn>")
    def product_trace(sn):
        try:
            trace = query_with_data_source(app, query_product_trace, csv_product_trace, sn)
        except DataSourceError as exc:
            return json_error("data_source_error", str(exc), 503)

        if trace["product"] is None:
            return json_error("not_found", f"SN not found: {sn}", 404)

        return jsonify(trace)

    @app.get("/api/batches/<batch_no>/yield")
    def batch_yield(batch_no):
        try:
            result = query_with_data_source(app, query_batch_yield, csv_batch_yield, batch_no)
        except DataSourceError as exc:
            return json_error("data_source_error", str(exc), 503)

        if result is None:
            return json_error("not_found", f"batch not found: {batch_no}", 404)

        return jsonify(result)

    @app.get("/api/defects")
    def defects():
        try:
            defect_rows = query_with_data_source(app, query_defect_summary, csv_defect_summary)
        except DataSourceError as exc:
            return json_error("data_source_error", str(exc), 503)

        return jsonify({"defects": defect_rows})

    @app.get("/api/stations/<station_id>/summary")
    def station_summary(station_id):
        try:
            result = query_with_data_source(app, query_station_summary, csv_station_summary, station_id)
        except DataSourceError as exc:
            return json_error("data_source_error", str(exc), 503)

        if result is None:
            return json_error("not_found", f"station not found: {station_id}", 404)

        return jsonify(result)

    @app.post("/api/predict")
    def predict_placeholder():
        if not request.is_json:
            return json_error("invalid_json", "request body must be JSON", 400)
        prediction = predict_if_available(app.config["MODEL_PATH"], request.get_json())
        if not prediction["available"]:
            return json_error("model_unavailable", "AI model is not trained yet", 503)
        return jsonify(prediction)

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Run adapter ATE query API")
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--model", default="models/quality_model.joblib")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    return parser.parse_args()


def main():
    args = parse_args()
    app = create_app(
        processed_dir=args.processed_dir,
        reports_dir=args.reports_dir,
        model_path=args.model,
    )
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
