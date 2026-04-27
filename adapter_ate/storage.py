import argparse
import csv
import os
from pathlib import Path

import pymysql


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS product_test_runs (
  run_id VARCHAR(32) NOT NULL,
  sn VARCHAR(64) NOT NULL,
  batch_no VARCHAR(64) NOT NULL,
  product_model VARCHAR(64) NOT NULL,
  line_id VARCHAR(64) NOT NULL,
  equipment_id VARCHAR(64) NOT NULL,
  start_time DATETIME NOT NULL,
  end_time DATETIME NOT NULL,
  final_result VARCHAR(8) NOT NULL,
  failure_codes TEXT NOT NULL,
  defect_count INT NOT NULL,
  simulated_sort_result VARCHAR(8) NOT NULL,
  PRIMARY KEY (run_id),
  KEY idx_product_sn (sn),
  KEY idx_product_batch (batch_no)
);

CREATE TABLE IF NOT EXISTS test_item_results (
  item_id VARCHAR(32) NOT NULL,
  run_id VARCHAR(32) NOT NULL,
  sn VARCHAR(64) NOT NULL,
  station_id VARCHAR(64) NOT NULL,
  item_name VARCHAR(128) NOT NULL,
  measured_value DOUBLE NOT NULL,
  lower_limit DOUBLE NOT NULL,
  upper_limit DOUBLE NOT NULL,
  unit VARCHAR(32) NOT NULL,
  test_time DATETIME NOT NULL,
  required_flag BOOLEAN NOT NULL,
  item_result VARCHAR(8) NOT NULL,
  failure_code VARCHAR(128) NOT NULL,
  failure_message TEXT NOT NULL,
  PRIMARY KEY (item_id),
  KEY idx_item_run (run_id),
  KEY idx_item_sn (sn)
);

CREATE TABLE IF NOT EXISTS station_events (
  event_id VARCHAR(32) NOT NULL,
  run_id VARCHAR(32) NOT NULL,
  sn VARCHAR(64) NOT NULL,
  station_id VARCHAR(64) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  event_message TEXT NOT NULL,
  event_time DATETIME NOT NULL,
  PRIMARY KEY (event_id),
  KEY idx_event_run (run_id),
  KEY idx_event_sn (sn)
);

CREATE TABLE IF NOT EXISTS defect_reasons (
  item_id VARCHAR(32) NOT NULL,
  run_id VARCHAR(32) NOT NULL,
  sn VARCHAR(64) NOT NULL,
  station_id VARCHAR(64) NOT NULL,
  item_name VARCHAR(128) NOT NULL,
  failure_code VARCHAR(128) NOT NULL,
  failure_message TEXT NOT NULL,
  measured_value DOUBLE NOT NULL,
  lower_limit DOUBLE NOT NULL,
  upper_limit DOUBLE NOT NULL,
  unit VARCHAR(32) NOT NULL,
  test_time DATETIME NOT NULL,
  PRIMARY KEY (item_id, failure_code),
  KEY idx_defect_run (run_id),
  KEY idx_defect_sn (sn)
);

CREATE TABLE IF NOT EXISTS ai_predictions (
  prediction_id BIGINT NOT NULL AUTO_INCREMENT,
  run_id VARCHAR(32) NOT NULL,
  sn VARCHAR(64) NOT NULL,
  predicted_result VARCHAR(8) NOT NULL,
  confidence DOUBLE NOT NULL,
  model_name VARCHAR(128) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (prediction_id),
  KEY idx_prediction_run (run_id),
  KEY idx_prediction_sn (sn)
);
"""


def read_csv(path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def db_config_from_env(env=os.environ):
    return {
        "host": env.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(env.get("MYSQL_PORT", "3306")),
        "user": env.get("MYSQL_USER", "root"),
        "password": env.get("MYSQL_PASSWORD", ""),
        "database": env.get("MYSQL_DATABASE", "adapter_ate"),
        "charset": "utf8mb4",
        "autocommit": False,
    }


def connect(config=None):
    return pymysql.connect(**(config or db_config_from_env()))


def normalize_db_value(key, value):
    if value is None:
        return ""
    if key == "required":
        return "True" if bool_to_int(value) else "False"
    return str(value)


def normalize_db_row(row):
    return {key: normalize_db_value(key, value) for key, value in row.items()}


def int_count(value):
    if value is None:
        return 0
    return int(value)


def query_yield_rate(pass_count, total_count):
    if total_count == 0:
        return 0.0
    return round(pass_count / total_count * 100, 2)


def query_product_trace(connection, sn):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            """
            SELECT run_id, sn, batch_no, product_model, line_id, equipment_id,
                   start_time, end_time, final_result, failure_codes,
                   defect_count, simulated_sort_result
            FROM product_test_runs
            WHERE sn = %s
            LIMIT 1
            """,
            (sn,),
        )
        product = cursor.fetchone()
        if product is None:
            return {"product": None, "items": [], "events": [], "defects": []}

        cursor.execute(
            """
            SELECT item_id, run_id, sn, station_id, item_name, measured_value,
                   lower_limit, upper_limit, unit, test_time,
                   required_flag AS required, item_result, failure_code,
                   failure_message
            FROM test_item_results
            WHERE sn = %s
            ORDER BY test_time, item_id
            """,
            (sn,),
        )
        items = cursor.fetchall()

        cursor.execute(
            """
            SELECT event_id, run_id, sn, station_id, event_type,
                   event_message, event_time
            FROM station_events
            WHERE sn = %s
            ORDER BY event_time, event_id
            """,
            (sn,),
        )
        events = cursor.fetchall()

        cursor.execute(
            """
            SELECT item_id, run_id, sn, station_id, item_name, failure_code,
                   failure_message, measured_value, lower_limit, upper_limit,
                   unit, test_time
            FROM defect_reasons
            WHERE sn = %s
            ORDER BY test_time, item_id
            """,
            (sn,),
        )
        defects = cursor.fetchall()

    return {
        "product": normalize_db_row(product),
        "items": [normalize_db_row(row) for row in items],
        "events": [normalize_db_row(row) for row in events],
        "defects": [normalize_db_row(row) for row in defects],
    }


def query_batch_yield(connection, batch_no):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS total_count,
                   SUM(final_result = 'PASS') AS pass_count,
                   SUM(final_result = 'FAIL') AS fail_count
            FROM product_test_runs
            WHERE batch_no = %s
            """,
            (batch_no,),
        )
        row = cursor.fetchone() or {}

    total_count = int_count(row.get("total_count"))
    if total_count == 0:
        return None
    pass_count = int_count(row.get("pass_count"))
    fail_count = int_count(row.get("fail_count"))
    return {
        "batch_no": batch_no,
        "total_count": total_count,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "yield_rate": query_yield_rate(pass_count, total_count),
    }


def query_defect_summary(connection):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            """
            SELECT station_id, item_name, failure_code, COUNT(*) AS defect_count
            FROM defect_reasons
            GROUP BY station_id, item_name, failure_code
            ORDER BY station_id, item_name, failure_code
            """
        )
        rows = cursor.fetchall()

    return [
        {
            "station_id": row["station_id"],
            "item_name": row["item_name"],
            "failure_code": row["failure_code"],
            "defect_count": int_count(row["defect_count"]),
        }
        for row in rows
    ]


def query_station_summary(connection, station_id):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            """
            SELECT COUNT(DISTINCT run_id) AS tested_product_count,
                   SUM(item_result = 'FAIL') AS fail_item_count
            FROM test_item_results
            WHERE station_id = %s
            """,
            (station_id,),
        )
        station_row = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT COUNT(DISTINCT product_test_runs.run_id) AS pass_count
            FROM product_test_runs
            JOIN test_item_results
              ON product_test_runs.run_id = test_item_results.run_id
            WHERE test_item_results.station_id = %s
              AND product_test_runs.final_result = 'PASS'
            """,
            (station_id,),
        )
        pass_row = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT COUNT(*) AS defect_count
            FROM defect_reasons
            WHERE station_id = %s
            """,
            (station_id,),
        )
        defect_row = cursor.fetchone() or {}

    tested_product_count = int_count(station_row.get("tested_product_count"))
    if tested_product_count == 0:
        return None
    pass_count = int_count(pass_row.get("pass_count"))
    fail_item_count = int_count(station_row.get("fail_item_count"))
    defect_count = int_count(defect_row.get("defect_count"))
    return {
        "station_id": station_id,
        "tested_product_count": tested_product_count,
        "pass_count": pass_count,
        "fail_item_count": fail_item_count,
        "defect_count": defect_count,
        "yield_rate": query_yield_rate(pass_count, tested_product_count),
    }


def create_schema(connection):
    statements = [statement.strip() for statement in CREATE_TABLES_SQL.split(";") if statement.strip()]
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    connection.commit()


def upsert_rows(cursor, table_name, rows, columns):
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)
    update_list = ", ".join(f"{column}=VALUES({column})" for column in columns)
    sql = (
        f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_list}"
    )
    for row in rows:
        cursor.execute(sql, [row[column] for column in columns])
    return len(rows)


def bool_to_int(value):
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "y"} else 0


def upsert_processed_dir(processed_dir, connection):
    processed_dir = Path(processed_dir)
    products = read_csv(processed_dir / "processed_results.csv")
    items = read_csv(processed_dir / "item_results.csv")
    events = read_csv(processed_dir / "station_events.csv")
    defects = read_csv(processed_dir / "defects.csv")

    item_rows = [
        {**row, "required_flag": bool_to_int(row.pop("required"))}
        for row in items
    ]

    counts = {}
    with connection.cursor() as cursor:
        counts["product_test_runs"] = upsert_rows(
            cursor,
            "product_test_runs",
            products,
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
        )
        counts["test_item_results"] = upsert_rows(
            cursor,
            "test_item_results",
            item_rows,
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
                "required_flag",
                "item_result",
                "failure_code",
                "failure_message",
            ],
        )
        counts["station_events"] = upsert_rows(
            cursor,
            "station_events",
            events,
            ["event_id", "run_id", "sn", "station_id", "event_type", "event_message", "event_time"],
        )
        counts["defect_reasons"] = upsert_rows(
            cursor,
            "defect_reasons",
            defects,
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
        )
    connection.commit()
    return counts


def parse_args():
    parser = argparse.ArgumentParser(description="Store adapter ATE processed data in MySQL")
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--create-schema", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    connection = connect()
    try:
        if args.create_schema:
            create_schema(connection)
        counts = upsert_processed_dir(args.processed_dir, connection)
    finally:
        connection.close()
    print("stored " + " ".join(f"{table}={count}" for table, count in sorted(counts.items())))


if __name__ == "__main__":
    main()
