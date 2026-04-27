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
