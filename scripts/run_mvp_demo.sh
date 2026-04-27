#!/usr/bin/env bash
set -euo pipefail

mkdir -p build data/raw data/processed reports

g++ -std=c++17 -Wall -Wextra cpp_ate_simulator/ate_line_simulator.cpp -o build/ate_line_simulator

./build/ate_line_simulator \
  --count 100 \
  --output-dir data/raw \
  --seed 20260425 \
  --abnormal-rate 0.2 \
  --batch-no B20260425 \
  --product-model ADP-65W \
  --line-id LINE-01

.venv/bin/python -m adapter_ate.processor \
  --raw-dir data/raw \
  --config config/test_rules.json \
  --output-dir data/processed \
  --log reports/process.log

.venv/bin/python -m adapter_ate.reports \
  --processed-dir data/processed \
  --reports-dir reports

echo "MVP demo complete"
echo "Processed results: data/processed/processed_results.csv"
echo "Reports: reports/daily_summary.csv reports/batch_summary.csv reports/defect_summary.csv"
