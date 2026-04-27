#!/usr/bin/env bash
set -euo pipefail

bash scripts/run_mvp_demo.sh

.venv/bin/python -m adapter_ate.ai_model \
  --processed-dir data/processed \
  --model models/quality_model.joblib \
  --metrics reports/model_metrics.json

.venv/bin/python scripts/api_smoke.py

if [[ -n "${MYSQL_HOST:-}" ]]; then
  .venv/bin/python -m adapter_ate.storage \
    --processed-dir data/processed \
    --create-schema
else
  echo "MYSQL_HOST is not set; skipping MySQL import"
fi

echo "Extension demo complete"
