#!/usr/bin/env bash
set -euo pipefail

python -m pytest -q \
  tests/test_mesan_composite_runner_async.py \
  tests/test_mesan_composite_runner_workers.py \
  tests/test_mesan_composite_runner_live.py \
  tests/test_mesan_composite_runner_pool_smoke.py
