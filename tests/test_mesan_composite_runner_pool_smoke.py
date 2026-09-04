"""Optional smoke tests using a real multiprocessing pool.

Run explicitly with::

    MESAN_RUN_MULTIPROCESSING_TESTS=1 pytest -q \
        tests/test_mesan_composite_runner_pool_smoke.py
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import time
from itertools import count
from queue import Queue

import pytest

from mesan_compositer import mesan_composite_runner as runner

pytestmark = pytest.mark.skipif(
    os.environ.get("MESAN_RUN_MULTIPROCESSING_TESTS") != "1",
    reason="set MESAN_RUN_MULTIPROCESSING_TESTS=1 to run real-pool smoke tests",
)


def _real_worker_success(value):
    """Test this."""
    return {"value": value, "worker_pid": os.getpid()}


def _real_worker_failure():
    """Test this."""
    raise RuntimeError("real pool worker failure")


def _wait_for_completion(completion_queue, pending_jobs, timeout=10.0):
    """Test this."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        completed = runner.collect_pool_results(
            completion_queue,
            pending_jobs,
        )
        if completed:
            return completed
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for the real pool result")


def test_real_pool_success_flows_through_callback_and_asyncresult_get():
    """Test this."""
    context = mp.get_context("spawn")
    pool = context.Pool(processes=1)
    completion_queue = Queue()
    pending_jobs = {}

    try:
        assert runner.submit_pool_job(
            pool,
            completion_queue,
            pending_jobs,
            count(1),
            job_key="real-success",
            product="CT",
            worker=_real_worker_success,
            worker_args=(17,),
        )

        completed = _wait_for_completion(completion_queue, pending_jobs)
    finally:
        pool.terminate()
        pool.join()

    assert len(completed) == 1
    job_key, succeeded, worker_result = completed[0]
    assert job_key == "real-success"
    assert succeeded is True
    assert worker_result["value"] == 17
    assert worker_result["worker_pid"] != os.getpid()
    assert pending_jobs == {}


def test_real_pool_exception_is_reported_by_asyncresult_get(caplog):
    """Test this."""
    context = mp.get_context("spawn")
    pool = context.Pool(processes=1)
    completion_queue = Queue()
    pending_jobs = {}

    try:
        assert runner.submit_pool_job(
            pool,
            completion_queue,
            pending_jobs,
            count(1),
            job_key="real-failure",
            product="CT",
            worker=_real_worker_failure,
            worker_args=(),
        )

        with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
            completed = _wait_for_completion(completion_queue, pending_jobs)
    finally:
        pool.terminate()
        pool.join()

    assert completed == [("real-failure", False, None)]
    assert "Pool job failed" in caplog.text
    assert "real pool worker failure" in caplog.text
    assert pending_jobs == {}
