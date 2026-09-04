"""Tests for asynchronous pool supervision.

Normal success/failure uses a real ThreadPool.  Tiny local result objects are
used only for states that are deliberately awkward to produce with a real
pool (never-ready, callback-before-ready, and stale completion).
"""

from __future__ import annotations

import logging
from itertools import count
from multiprocessing.pool import ThreadPool
from queue import Queue
from unittest.mock import Mock

import pytest

from mesan_compositer import mesan_composite_runner as runner


def _return_value(value):
    """Minimal helper function just returning the input value."""
    return value


def _raise_error():
    """Minimal helper function raising and error."""
    raise RuntimeError("worker failed")


class NeverReadyResult:
    """Minimal state object for a job which never becomes ready."""

    def ready(self):
        """Return the ready value (which in this case is always False)."""
        return False


class ToggleResult:
    """Minimal AsyncResult-like object for race/stale completion tests."""

    def __init__(self, *, ready=False, value=None, error=None):
        """Initialize the object."""
        self.ready_value = ready
        self.value = value
        self.error = error
        self.get_calls = 0

    def ready(self):
        """Return ready value."""
        return self.ready_value

    def get(self):
        """Get the result."""
        self.get_calls += 1
        if self.error is not None:
            raise self.error
        return self.value


def _pending(async_result, *, submitted=0.0, token=1, key="CT_scene"):
    """Summy helper function for pending pool jobs."""
    return runner.PendingPoolJob(
        key=key,
        token=token,
        product="CT",
        submitted_monotonic=submitted,
        async_result=async_result,
    )


def test_success_callback_enqueues_identity_only():
    """Test success callback enqueues identity only."""
    queue = Queue()
    runner._notify_pool_success(queue, "CT_scene", 7, {"large": "result"})
    assert queue.get_nowait() == ("CT_scene", 7, "success")


def test_failure_callback_enqueues_exception_repr():
    """Test failure callback enqueues exception repr."""
    queue = Queue()
    runner._notify_pool_failure(queue, "CT_scene", 8, RuntimeError("boom"))
    assert queue.get_nowait() == (
        "CT_scene",
        8,
        "failure",
        "RuntimeError('boom')",
    )


def test_submit_and_collect_success_with_real_thread_pool(monkeypatch):
    """Test submit and collect success with real ThreadPool."""
    monkeypatch.setattr(runner, "_pool_worker_snapshot", lambda pool: [])

    pool = ThreadPool(processes=1)
    completion_q = Queue()
    pending_jobs = {}

    try:
        assert runner.submit_pool_job(
            pool,
            completion_q,
            pending_jobs,
            count(1),
            job_key="CT_scene",
            product="CT",
            worker=_return_value,
            worker_args=("done",),
        )

        pool.close()
        pool.join()

        completed = runner.collect_pool_results(completion_q, pending_jobs)
    finally:
        pool.terminate()

    assert completed == [("CT_scene", True, "done")]
    assert pending_jobs == {}


def test_worker_exception_surfaces_via_asyncresult_get(caplog, monkeypatch):
    """Test worker exception surfaces via async result get."""
    monkeypatch.setattr(runner, "_pool_worker_snapshot", lambda pool: [])

    pool = ThreadPool(processes=1)
    completion_q = Queue()
    pending_jobs = {}

    try:
        assert runner.submit_pool_job(
            pool,
            completion_q,
            pending_jobs,
            count(1),
            job_key="CT_scene",
            product="CT",
            worker=_raise_error,
            worker_args=(),
        )

        pool.close()
        pool.join()

        with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
            completed = runner.collect_pool_results(completion_q, pending_jobs)
    finally:
        pool.terminate()

    assert completed == [("CT_scene", False, None)]
    assert pending_jobs == {}
    assert "Pool job failed" in caplog.text
    assert "worker failed" in caplog.text


def test_submit_pool_job_rejects_duplicate_without_calling_pool(caplog):
    """Test submit pool job rejects duplicate without calling  pool."""
    pool = Mock()
    pending_jobs = {"CT_scene": _pending(NeverReadyResult())}

    with caplog.at_level(logging.WARNING, logger=runner.LOG.name):
        submitted = runner.submit_pool_job(
            pool,
            Queue(),
            pending_jobs,
            count(1),
            job_key="CT_scene",
            product="CT",
            worker=_return_value,
            worker_args=(1,),
        )

    assert not submitted
    pool.apply_async.assert_not_called()
    assert "duplicate ignored" in caplog.text


def test_submit_pool_job_handles_synchronous_pool_failure(caplog):
    """Test submitting a pool job handles synchronous pool failure."""
    pool = Mock()
    pool._pool = []
    pool.apply_async.side_effect = RuntimeError("pool closed")

    completion_q = Queue()
    pending_jobs = {}

    submitted = runner.submit_pool_job(
        pool,
        completion_q,
        pending_jobs,
        count(1),
        job_key="CT_scene",
        product="CT",
        worker=Mock(),
        worker_args=(),
    )

    assert submitted is False
    assert pending_jobs == {}

    assert "pool.apply_async failed synchronously" in caplog.text


def test_collect_results_falls_back_when_callback_event_is_missing(monkeypatch):
    """Test collect results falls back when callback event is missing."""
    result = ToggleResult(ready=True, value="done")
    pending_jobs = {"CT_scene": _pending(result, submitted=5.0)}
    monkeypatch.setattr(runner.time, "monotonic", lambda: 6.0)

    assert runner.collect_pool_results(Queue(), pending_jobs) == [
        ("CT_scene", True, "done")
    ]
    assert pending_jobs == {}
    assert result.get_calls == 1


def test_collect_results_handles_callback_before_ready_race(monkeypatch):
    """Test collect results handles callback before ready."""
    result = ToggleResult(ready=False, value="done")
    pending_jobs = {"CT_scene": _pending(result, submitted=5.0)}
    completion_q = Queue()
    completion_q.put(("CT_scene", 1, "success"))
    monkeypatch.setattr(runner.time, "monotonic", lambda: 6.0)

    assert runner.collect_pool_results(completion_q, pending_jobs) == []
    assert "CT_scene" in pending_jobs
    assert result.get_calls == 0

    result.ready_value = True
    assert runner.collect_pool_results(completion_q, pending_jobs) == [
        ("CT_scene", True, "done")
    ]
    assert pending_jobs == {}


def test_stale_completion_does_not_remove_new_submission():
    """Test this."""
    result = ToggleResult(ready=False)
    pending_jobs = {"CT_scene": _pending(result, token=2)}
    completion_q = Queue()
    completion_q.put(("CT_scene", 1, "success"))

    assert runner.collect_pool_results(completion_q, pending_jobs) == []
    assert pending_jobs["CT_scene"].token == 2
    assert result.get_calls == 0


def test_monitor_pending_job_warns_and_rate_limits(caplog, monkeypatch):
    """Test monitor pending job warns and rate limits."""
    pool = Mock()
    pool._pool = []

    pending = _pending(NeverReadyResult(), submitted=0.0)
    clock = iter([100.0, 120.0, 161.0])
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock))

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        for _ in range(3):
            runner.monitor_pending_pool_jobs(
                {pending.key: pending},
                pool,
                warning_seconds=60.0,
                warning_repeat_seconds=60.0,
                hard_timeout_seconds=0.0,
            )

    records = [
        record
        for record in caplog.records
        if "Pool job has not completed" in record.getMessage()
    ]
    assert len(records) == 2
    assert pending.last_warning_monotonic == 161.0


def test_monitor_pending_job_raises_after_hard_timeout(monkeypatch):
    """Test monitor a pending job raises after a hard time out."""
    pending = _pending(NeverReadyResult(), submitted=10.0)
    monkeypatch.setattr(runner.time, "monotonic", lambda: 50.0)

    with pytest.raises(runner.StuckPoolJob, match="CT_scene"):
        runner.monitor_pending_pool_jobs(
            {pending.key: pending},
            Mock(),
            warning_seconds=10_000.0,
            warning_repeat_seconds=60.0,
            hard_timeout_seconds=30.0,
        )
