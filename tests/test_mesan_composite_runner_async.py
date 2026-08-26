"""Unit tests for the AsyncResult supervision added to the MESAN runner."""

from __future__ import annotations

import logging
from itertools import count
from queue import Queue

import pytest
from _mesan_runner_fakes import FakeAsyncResult, FakeProcess, RecordingPool

from mesan_compositer import mesan_composite_runner as runner


def _dummy_worker(value):
    return value


def _pending_job(
    *,
    async_result: FakeAsyncResult,
    submitted: float = 0.0,
    token: int = 1,
    key: str = "CT_scene",
    product: str = "CT",
):
    return runner.PendingPoolJob(
        key=key,
        token=token,
        product=product,
        submitted_monotonic=submitted,
        async_result=async_result,
    )


def test_success_callback_enqueues_only_identity_information():
    completion_queue = Queue()

    runner._notify_pool_success(
        completion_queue,
        "CT_scene",
        7,
        {"large": "worker result is deliberately not copied to this queue"},
    )

    assert completion_queue.get_nowait() == ("CT_scene", 7, "success")


def test_failure_callback_enqueues_exception_representation():
    completion_queue = Queue()

    runner._notify_pool_failure(
        completion_queue,
        "CT_scene",
        8,
        RuntimeError("worker exploded"),
    )

    assert completion_queue.get_nowait() == (
        "CT_scene",
        8,
        "failure",
        "RuntimeError('worker exploded')",
    )


def test_pool_worker_snapshot_reports_process_state():
    pool = RecordingPool()
    pool._pool = [
        FakeProcess(pid=101, alive=True, exitcode=None),
        FakeProcess(pid=102, alive=False, exitcode=1),
    ]

    assert runner._pool_worker_snapshot(pool) == [
        {"pid": 101, "alive": True, "exitcode": None},
        {"pid": 102, "alive": False, "exitcode": 1},
    ]


def test_submit_pool_job_retains_async_result_and_callbacks(monkeypatch):
    result = FakeAsyncResult()
    pool = RecordingPool(async_results=[result])
    completion_queue = Queue()
    pending_jobs = {}
    monkeypatch.setattr(runner.time, "monotonic", lambda: 12.5)

    submitted = runner.submit_pool_job(
        pool,
        completion_queue,
        pending_jobs,
        count(41),
        job_key="CT_scene",
        product="CT",
        worker=_dummy_worker,
        worker_args=(123,),
    )

    assert submitted is True
    assert list(pending_jobs) == ["CT_scene"]
    pending = pending_jobs["CT_scene"]
    assert pending.token == 41
    assert pending.product == "CT"
    assert pending.submitted_monotonic == 12.5
    assert pending.async_result is result

    call = pool.apply_async_calls[0]
    assert call.worker is _dummy_worker
    assert call.args == (123,)
    assert call.callback is not None
    assert call.error_callback is not None

    call.callback({"status": "success"})
    assert completion_queue.get_nowait() == ("CT_scene", 41, "success")


def test_submit_pool_job_rejects_duplicate_without_touching_pool(caplog):
    result = FakeAsyncResult()
    pool = RecordingPool(async_results=[result])
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result),
    }

    with caplog.at_level(logging.WARNING, logger=runner.LOG.name):
        submitted = runner.submit_pool_job(
            pool,
            Queue(),
            pending_jobs,
            count(1),
            job_key="CT_scene",
            product="CT",
            worker=_dummy_worker,
            worker_args=(),
        )

    assert submitted is False
    assert pool.apply_async_calls == []
    assert "duplicate ignored" in caplog.text


def test_submit_pool_job_handles_synchronous_pool_failure(caplog):
    pool = RecordingPool(
        submission_exception=RuntimeError("pool is not running")
    )
    pending_jobs = {}

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        submitted = runner.submit_pool_job(
            pool,
            Queue(),
            pending_jobs,
            count(1),
            job_key="CT_scene",
            product="CT",
            worker=_dummy_worker,
            worker_args=(),
        )

    assert submitted is False
    assert pending_jobs == {}
    assert "pool.apply_async failed synchronously" in caplog.text
    assert "pool is not running" in caplog.text


def test_collect_pool_results_returns_success_and_removes_pending(monkeypatch):
    result = FakeAsyncResult(
        ready=True,
        value={"status": "success", "result_file": "/tmp/ct.nc"},
    )
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result, submitted=10.0),
    }
    completion_queue = Queue()
    completion_queue.put(("CT_scene", 1, "success"))
    monkeypatch.setattr(runner.time, "monotonic", lambda: 15.0)

    completed = runner.collect_pool_results(completion_queue, pending_jobs)

    assert completed == [
        (
            "CT_scene",
            True,
            {"status": "success", "result_file": "/tmp/ct.nc"},
        )
    ]
    assert pending_jobs == {}
    assert result.get_calls == 1


def test_collect_pool_results_logs_worker_exception_in_parent(caplog, monkeypatch):
    result = FakeAsyncResult(
        ready=True,
        exception=ValueError("bad satellite input"),
    )
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result, submitted=20.0),
    }
    completion_queue = Queue()
    completion_queue.put(
        (
            "CT_scene",
            1,
            "failure",
            "ValueError('bad satellite input')",
        )
    )
    monkeypatch.setattr(runner.time, "monotonic", lambda: 23.0)

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        completed = runner.collect_pool_results(completion_queue, pending_jobs)

    assert completed == [("CT_scene", False, None)]
    assert pending_jobs == {}
    assert result.get_calls == 1
    assert "Pool error callback" in caplog.text
    assert "Pool job failed" in caplog.text
    assert "bad satellite input" in caplog.text


def test_collect_pool_results_falls_back_when_callback_event_is_missing(monkeypatch):
    result = FakeAsyncResult(ready=True, value="done")
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result, submitted=5.0),
    }
    monkeypatch.setattr(runner.time, "monotonic", lambda: 6.0)

    completed = runner.collect_pool_results(Queue(), pending_jobs)

    assert completed == [("CT_scene", True, "done")]
    assert pending_jobs == {}


def test_collect_pool_results_handles_callback_before_ready_race(monkeypatch):
    result = FakeAsyncResult(ready=False, value="done")
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result, submitted=5.0),
    }
    completion_queue = Queue()
    completion_queue.put(("CT_scene", 1, "success"))
    monkeypatch.setattr(runner.time, "monotonic", lambda: 6.0)

    assert runner.collect_pool_results(completion_queue, pending_jobs) == []
    assert "CT_scene" in pending_jobs
    assert result.get_calls == 0

    result.ready_value = True
    assert runner.collect_pool_results(completion_queue, pending_jobs) == [
        ("CT_scene", True, "done")
    ]
    assert pending_jobs == {}


def test_stale_completion_event_does_not_remove_new_submission():
    result = FakeAsyncResult(ready=False)
    pending_jobs = {
        "CT_scene": _pending_job(async_result=result, token=2),
    }
    completion_queue = Queue()
    completion_queue.put(("CT_scene", 1, "success"))

    assert runner.collect_pool_results(completion_queue, pending_jobs) == []
    assert pending_jobs["CT_scene"].token == 2
    assert result.get_calls == 0


def test_monitor_pending_jobs_warns_and_rate_limits_repeated_messages(
    caplog,
    monkeypatch,
):
    result = FakeAsyncResult(ready=False)
    pending = _pending_job(async_result=result, submitted=0.0)
    pending_jobs = {pending.key: pending}
    pool = RecordingPool()

    clock = iter([100.0, 120.0, 161.0])
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock))

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        runner.monitor_pending_pool_jobs(
            pending_jobs,
            pool,
            warning_seconds=60.0,
            warning_repeat_seconds=60.0,
            hard_timeout_seconds=0.0,
        )
        runner.monitor_pending_pool_jobs(
            pending_jobs,
            pool,
            warning_seconds=60.0,
            warning_repeat_seconds=60.0,
            hard_timeout_seconds=0.0,
        )
        runner.monitor_pending_pool_jobs(
            pending_jobs,
            pool,
            warning_seconds=60.0,
            warning_repeat_seconds=60.0,
            hard_timeout_seconds=0.0,
        )

    warning_records = [
        record
        for record in caplog.records
        if "Pool job has not completed" in record.getMessage()
    ]
    assert len(warning_records) == 2
    assert pending.last_warning_monotonic == 161.0


def test_monitor_pending_jobs_raises_on_hard_timeout(monkeypatch):
    result = FakeAsyncResult(ready=False)
    pending = _pending_job(async_result=result, submitted=10.0)
    monkeypatch.setattr(runner.time, "monotonic", lambda: 50.0)

    with pytest.raises(runner.StuckPoolJob, match="CT_scene"):
        runner.monitor_pending_pool_jobs(
            {pending.key: pending},
            RecordingPool(),
            warning_seconds=10_000.0,
            warning_repeat_seconds=60.0,
            hard_timeout_seconds=30.0,
        )


def test_reset_job_registry_is_safe_for_present_and_missing_keys():
    registry = {"CT_scene": object()}

    runner.reset_job_registry(registry, "CT_scene")
    runner.reset_job_registry(registry, "CT_scene")

    assert registry == {}


def test_rewrite_exposes_all_supervision_entry_points():
    expected = {
        "StuckPoolJob",
        "PendingPoolJob",
        "submit_pool_job",
        "collect_pool_results",
        "monitor_pending_pool_jobs",
        "ctype_composite_worker",
        "ctth_composite_worker",
        "mesan_live_runner",
    }

    assert expected.issubset(vars(runner))


def test_very_fast_callback_before_pending_registration_is_not_lost(monkeypatch):
    result = FakeAsyncResult(ready=True, value="fast-result")
    completion_queue = Queue()
    pending_jobs = {}

    class ImmediateCallbackPool(RecordingPool):
        def apply_async(self, worker, args=(), callback=None, error_callback=None):
            async_result = super().apply_async(
                worker,
                args=args,
                callback=callback,
                error_callback=error_callback,
            )
            callback(async_result.value)
            return async_result

    pool = ImmediateCallbackPool(async_results=[result])
    monkeypatch.setattr(runner.time, "monotonic", lambda: 1.0)

    assert runner.submit_pool_job(
        pool,
        completion_queue,
        pending_jobs,
        count(1),
        job_key="fast-job",
        product="CT",
        worker=_dummy_worker,
        worker_args=(1,),
    )

    assert runner.collect_pool_results(completion_queue, pending_jobs) == [
        ("fast-job", True, "fast-result")
    ]
    assert pending_jobs == {}
