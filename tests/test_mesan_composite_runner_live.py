"""Unit tests for the supervised ``mesan_live_runner`` control flow."""

from __future__ import annotations

import logging
from datetime import datetime
from queue import Empty
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _mesan_runner_fakes import (FakeAsyncResult, RecordingManager,
                                 RecordingPool, RecordingTimer, ScriptedQueue,
                                 make_geo_message, make_polar_message,
                                 make_thread_class)

from mesan_compositer import mesan_composite_runner as runner


def _install_runtime_fakes(
    monkeypatch,
    *,
    listener_items,
    async_results=None,
    submission_exception=None,
):
    """Replace all process/thread infrastructure with deterministic fakes."""

    listener_queue = ScriptedQueue(listener_items)
    publisher_queue = ScriptedQueue()
    manager = RecordingManager(listener_queue, publisher_queue)
    pool = RecordingPool(
        async_results=async_results,
        submission_exception=submission_exception,
    )

    pool_factory_calls = []

    def pool_factory(*args, **kwargs):
        pool_factory_calls.append((args, kwargs))
        return pool

    publisher_class = make_thread_class("FakeFilePublisher")
    listener_class = make_thread_class("FakeFileListener")
    RecordingTimer.instances = []

    monkeypatch.setattr(runner, "Pool", pool_factory)
    monkeypatch.setattr(runner, "Manager", lambda: manager)
    monkeypatch.setattr(runner, "FilePublisher", publisher_class)
    monkeypatch.setattr(runner, "FileListener", listener_class)
    monkeypatch.setattr(runner.threading, "Timer", RecordingTimer)
    monkeypatch.setattr(runner, "POLAR_SATELLITES", ["NOAA-20"], raising=False)
    monkeypatch.setattr(runner, "SERVERNAME", "test-host", raising=False)

    return SimpleNamespace(
        listener_queue=listener_queue,
        publisher_queue=publisher_queue,
        manager=manager,
        pool=pool,
        pool_factory_calls=pool_factory_calls,
        publisher_class=publisher_class,
        listener_class=listener_class,
    )


def _ready_and_register(holder):
    """Build a ``ready2run`` fake that preserves the original side effect."""

    def fake_ready(msg, composite_files, jobs_dict, keyname, product):
        holder["calls"].append(
            (msg, composite_files, jobs_dict, keyname, product)
        )
        holder["jobs_dict"] = jobs_dict
        jobs_dict[keyname] = datetime(2026, 8, 3, 22, 14, 49)
        return True

    return fake_ready


def _assert_clean_shutdown(runtime):
    assert runtime.pool.terminate_calls == 1
    assert runtime.pool.join_calls == 1
    assert runtime.manager.shutdown_calls == 1

    publisher = runtime.publisher_class.instances[0]
    listener = runtime.listener_class.instances[0]
    assert publisher.start_calls == 1
    assert listener.start_calls == 1
    assert publisher.stop_calls == 1
    assert listener.stop_calls == 1
    assert publisher.join_timeouts == [10]
    assert listener.join_timeouts == [10]


def test_live_runner_routes_geo_ct_message_and_collects_async_result(monkeypatch):
    start = datetime(2026, 8, 3, 22, 0)
    message = make_geo_message(product="CT", start_time=start)
    async_result = FakeAsyncResult(
        ready=True,
        value={"status": "success", "product": "CT"},
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
        async_results=[async_result],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    config = {"number_of_pixels": 24}
    runner.mesan_live_runner(config)

    assert runtime.pool_factory_calls == [
        ((), {"processes": 1, "maxtasksperchild": 1})
    ]
    assert len(runtime.pool.apply_async_calls) == 1
    call = runtime.pool.apply_async_calls[0]
    assert call.worker is runner.ctype_composite_worker

    scene, job_id, publish_queue, submitted_config = call.args
    assert scene == {
        "platform_name": "Meteosat-10",
        "orbit_number": "00000",
        "starttime": start,
        "endtime": None,
        "sensor": "['seviri']",
        "filename": message.data["uri"],
        "product": "CT",
    }
    assert job_id == datetime(2026, 8, 3, 22, 14, 49)
    assert publish_queue is runtime.publisher_queue
    assert submitted_config is config
    assert holder["calls"][0][3] == (
        "CT_Meteosat-10_00000_202608032200"
    )
    assert async_result.get_calls == 1

    assert len(RecordingTimer.instances) == 1
    timer = RecordingTimer.instances[0]
    assert timer.interval == 300.0
    assert timer.function is runner.reset_job_registry
    assert timer.args[1] == "CT_Meteosat-10_00000_202608032200"
    assert timer.daemon is True
    assert timer.start_calls == 1

    assert runtime.listener_queue.get_timeouts == [1.0, 1.0]
    _assert_clean_shutdown(runtime)


def test_live_runner_routes_ctth_message_to_ctth_worker(monkeypatch):
    start = datetime(2026, 8, 3, 22, 0)
    message = make_geo_message(product="CTTH", start_time=start)
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
        async_results=[FakeAsyncResult(ready=True, value={"status": "success"})],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    call = runtime.pool.apply_async_calls[0]
    assert call.worker is runner.ctth_composite_worker
    assert call.args[0]["product"] == "CTTH"
    assert holder["calls"][0][3] == (
        "CTTH_Meteosat-10_00000_202608032200"
    )
    _assert_clean_shutdown(runtime)


def test_live_runner_detects_product_from_uid_when_pge_is_absent(monkeypatch):
    start = datetime(2026, 8, 3, 22, 0)
    message = make_geo_message(
        product="CTTH",
        start_time=start,
        include_pge=False,
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
        async_results=[FakeAsyncResult(ready=True, value=None)],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    assert runtime.pool.apply_async_calls[0].worker is runner.ctth_composite_worker
    assert holder["calls"][0][4] == "CTTH"


def test_live_runner_preserves_polar_orbit_number_in_key_and_scene(monkeypatch):
    start = datetime(2026, 8, 3, 22, 0)
    message = make_polar_message(
        product="CT",
        start_time=start,
        orbit_number=12345,
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
        async_results=[FakeAsyncResult(ready=True, value=None)],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    scene = runtime.pool.apply_async_calls[0].args[0]
    assert scene["orbit_number"] == 12345
    assert holder["calls"][0][3] == "CT_NOAA-20_12345_202608032200"


def test_live_runner_does_not_submit_when_ready2run_returns_false(monkeypatch):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    ready = Mock(return_value=False)
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.mesan_live_runner({})

    ready.assert_called_once()
    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)


def test_live_runner_ignores_duplicate_while_first_job_is_pending(monkeypatch):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, message, None],
        async_results=[FakeAsyncResult(ready=False)],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    assert len(holder["calls"]) == 1
    assert len(runtime.pool.apply_async_calls) == 1
    assert len(RecordingTimer.instances) == 1
    _assert_clean_shutdown(runtime)


def test_live_runner_logs_bad_message_and_continues_with_next_message(
    monkeypatch,
    caplog,
):
    start = datetime(2026, 8, 3, 22, 0)
    malformed = SimpleNamespace(
        type="file",
        data={
            "platform_name": "Meteosat-10",
            "nominal_time": start,
            # Missing sensor deliberately triggers the per-message handler.
        },
    )
    valid = make_geo_message(product="CT", start_time=start)
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[malformed, valid, None],
        async_results=[FakeAsyncResult(ready=True, value=None)],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        runner.mesan_live_runner({})

    assert "Unhandled exception while processing message" in caplog.text
    assert len(runtime.pool.apply_async_calls) == 1
    assert len(holder["calls"]) == 1
    _assert_clean_shutdown(runtime)


def test_live_runner_releases_registry_when_pool_submission_fails(
    monkeypatch,
    caplog,
):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
        submission_exception=RuntimeError("pool closed"),
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        runner.mesan_live_runner({})

    assert holder["jobs_dict"] == {}
    assert RecordingTimer.instances == []
    assert "pool.apply_async failed synchronously" in caplog.text
    _assert_clean_shutdown(runtime)


def test_live_runner_cleans_registry_for_unsupported_product(monkeypatch):
    message = make_geo_message(
        product="CMA",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    assert holder["jobs_dict"] == {}
    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)


def test_live_runner_catches_unexpected_exception_after_registration(
    monkeypatch,
    caplog,
):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))
    monkeypatch.setattr(
        runner,
        "submit_pool_job",
        Mock(side_effect=RuntimeError("supervision programming error")),
    )

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        runner.mesan_live_runner({})

    assert holder["jobs_dict"] == {}
    assert "Unhandled exception while processing message" in caplog.text
    assert "supervision programming error" in caplog.text
    _assert_clean_shutdown(runtime)


def test_live_runner_raises_stuck_pool_job_and_still_cleans_up(monkeypatch):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message],
        async_results=[FakeAsyncResult(ready=False)],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    # First call: monitor with no jobs. Second: submission timestamp.
    # Third: next monitoring pass, now beyond the hard timeout.
    clock = iter([0.0, 0.0, 20.0])
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock))

    with pytest.raises(runner.StuckPoolJob, match="CT_Meteosat"):
        runner.mesan_live_runner(
            {
                "pool_job_warning_seconds": 10_000,
                "pool_job_hard_timeout_seconds": 10,
            }
        )

    _assert_clean_shutdown(runtime)


def test_live_runner_polls_again_after_empty_listener_queue(monkeypatch):
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[Empty(), None],
    )

    runner.mesan_live_runner({})

    assert runtime.listener_queue.get_timeouts == [1.0, 1.0]
    assert runtime.pool.apply_async_calls == []
    _assert_clean_shutdown(runtime)


def test_live_runner_skips_message_without_start_or_nominal_time(monkeypatch):
    message = SimpleNamespace(
        type="file",
        data={
            "platform_name": "Meteosat-10",
            "sensor": ["seviri"],
            "uri": "/CT/S_NWC_CT_MSG3_missing_time.nc",
            "pge": "CT",
        },
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    ready = Mock(return_value=True)
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.mesan_live_runner({})

    ready.assert_not_called()
    assert runtime.pool.apply_async_calls == []
    _assert_clean_shutdown(runtime)


def test_live_runner_refuses_scene_when_ready2run_did_not_register_it(
    monkeypatch,
):
    message = make_geo_message(
        product="CT",
        start_time=datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )

    def returns_true_without_registration(
        msg,
        composite_files,
        jobs_dict,
        keyname,
        product,
    ):
        del msg, composite_files, jobs_dict, keyname, product
        return True

    monkeypatch.setattr(
        runner,
        "ready2run",
        returns_true_without_registration,
    )

    runner.mesan_live_runner({})

    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)
