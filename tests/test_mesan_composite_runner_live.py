"""Unit tests for the supervised ``mesan_live_runner`` control flow."""

from __future__ import annotations

import datetime as dt
import logging
from itertools import count
from queue import Empty, Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from _mesan_runner_fakes import (
    FakeAsyncResult,
    RecordingManager,
    RecordingPool,
    RecordingTimer,
    ScriptedQueue,
    make_geo_message,
    make_polar_message,
    make_thread_class,
)
from freezegun import freeze_time

from mesan_compositer import mesan_composite_runner as runner


def _install_runtime_fakes(monkeypatch, *, listener_items, async_results=None, submission_exception=None):
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
    """Build a ``ready2run`` fake that preserves its registration side effect."""

    def fake_ready(msg, composite_files, jobs_dict, product="CT", now=None):
        del now

        platform_name = msg.data["platform_name"]
        start_time = msg.data.get(
            "start_time",
            msg.data.get("nominal_time"),
        )

        if platform_name in runner.GEO_SATS:
            orbit_number = 0
        else:
            orbit_number = int(msg.data["orbit_number"])

        scene_id = runner.make_scene_id(product, platform_name, orbit_number, start_time)

        files = [msg.data["uri"]]

        holder["calls"].append(
            (msg, composite_files, jobs_dict, product)
        )
        holder["jobs_dict"] = jobs_dict
        holder["scene_id"] = scene_id

        jobs_dict[scene_id] = dt.datetime(2026, 8, 3, 22, 14, 49, tzinfo=dt.timezone.utc)

        return scene_id, files

    return fake_ready



def _assert_clean_shutdown(runtime):
    """Assert clean shut down."""
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
    """Test that the live runner routes GEO CT message and collects the async-result."""
    start = dt.datetime(2026, 8, 3, 22, 0)
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
        "orbit_number": 0,
        "starttime": start,
        "endtime": None,
        "sensor": ["seviri"],
        "filename": message.data["uri"],
        "product": "CT",
    }
    assert job_id == dt.datetime(2026, 8, 3, 22, 14, 49, tzinfo=dt.timezone.utc)
    assert publish_queue is runtime.publisher_queue
    assert submitted_config is config
    assert holder["scene_id"] == ("CT_Meteosat-10_00000_202608032200")
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
    """Test that the live runner routes a CTTH message to the CTTH worker."""
    start = dt.datetime(2026, 8, 3, 22, 0)
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
    assert holder["scene_id"] == ("CTTH_Meteosat-10_00000_202608032200")
    _assert_clean_shutdown(runtime)


def test_live_runner_detects_product_from_uid_when_pge_is_absent(monkeypatch):
    """Test that the live runner detects product from uid when PGE is absent."""
    start = dt.datetime(2026, 8, 3, 22, 0)
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
    assert holder["calls"][0][3] == "CTTH"


def test_live_runner_preserves_polar_orbit_number_in_key_and_scene(monkeypatch):
    """Test that the live runner perserves the orbit number in key and scene."""
    start = dt.datetime(2026, 8, 3, 22, 0)
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
    assert holder["scene_id"] == ("CT_NOAA-20_12345_202608032200")


def test_live_runner_does_not_submit_when_ready2run_returns_false(monkeypatch):
    """Test that the live runner dows not submit when ready2run returns False."""
    message = make_geo_message(
        product="CT",
        start_time=dt.datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    ready = Mock(return_value=None)
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.mesan_live_runner({})

    ready.assert_called_once()
    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)


def test_live_runner_does_not_resubmit_pending_duplicate(monkeypatch):
    """Test that the live runner does not re-submit pending duplicate."""
    message = make_geo_message(product="CT", start_time=dt.datetime(2026, 8, 3, 22, 0))
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


def test_live_runner_logs_bad_message_and_continues_with_next_message(monkeypatch, caplog):
    """Test that the live runner logs bad message and continues with next message."""
    start = dt.datetime(2026, 8, 3, 22, 0)
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


def test_live_runner_releases_registry_when_pool_submission_fails(monkeypatch, caplog, ):
    """Test that the live runner releases registry when poolmsubmission fails."""
    message = make_geo_message(product="CT", start_time=dt.datetime(2026, 8, 3, 22, 0), )
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
    """Test that the live runner cleans the registry for unsupported product."""
    message = make_geo_message( product="CMA", start_time=dt.datetime(2026, 8, 3, 22, 0), )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )
    holder = {"calls": []}
    monkeypatch.setattr(runner, "ready2run", _ready_and_register(holder))

    runner.mesan_live_runner({})

    assert holder["calls"] == []
    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)


def test_live_runner_catches_unexpected_exception_after_registration(monkeypatch, caplog):
    """Test that the live runner catches unexpected exception after registration."""
    message = make_geo_message(product="CT", start_time=dt.datetime(2026, 8, 3, 22, 0))
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
    """Test that the live runner raises a stuck pool job and still cleans up."""
    message = make_geo_message(
        product="CT",
        start_time=dt.datetime(2026, 8, 3, 22, 0),
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
    """Test that the live runner polls again after empry listener queue."""
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[Empty(), None],
    )

    runner.mesan_live_runner({})

    assert runtime.listener_queue.get_timeouts == [1.0, 1.0]
    assert runtime.pool.apply_async_calls == []
    _assert_clean_shutdown(runtime)


def test_live_runner_skips_message_without_start_or_nominal_time(monkeypatch):
    """Test that the live runer skips message without start or nominal time set."""
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

    scene_id = "CT_Meteosat-10_00000_202608032200"

    ready = Mock(
        return_value=(scene_id, [message.data["uri"]])
    )
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.mesan_live_runner({})

    ready.assert_not_called()
    assert runtime.pool.apply_async_calls == []
    _assert_clean_shutdown(runtime)


def test_live_runner_refuses_scene_when_ready2run_did_not_register_it(monkeypatch):
    """Test that the live runner refuses scene when ready2run did not register it."""
    message = make_geo_message(
        product="CT",
        start_time=dt.datetime(2026, 8, 3, 22, 0),
    )
    runtime = _install_runtime_fakes(
        monkeypatch,
        listener_items=[message, None],
    )

    def returns_job_without_registration(
            msg,
            composite_files,
            jobs_dict,
            product="CT",
            now=None):
        del composite_files, jobs_dict, now

        metadata = runner.get_scene_metadata(msg)
        scene_id = runner.make_scene_id(
            product,
            metadata["platform_name"],
            metadata["orbit_number"],
            metadata["start_time"],
        )

        return scene_id, [msg.data["uri"]]

    monkeypatch.setattr(
        runner,
        "ready2run",
        returns_job_without_registration,
    )

    runner.mesan_live_runner({})

    assert runtime.pool.apply_async_calls == []
    assert RecordingTimer.instances == []
    _assert_clean_shutdown(runtime)



@freeze_time("2026-08-28 14:00:00+00:00")
@pytest.mark.parametrize(
    ("obstime", "expected"),
    [
        (
            dt.datetime(2026, 8, 28, 13, 15, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 8, 28, 13, 15, tzinfo=dt.timezone.utc),
        ),
        (
            None,
            dt.datetime(2026, 8, 28, 14, 0, tzinfo=dt.timezone.utc),
        ),
    ],
)
def test_ready2run_registers_new_job(obstime, expected):
    """Test the ready2run function and that it registers a new job."""
    files = {}
    jobs = {}
    valid_message = make_geo_message(product="CT", start_time=expected)
    result = runner.ready2run(valid_message, files, jobs, product="CT", now=obstime)
    scene_id, scene_files = result

    assert scene_id in jobs
    assert len(scene_files) == 1
    assert scene_id in jobs
    assert jobs[scene_id] == expected


def test_ready2run_registers_scene():
    """Test that the ready2run registers a scene correctly."""
    now = dt.datetime(2026, 8, 30, 10, 0, tzinfo=dt.timezone.utc)

    files = {}
    jobs = {}
    valid_msg = make_geo_message(product="CT", start_time=now)

    result = runner.ready2run(valid_msg, files, jobs, product="CT", now=now)
    scene_id, scene_files = result

    assert scene_id in jobs
    assert jobs[scene_id] == now
    assert scene_files == ["/CT/S_NWC_CT_MSG3_MSG-N-VISIR_20260830T100000Z_PLAX.nc"]


def test_ready2run_does_not_register_invalid_file():
    """Unit test..."""
    START_TIME = dt.datetime(2026, 8, 30, 10, 0, tzinfo=dt.timezone.utc)
    with patch(
        "mesan_compositer.mesan_composite_runner.find_files_for_composite",
        return_value=None,
    ):
        files = {}
        jobs = {}
        valid_msg = make_geo_message(product="CT", start_time=START_TIME)

        result = runner.ready2run(valid_msg, files, jobs, product="CT")

    assert result is None
    assert jobs == {}


def test_ready2run_rejects_registered_scene():
    """Test that ready2run rejects already registered scenes."""
    START_TIME = dt.datetime(2026, 8, 30, 10, 0, tzinfo=dt.timezone.utc)
    valid_msg = make_geo_message(product="CT", start_time=START_TIME)
    scene_id = runner.make_scene_id(
        "CT",
        "Meteosat-10",
        0,
        START_TIME,
    )

    registered_at = dt.datetime(2026, 8, 30, 10, 0, tzinfo=dt.timezone.utc)

    jobs = {scene_id: registered_at, }

    result = runner.ready2run(
        valid_msg,
        {},
        jobs,
        product="CT",
    )

    assert result is None
    assert jobs[scene_id] == registered_at


def test_make_scene_id():
    """Test make a scene id."""
    result = runner.make_scene_id(
        "CT",
        "Meteosat-10",
        0,
        dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    )

    assert result == ("CT_Meteosat-10_00000_202608032200")


def test_process_message_rejects_scene_still_pending(monkeypatch):
    """Test that processing a message rejects a scene which is still pending."""
    start = dt.datetime(2026, 8,30, 12, 0, tzinfo=dt.timezone.utc)

    msg = make_geo_message(product="CT", start_time=start)

    scene_id = runner.make_scene_id("CT", "Meteosat-10", 0, start)

    pending_jobs = {
        scene_id: runner.PendingPoolJob(
            key=scene_id,
            token=1,
            product="CT",
            submitted_monotonic=100.0,
            async_result=FakeAsyncResult(
                ready=False,
            ),
        )
    }

    jobs_dict = {}

    ready = Mock()
    monkeypatch.setattr(runner, "ready2run", ready)

    state = runner.RunnerState(
        pool=RecordingPool(),
        publisher_q=ScriptedQueue(),
        completion_q=Queue(),
        composite_files={},
        jobs_dict=jobs_dict,
        pending_jobs=pending_jobs,
        token_counter=count(1),
        config_options={},
    )
    runner.process_message(msg, state)

    ready.assert_not_called()
    assert jobs_dict == {}
