"""Tests for one-message orchestration, the message loop, and service lifecycle."""

from __future__ import annotations

import datetime as dt
import logging
from queue import Queue
from unittest.mock import Mock

import pytest

from mesan_compositer import mesan_composite_runner as runner


class NeverReadyResult:
    """Create a dummy never-ready-result class."""

    def ready(self):
        """Return the ready value."""
        return False


def _registering_ready(msg, composite_files, jobs_dict, product="CT", now=None):
    """Small stub for the application-level ready2run contract."""
    del composite_files, now
    metadata = runner.get_scene_metadata(msg)
    scene_id = runner.make_scene_id(
        product,
        metadata["platform_name"],
        metadata["orbit_number"],
        metadata["start_time"],
    )
    jobs_dict[scene_id] = dt.datetime(2026, 8, 3, 22, 14, 49, tzinfo=dt.timezone.utc)
    return scene_id, [msg.data["uri"]]


def test_process_message_routes_ct(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message routes the CT composition."""
    submit = Mock(return_value=True)
    timer = Mock()
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)
    monkeypatch.setattr(runner, "start_registry_timer", timer)

    runner.process_message(geo_ct_message, runner_state)

    kwargs = submit.call_args.kwargs
    scene, job_id, publish_q, config = kwargs["worker_args"]
    assert kwargs["product"] == "CT"
    assert kwargs["worker"] is runner.ctype_composite_worker
    assert scene["orbit_number"] == 0
    assert scene["product"] == "CT"
    assert job_id == dt.datetime(2026, 8, 3, 22, 14, 49, tzinfo=dt.timezone.utc)
    assert publish_q is runner_state.publisher_q
    assert config is runner_state.config_options
    timer.assert_called_once_with(runner_state.jobs_dict, kwargs["job_key"])


def test_process_message_routes_ctth(monkeypatch, runner_state, geo_ctth_message):
    """Tets that process_message routes the CTTH composition."""
    submit = Mock(return_value=True)
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)
    monkeypatch.setattr(runner, "start_registry_timer", Mock())

    runner.process_message(geo_ctth_message, runner_state)

    assert submit.call_args.kwargs["product"] == "CTTH"
    assert submit.call_args.kwargs["worker"] is runner.ctth_composite_worker


def test_process_message_detects_product_from_uid(monkeypatch, runner_state, geo_ctth_message):
    """Tets that process_message detects the product from the uid."""
    geo_ctth_message.data.pop("pge")
    submit = Mock(return_value=True)
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)
    monkeypatch.setattr(runner, "start_registry_timer", Mock())

    runner.process_message(geo_ctth_message, runner_state)

    assert submit.call_args.kwargs["product"] == "CTTH"


def test_process_message_preserves_polar_orbit(monkeypatch, runner_state, polar_ct_message):
    """Tets that process_message preserves the polar orbit number."""
    monkeypatch.setattr(runner, "POLAR_SATELLITES", ["NOAA-20"])
    submit = Mock(return_value=True)
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)
    monkeypatch.setattr(runner, "start_registry_timer", Mock())

    runner.process_message(polar_ct_message, runner_state)

    scene = submit.call_args.kwargs["worker_args"][0]
    assert scene["orbit_number"] == 12345
    assert submit.call_args.kwargs["job_key"] == "CT_NOAA-20_12345_202608032200"


def test_process_message_returns_when_scene_not_ready(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message returns when scene is not ready."""
    ready = Mock(return_value=None)
    submit = Mock()
    monkeypatch.setattr(runner, "ready2run", ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)

    runner.process_message(geo_ct_message, runner_state)

    ready.assert_called_once()
    submit.assert_not_called()


def test_process_message_rejects_scene_still_pending(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message rejects scene that is still pending."""
    start = geo_ct_message.data["start_time"]
    scene_id = runner.make_scene_id("CT", "Meteosat-10", 0, start)
    runner_state.pending_jobs[scene_id] = runner.PendingPoolJob(
        key=scene_id,
        token=1,
        product="CT",
        submitted_monotonic=100.0,
        async_result=NeverReadyResult(),
    )
    ready = Mock()
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.process_message(geo_ct_message, runner_state)

    ready.assert_not_called()
    assert runner_state.jobs_dict == {}


def test_process_message_cleans_registry_when_submission_fails(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message cleans the registry when submission fails."""
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(runner, "submit_pool_job", Mock(return_value=False))
    timer = Mock()
    monkeypatch.setattr(runner, "start_registry_timer", timer)

    runner.process_message(geo_ct_message, runner_state)

    assert runner_state.jobs_dict == {}
    timer.assert_not_called()


def test_process_message_cleans_registry_on_unexpected_exception(
    monkeypatch, runner_state, geo_ct_message
):
    """Tets that process_message cleans the registry on unexpected exception."""
    monkeypatch.setattr(runner, "ready2run", _registering_ready)
    monkeypatch.setattr(
        runner,
        "submit_pool_job",
        Mock(side_effect=RuntimeError("supervision programming error")),
    )

    with pytest.raises(RuntimeError, match="supervision programming error"):
        runner.process_message(geo_ct_message, runner_state)

    assert runner_state.jobs_dict == {}


def test_process_message_skips_message_without_time(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message skips message without start time."""
    geo_ct_message.data.pop("start_time")
    ready = Mock()
    monkeypatch.setattr(runner, "ready2run", ready)

    runner.process_message(geo_ct_message, runner_state)

    ready.assert_not_called()


def test_process_message_refuses_ready_scene_not_registered(monkeypatch, runner_state, geo_ct_message):
    """Tets that process_message refuses a ready scene that is not registered."""
    def unregistered_ready(msg, composite_files, jobs_dict, product="CT", now=None):
        del composite_files, jobs_dict, now
        metadata = runner.get_scene_metadata(msg)
        scene_id = runner.make_scene_id(
            product,
            metadata["platform_name"],
            metadata["orbit_number"],
            metadata["start_time"],
        )
        return scene_id, [msg.data["uri"]]

    submit = Mock()
    monkeypatch.setattr(runner, "ready2run", unregistered_ready)
    monkeypatch.setattr(runner, "submit_pool_job", submit)

    runner.process_message(geo_ct_message, runner_state)

    submit.assert_not_called()
    assert runner_state.jobs_dict == {}


def test_message_loop_processes_message_and_stops(monkeypatch, runner_state, geo_ct_message):
    """Tets the message loop processes a message and stops."""
    listener_q = Queue()
    listener_q.put(geo_ct_message)
    listener_q.put(None)
    process = Mock()
    monitor = Mock()
    collect = Mock()
    monkeypatch.setattr(runner, "process_message", process)
    monkeypatch.setattr(runner, "monitor_pending_pool_jobs", monitor)
    monkeypatch.setattr(runner, "collect_pool_results", collect)

    runner.run_message_loop(
        listener_q,
        runner_state,
        warning_seconds=100,
        warning_repeat_seconds=100,
        hard_timeout_seconds=0,
    )

    process.assert_called_once_with(geo_ct_message, runner_state)
    assert monitor.call_count == 2
    assert collect.call_count == 2


def test_message_loop_continues_after_bad_message(monkeypatch, caplog, runner_state, geo_ct_message):
    """Tets the message loop continues after a bad message."""
    listener_q = Queue()
    bad = object()
    listener_q.put(bad)
    listener_q.put(geo_ct_message)
    listener_q.put(None)
    process = Mock(side_effect=[RuntimeError("bad message"), None])
    monkeypatch.setattr(runner, "process_message", process)

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        runner.run_message_loop(
            listener_q,
            runner_state,
            warning_seconds=100,
            warning_repeat_seconds=100,
            hard_timeout_seconds=0,
        )

    assert process.call_count == 2
    assert "Unhandled exception while processing message" in caplog.text


def test_message_loop_polls_again_after_timeout(monkeypatch, runner_state):
    """Tets the message loop polls again after a time out."""
    responses = iter([runner.NO_MESSAGE, None])
    get_next = Mock(side_effect=lambda queue: next(responses))
    monkeypatch.setattr(runner, "get_next_message", get_next)

    runner.run_message_loop(
        Queue(),
        runner_state,
        warning_seconds=100,
        warning_repeat_seconds=100,
        hard_timeout_seconds=0,
    )

    assert get_next.call_count == 2


def test_message_loop_propagates_stuck_pool_job(monkeypatch, runner_state):
    """Tets the message loop propagates a stuck pool job."""
    monkeypatch.setattr(
        runner,
        "monitor_pending_pool_jobs",
        Mock(side_effect=runner.StuckPoolJob("stuck")),
    )

    with pytest.raises(runner.StuckPoolJob, match="stuck"):
        runner.run_message_loop(
            Queue(),
            runner_state,
            warning_seconds=10,
            warning_repeat_seconds=10,
            hard_timeout_seconds=20,
        )


def test_live_runner_cleans_up_after_stuck_job(monkeypatch):
    """Tets the live runner cleans up after a stuck job."""
    manager = Mock()
    listener_q = Queue()
    publisher_q = Queue()
    manager.Queue.side_effect = [listener_q, publisher_q]

    pool = Mock()
    publisher = Mock()
    listener = Mock()

    monkeypatch.setattr(runner, "Manager", Mock(return_value=manager))
    monkeypatch.setattr(runner, "Pool", Mock(return_value=pool))
    monkeypatch.setattr(runner, "FilePublisher", Mock(return_value=publisher))
    monkeypatch.setattr(runner, "FileListener", Mock(return_value=listener))
    monkeypatch.setattr(
        runner,
        "run_message_loop",
        Mock(side_effect=runner.StuckPoolJob("stuck")),
    )

    with pytest.raises(runner.StuckPoolJob, match="stuck"):
        runner.mesan_live_runner({})

    publisher.start.assert_called_once()
    listener.start.assert_called_once()
    pool.terminate.assert_called_once()
    pool.join.assert_called_once()
    publisher.stop.assert_called_once()
    listener.stop.assert_called_once()
    publisher.join.assert_called_once_with(timeout=10)
    listener.join.assert_called_once_with(timeout=10)
    manager.shutdown.assert_called_once()
