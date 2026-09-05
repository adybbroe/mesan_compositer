"""Tests for common CompositeWorker behaviour and product-specific dispatch."""

from __future__ import annotations

import datetime as dt
import logging
import pickle
from queue import Empty, Queue
from unittest.mock import Mock

import pytest
from posttroll.message import Message

from mesan_compositer import mesan_composite_runner as runner


@pytest.fixture
def scene():
    """Create a scene for testing."""
    return {
        "platform_name": "Meteosat-10",
        "orbit_number": 0,
        "starttime": dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc),
        "endtime": None,
        "sensor": ["seviri"],
        "filename": "/CT/S_NWC_CT_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
        "product": "CT",
    }


class DummyCompositeWorker(runner.CompositeWorker):
    """Make a dummy composite worker for testing."""

    product = "TEST"

    def __init__(self, *, result_file="/output/test.nc", superobs_file="/output/test.dat"):
        """Initialize the object."""
        self.result_file = result_file
        self.superobs_file = superobs_file
        self.composite_calls = []
        self.superobs_calls = []

    def make_composite(self, time_of_analysis, delta_t, config_options):
        """Make the composite."""
        self.composite_calls.append((time_of_analysis, delta_t, config_options))
        return self.result_file

    def make_super_observations(self, result_file, time_of_analysis, config_options):
        """Make the super observations."""
        self.superobs_calls.append((result_file, time_of_analysis, config_options))
        return self.superobs_file


def _worker_config(*, generate=True):
    return {
        "absolute_time_threshold_minutes": 35,
        "mesan_area_id": "mesanEx",
        "generate_superobservations_live_runner": {
            "test": {
                "name": "TEST",
                "generate": generate,
            }
        },
    }


def test_base_worker_publishes_and_returns_picklable_success(monkeypatch, scene):
    """Test base worker publishes and returns picklable on success."""
    worker = DummyCompositeWorker()
    publish_q = Queue()
    analysis_time = scene["starttime"]

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(worker, "create_message", Mock(return_value=b"posttroll-message"))
    monkeypatch.setattr(runner.os, "getpid", lambda: 9876)

    config = _worker_config(generate=True)
    result = worker(scene, dt.datetime.now(dt.timezone.utc), publish_q, config)

    assert worker.area_id == "mesanEx"
    assert worker.composite_calls == [
        (analysis_time, dt.timedelta(minutes=35), config)
    ]
    assert worker.superobs_calls == [
        ("/output/test.nc", analysis_time, config)
    ]
    worker.create_message.assert_called_once_with("/output/test.nc", scene)
    assert publish_q.get_nowait() == b"posttroll-message"
    assert result == {
        "status": "success",
        "product": "TEST",
        "worker_pid": 9876,
        "result_file": "/output/test.nc",
        "super_obs_file": "/output/test.dat",
    }
    assert pickle.loads(pickle.dumps(result)) == result  # noqa: S301


def test_base_worker_skips_superobs_when_disabled(monkeypatch, scene):
    """Test base worker skips super obbing when disabled."""
    worker = DummyCompositeWorker()
    publish_q = Queue()

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))
    monkeypatch.setattr(worker, "create_message", Mock(return_value=b"message"))

    result = worker(scene, dt.datetime.now(dt.timezone.utc), publish_q, _worker_config(generate=False))

    assert worker.superobs_calls == []
    assert result["status"] == "success"
    assert result["super_obs_file"] is None
    assert publish_q.get_nowait() == b"message"


def test_base_worker_without_superobs_configuration(monkeypatch, scene):
    """Test base worker without super obs configuration."""
    worker = DummyCompositeWorker()
    publish_q = Queue()

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))
    monkeypatch.setattr(worker, "create_message", Mock(return_value=b"message"))

    result = worker(scene, dt.datetime.now(dt.timezone.utc), publish_q, {})

    assert worker.superobs_calls == []
    assert result["status"] == "success"
    assert result["super_obs_file"] is None
    assert publish_q.get_nowait() == b"message"


def test_base_worker_returns_no_result_without_publishing(monkeypatch, scene):
    """Test base worker returns no result without publishing."""
    worker = DummyCompositeWorker(result_file=None)
    publish_q = Queue()
    create_message = Mock()

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))
    monkeypatch.setattr(worker, "create_message", create_message)
    monkeypatch.setattr(runner.os, "getpid", lambda: 123)

    result = worker(scene, "legacy-job-value", publish_q, {})

    assert result == {
        "status": "no_result",
        "product": "TEST",
        "worker_pid": 123,
        "result_file": None,
        "super_obs_file": None,
    }
    create_message.assert_not_called()
    with pytest.raises(Empty):
        publish_q.get_nowait()


def test_base_worker_uses_default_area(monkeypatch, scene):
    """Test default area is stored on the worker."""
    worker = DummyCompositeWorker(result_file=None)

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))

    worker(scene, "job", Queue(), {})

    assert worker.area_id == runner.DEFAULT_AREA
    assert worker.composite_calls == [
        (scene["starttime"], dt.timedelta(minutes=30), {})
    ]


def test_base_worker_logs_non_datetime_job_id(monkeypatch, caplog, scene):
    """Test warning for legacy/non-datetime job id."""
    worker = DummyCompositeWorker()

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))
    monkeypatch.setattr(worker, "create_message", Mock(return_value=b"message"))

    with caplog.at_level(logging.WARNING, logger=runner.LOG.name):
        result = worker(scene, "legacy-value", Queue(), {})

    assert result["status"] == "success"
    assert "Job entry is not a datetime instance" in caplog.text


def test_base_worker_reraises_processing_exception(monkeypatch, caplog, scene):
    """Test worker processing exceptions are reraised."""
    worker = DummyCompositeWorker()
    worker.make_composite = Mock(side_effect=RuntimeError("composite failed"))

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=scene["starttime"]))

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        with pytest.raises(RuntimeError, match="composite failed"):
            worker(scene, dt.datetime.now(dt.timezone.utc), Queue(), {})

    assert "Failed in TEST composite worker" in caplog.text


def test_cloud_type_worker_dispatches_to_product_functions(monkeypatch, scene):
    """Test CT worker dispatch."""
    analysis_time = scene["starttime"]
    composite = Mock(return_value="/output/ct.nc")
    superobs = Mock(return_value="/output/clamount.dat")

    monkeypatch.setattr(runner, "do_cloud_type_composite", composite)
    monkeypatch.setattr(runner, "do_cloudamount", superobs)

    worker = runner.CloudTypeCompositeWorker()
    worker.area_id = "mesanEx"
    config = {}
    delta_t = dt.timedelta(minutes=35)

    assert worker.make_composite(analysis_time, delta_t, config) == "/output/ct.nc"
    assert (
        worker.make_super_observations("/output/ct.nc", analysis_time, config)
        == "/output/clamount.dat"
    )

    composite.assert_called_once_with(analysis_time, delta_t, "mesanEx", config)
    superobs.assert_called_once_with("/output/ct.nc", analysis_time, "mesanEx", config)


def test_ctth_worker_dispatches_to_product_functions(monkeypatch, scene):
    """Test CTTH worker dispatch."""
    analysis_time = scene["starttime"]
    composite = Mock(return_value="/output/ctth.nc")
    superobs = Mock(return_value="/output/clheight.dat")

    monkeypatch.setattr(runner, "do_ctth_composite", composite)
    monkeypatch.setattr(runner, "do_cloudheight", superobs)

    worker = runner.CloudTopHeightCompositeWorker()
    worker.area_id = "mesanEx"
    config = {}
    delta_t = dt.timedelta(minutes=35)

    assert worker.make_composite(analysis_time, delta_t, config) == "/output/ctth.nc"
    assert (
        worker.make_super_observations("/output/ctth.nc", analysis_time, config)
        == "/output/clheight.dat"
    )

    composite.assert_called_once_with(analysis_time, delta_t, "mesanEx", config)
    superobs.assert_called_once_with("/output/ctth.nc", analysis_time, "mesanEx", config)

def test_create_message_contains_area(scene):
    """Test created Posttroll message contains product and area."""
    worker = runner.CloudTypeCompositeWorker()
    worker.area_id = "mesanEx"

    encoded = worker.create_message("/data/mesan/mesan_ct.nc", scene)

    msg = Message.from_string(encoded)

    assert msg.data["product"] == "CT"
    assert msg.data["area"] == "mesanEx"
    assert msg.data["uri"] == "/data/mesan/mesan_ct.nc"
    assert msg.data["uid"] == "mesan_ct.nc"
