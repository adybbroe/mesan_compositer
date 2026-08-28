"""Regression tests for the CT and CTTH pool worker entry points."""

from __future__ import annotations

import datetime as dt
import logging
import pickle
import socket
from unittest.mock import Mock

import pytest
from _mesan_runner_fakes import ScriptedQueue

from mesan_compositer import mesan_composite_runner as runner


@pytest.fixture
def scene():
    """Fixture to create a Mesan-compositer input scene."""
    return {
        "platform_name": "Meteosat-10",
        "orbit_number": "00000",
        "starttime": dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc),
        "endtime": None,
        "sensor": "['seviri']",
        "filename": "/CT/S_NWC_CT_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
        "product": "CT",
    }


@pytest.fixture
def config():
    """Fixture to create a default Mesan-runner configuration."""
    return {
        "absolute_time_threshold_minutes": "35",
        "mesan_area_id": "mesanEx",
        "number_of_pixels": 24,
    }


def test_ctype_worker_publishes_and_returns_picklable_success_record(
    monkeypatch,
    scene,
    config,
):
    """Test that the the Ctype worker publishes and returns picklable success-record."""
    analysis_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc),
    composite = Mock(return_value="/output/ct.nc")
    superobs = Mock(return_value="/output/cloud_amount.dat")
    create_message = Mock(return_value=b"encoded-posttroll-message")
    publish_queue = ScriptedQueue()
    servername = socket.gethostname() # 'myfake.server.se'

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(runner, "do_cloud_type_composite", composite)
    monkeypatch.setattr(runner, "do_cloudamount", superobs)
    monkeypatch.setattr(runner, "create_message", create_message)
    monkeypatch.setattr(runner.os, "getpid", lambda: 9876)

    result = runner.ctype_composite_worker(
        scene,
        dt.datetime.now(dt.timezone.utc),
        publish_queue,
        config,
    )

    composite.assert_called_once_with(
        analysis_time,
        dt.timedelta(minutes=35),
        "mesanEx",
        config,
    )
    create_message.assert_called_once_with("/output/ct.nc", scene, servername)
    superobs.assert_called_once_with(
        "/output/ct.nc",
        analysis_time,
        "mesanEx",
        config,
    )
    assert publish_queue.put_items == [b"encoded-posttroll-message"]
    assert result == {
        "status": "success",
        "product": "CT",
        "worker_pid": 9876,
        "result_file": "/output/ct.nc",
        "super_obs_file": "/output/cloud_amount.dat",
    }
    assert pickle.loads(pickle.dumps(result)) == result  # noqa: S301


def test_ctype_worker_uses_default_area_when_configuration_omits_it(monkeypatch, scene):
    """Test that the Ctype worker uses default area when not configured."""
    analysis_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    composite = Mock(return_value=None)

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(runner, "do_cloud_type_composite", composite)
    monkeypatch.setattr(runner.os, "getpid", lambda: 111)

    result = runner.ctype_composite_worker(
        scene,
        "not-a-datetime",
        ScriptedQueue(),
        {},
    )

    composite.assert_called_once_with(
        analysis_time,
        dt.timedelta(minutes=30),
        runner.DEFAULT_AREA,
        {},
    )
    assert result["status"] == "no_result"
    assert result["worker_pid"] == 111


def test_ctype_worker_does_not_publish_or_generate_superobs_without_result(
    monkeypatch,
    scene,
    config,
):
    """Test that the Ctype worker does not publish or try generate superobs without a result."""
    create_message = Mock()
    superobs = Mock()
    publish_queue = ScriptedQueue()

    monkeypatch.setattr(
        runner,
        "get_analysis_time",
        Mock(return_value=dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc))
    )
    monkeypatch.setattr(runner, "do_cloud_type_composite", Mock(return_value=None))
    monkeypatch.setattr(runner, "create_message", create_message)
    monkeypatch.setattr(runner, "do_cloudamount", superobs)
    monkeypatch.setattr(runner.os, "getpid", lambda: 222)

    result = runner.ctype_composite_worker(
        scene,
        dt.datetime.now(dt.timezone.utc),
        publish_queue,
        config,
    )

    assert result == {
        "status": "no_result",
        "product": "CT",
        "worker_pid": 222,
        "result_file": None,
        "super_obs_file": None,
    }
    create_message.assert_not_called()
    superobs.assert_not_called()
    assert publish_queue.put_items == []


def test_ctype_worker_reraises_processing_exception_for_asyncresult(
    monkeypatch,
    caplog,
    scene,
    config,
):
    """Test that the Ctype worker re-raises processing exception for async-result."""
    monkeypatch.setattr(
        runner,
        "get_analysis_time",
        Mock(return_value=dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc))
    )
    monkeypatch.setattr(
        runner,
        "do_cloud_type_composite",
        Mock(side_effect=RuntimeError("composite failed")),
    )

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        with pytest.raises(RuntimeError, match="composite failed"):
            runner.ctype_composite_worker(
                scene,
                dt.datetime.now(dt.timezone.utc),
                ScriptedQueue(),
                config,
            )

    assert "Failed in ctype_composite_worker" in caplog.text


def test_ctth_worker_publishes_and_returns_success_record(monkeypatch, scene, config):
    """Test that the CTTH worker publishes and returns a success record."""
    scene = dict(scene, product="CTTH")
    analysis_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    composite = Mock(return_value="/output/ctth.nc")
    superobs = Mock(return_value="/output/cloud_height.dat")
    create_message = Mock(return_value=b"ctth-message")
    publish_queue = ScriptedQueue()
    servername = socket.gethostname() # 'myfake.server.se'

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(runner, "do_ctth_composite", composite)
    monkeypatch.setattr(runner, "do_cloudheight", superobs)
    monkeypatch.setattr(runner, "create_message", create_message)
    monkeypatch.setattr(runner.os, "getpid", lambda: 5432)

    result = runner.ctth_composite_worker(
        scene,
        dt.datetime.now(dt.timezone.utc),
        publish_queue,
        config,
    )

    composite.assert_called_once_with(
        analysis_time,
        dt.timedelta(minutes=35),
        "mesanEx",
        config,
    )
    create_message.assert_called_once_with("/output/ctth.nc", scene, servername)
    superobs.assert_called_once_with(
        "/output/ctth.nc",
        analysis_time,
        "mesanEx",
        config,
    )
    assert publish_queue.put_items == [b"ctth-message"]
    assert result == {
        "status": "success",
        "product": "CTTH",
        "worker_pid": 5432,
        "result_file": "/output/ctth.nc",
        "super_obs_file": "/output/cloud_height.dat",
    }


def test_ctth_worker_does_not_publish_without_result(monkeypatch, scene, config):
    """Test that the CTTH worker dows not publish without a result."""
    scene = dict(scene, product="CTTH")
    create_message = Mock()
    superobs = Mock()
    publish_queue = ScriptedQueue()

    monkeypatch.setattr(
        runner,
        "get_analysis_time",
        Mock(return_value=dt.datetime(2026, 8, 3, 22, 0), tzinfo=dt.timezone.utc)
    )
    monkeypatch.setattr(runner, "do_ctth_composite", Mock(return_value=None))
    monkeypatch.setattr(runner, "create_message", create_message)
    monkeypatch.setattr(runner, "do_cloudheight", superobs)
    monkeypatch.setattr(runner.os, "getpid", lambda: 333)

    result = runner.ctth_composite_worker(
        scene,
        dt.datetime.now(dt.timezone.utc),
        publish_queue,
        config,
    )

    assert result["status"] == "no_result"
    assert result["product"] == "CTTH"
    create_message.assert_not_called()
    superobs.assert_not_called()
    assert publish_queue.put_items == []


def test_ctth_worker_reraises_processing_exception_for_asyncresult(
    monkeypatch,
    caplog,
    scene,
    config,
):
    """Test the CTTH worker: That it re-raises processing exception for async-result."""
    scene = dict(scene, product="CTTH")
    monkeypatch.setattr(
        runner,
        "get_analysis_time",
        Mock(return_value=dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc))
    )
    monkeypatch.setattr(
        runner,
        "do_ctth_composite",
        Mock(side_effect=OSError("output filesystem unavailable")),
    )

    with caplog.at_level(logging.ERROR, logger=runner.LOG.name):
        with pytest.raises(OSError, match="filesystem unavailable"):
            runner.ctth_composite_worker(
                scene,
                dt.datetime.now(dt.timezone.utc),
                ScriptedQueue(),
                config,
            )

    assert "Failed in ctth_composite_worker" in caplog.text


def test_ctype_worker_success_tolerates_non_datetime_registry_value(
    monkeypatch,
    caplog,
    scene,
    config,
):
    """Test the Ctype worker: That it tolerates non-datetime registry value."""
    analysis_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(
        runner,
        "do_cloud_type_composite",
        Mock(return_value="/output/ct.nc"),
    )
    monkeypatch.setattr(runner, "create_message", Mock(return_value=b"message"))
    monkeypatch.setattr(
        runner,
        "do_cloudamount",
        Mock(return_value="/output/cloud_amount.dat"),
    )
    monkeypatch.setattr(runner.os, "getpid", lambda: 444)

    with caplog.at_level(logging.WARNING, logger=runner.LOG.name):
        result = runner.ctype_composite_worker(
            scene,
            "legacy-non-datetime-value",
            ScriptedQueue(),
            config,
        )

    assert result["status"] == "success"
    assert "Job entry is not a datetime instance" in caplog.text


def test_ctth_worker_uses_default_area_and_tolerates_non_datetime_job_id(
    monkeypatch,
    caplog,
    scene,
):
    """Test the CTTH worker: That is uses default area and tolerates non-datetime-job-id."""
    scene = dict(scene, product="CTTH")
    analysis_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    composite = Mock(return_value="/output/ctth.nc")

    monkeypatch.setattr(runner, "get_analysis_time", Mock(return_value=analysis_time))
    monkeypatch.setattr(runner, "do_ctth_composite", composite)
    monkeypatch.setattr(runner, "create_message", Mock(return_value=b"message"))
    monkeypatch.setattr(
        runner,
        "do_cloudheight",
        Mock(return_value="/output/cloud_height.dat"),
    )
    monkeypatch.setattr(runner.os, "getpid", lambda: 555)

    with caplog.at_level(logging.WARNING, logger=runner.LOG.name):
        result = runner.ctth_composite_worker(
            scene,
            "legacy-non-datetime-value",
            ScriptedQueue(),
            {},
        )

    composite.assert_called_once_with(
        analysis_time,
        dt.timedelta(minutes=30),
        runner.DEFAULT_AREA,
        {},
    )
    assert result["status"] == "success"
    assert "No area id specified" in caplog.text
    assert "Job entry is not a datetime instance" in caplog.text
