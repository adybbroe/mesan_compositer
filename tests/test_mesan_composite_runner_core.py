"""Tests for message parsing, registration, and scene construction."""

from __future__ import annotations

import datetime as dt
from queue import Queue
from types import SimpleNamespace

import pytest

from mesan_compositer import mesan_composite_runner as runner


def test_make_scene_id():
    """Test make a scene ID."""
    start = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)

    assert runner.make_scene_id("CT", "Meteosat-10", 0, start) == (
        "CT_Meteosat-10_00000_202608032200"
    )


def test_register_job_requires_timezone_aware_datetime():
    """Test that registering a job requires a timezone aware datetime."""
    with pytest.raises(ValueError, match="timezone-aware"):
        runner.register_job("scene", {}, now=dt.datetime(2026, 8, 3, 22, 0))


def test_register_and_reset_job():
    """Test register and reset a job."""
    registered_at = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    jobs = {}

    runner.register_job("scene", jobs, now=registered_at)

    assert runner.job_is_registered("scene", jobs)
    assert jobs["scene"] == registered_at

    runner.reset_job_registry(jobs, "scene")
    runner.reset_job_registry(jobs, "scene")
    assert jobs == {}


def test_get_product_prefers_pge(geo_ct_message):
    """Test get product prefers PGE."""
    assert runner.get_product(geo_ct_message) == "CT"


def test_get_product_falls_back_to_uid(geo_ctth_message):
    """Test get product falls back to UID."""
    geo_ctth_message.data.pop("pge")
    assert runner.get_product(geo_ctth_message) == "CTTH"


def test_get_product_rejects_unknown_product(geo_ct_message):
    """Test get product rejects unknown product."""
    geo_ct_message.data["pge"] = "CMA"
    geo_ct_message.data["uid"] = "S_NWC_CMA_MSG3_test.nc"
    assert runner.get_product(geo_ct_message) is None


def test_get_scene_metadata_normalizes_geo_orbit_to_zero(geo_ct_message):
    """Test get scene metadata normalizes geo orbit number to zero."""
    metadata = runner.get_scene_metadata(geo_ct_message)

    assert metadata == {
        "platform_name": "Meteosat-10",
        "start_time": geo_ct_message.data["start_time"],
        "end_time": None,
        "orbit_number": 0,
        "sensor": ["seviri"],
    }


def test_get_scene_metadata_preserves_polar_orbit(monkeypatch, polar_ct_message):
    """Test get scene metadata preserves polar orbit number."""
    monkeypatch.setattr(runner, "POLAR_SATELLITES", ["NOAA-20"])

    metadata = runner.get_scene_metadata(polar_ct_message)

    assert metadata["orbit_number"] == 12345
    assert metadata["sensor"] == ["viirs"]


def test_get_scene_metadata_requires_sensor(geo_ct_message):
    """Test get scene metadata requires sensor."""
    geo_ct_message.data.pop("sensor")

    with pytest.raises(ValueError, match="missing sensor"):
        runner.get_scene_metadata(geo_ct_message)


def test_message_is_applicable_accepts_geo_seviri(geo_ct_message):
    """Test the check that message is applicable - accepts geo SEVIRI."""
    assert runner.message_is_applicable(geo_ct_message)


def test_message_is_applicable_rejects_wrong_geo_sensor(geo_ct_message):
    """Test the check that message is applicable - rejects wrong geo sensor name."""
    geo_ct_message.data["sensor"] = ["viirs"]
    assert not runner.message_is_applicable(geo_ct_message)


def test_message_is_applicable_accepts_supported_polar(monkeypatch, polar_ct_message):
    """Test the check that message is applicable - accepts supported polar satellite."""
    monkeypatch.setattr(runner, "POLAR_SATELLITES", ["NOAA-20"])
    assert runner.message_is_applicable(polar_ct_message)


def test_message_is_applicable_requires_time(geo_ct_message):
    """Test the check that message is applicable - requires a start time."""
    geo_ct_message.data.pop("start_time")
    assert not runner.message_is_applicable(geo_ct_message)


def test_ready2run_registers_new_scene(monkeypatch, geo_ct_message):
    """Test that the ready2run function registers new scene."""
    files = {}
    jobs = {}
    now = dt.datetime(2026, 8, 3, 22, 14, 49, tzinfo=dt.timezone.utc)
    input_file = geo_ct_message.data["uri"]
    monkeypatch.setattr(
        runner,
        "find_files_for_composite",
        lambda msg, product: input_file,
    )

    scene_id, scene_files = runner.ready2run(
        geo_ct_message,
        files,
        jobs,
        product="CT",
        now=now,
    )

    assert scene_id == "CT_Meteosat-10_00000_202608032200"
    assert scene_files == [input_file]
    assert jobs[scene_id] == now


def test_ready2run_does_not_register_invalid_file(monkeypatch, geo_ct_message):
    """Test that the ready2run function does not register invalid file."""
    monkeypatch.setattr(runner, "find_files_for_composite", lambda msg, product: None)
    jobs = {}

    assert runner.ready2run(geo_ct_message, {}, jobs, product="CT") is None
    assert jobs == {}


def test_ready2run_rejects_registered_scene(monkeypatch, geo_ct_message):
    """Test that the ready2run function rejects already registered scene."""
    start = geo_ct_message.data["start_time"]
    scene_id = runner.make_scene_id("CT", "Meteosat-10", 0, start)
    jobs = {scene_id: dt.datetime.now(dt.timezone.utc)}
    def find_files(msg, product):
        return msg.data["uri"]

    monkeypatch.setattr(runner, "find_files_for_composite", find_files)

    assert runner.ready2run(geo_ct_message, {}, jobs, product="CT") is None


def test_build_scene(geo_ct_message):
    """Test the build_scene."""
    metadata = runner.get_scene_metadata(geo_ct_message)

    scene = runner.build_scene(geo_ct_message, metadata, "CT")

    assert scene == {
        "platform_name": "Meteosat-10",
        "orbit_number": 0,
        "starttime": geo_ct_message.data["start_time"],
        "endtime": None,
        "sensor": ["seviri"],
        "filename": geo_ct_message.data["uri"],
        "product": "CT",
    }


def test_get_next_message_reads_real_queue():
    """Test that get_next_message reads the real queue."""
    queue = Queue()
    expected = object()
    queue.put(expected)

    assert runner.get_next_message(queue) is expected


def test_get_next_message_returns_sentinel_on_timeout(monkeypatch):
    """Test the get_next_message returns sentinel on timeout."""
    class EmptyQueue:
        def get(self, timeout):
            assert timeout == 1.0
            from queue import Empty

            raise Empty

    assert runner.get_next_message(EmptyQueue()) is runner.NO_MESSAGE


def test_find_files_for_composite_ignores_non_file_message(tmp_path):
    """Test that find_files_for_composite ignores non-file message."""
    msg = SimpleNamespace(type="dataset", data={"uri": tmp_path / "a.nc"})
    assert runner.find_files_for_composite(msg, "CT") is None
