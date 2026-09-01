"""Small deterministic fakes used by the MESAN live-runner tests."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from queue import Empty
from types import SimpleNamespace
from typing import Any, Callable, Iterable


class FakeAsyncResult:
    """Controllable stand-in for ``multiprocessing.pool.AsyncResult``."""

    def __init__(
        self,
        *,
        ready: bool = False,
        value: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        self.ready_value = ready
        self.value = value
        self.exception = exception
        self.ready_calls = 0
        self.get_calls = 0

    def ready(self) -> bool:
        self.ready_calls += 1
        return self.ready_value

    def get(self, timeout: float | None = None) -> Any:
        del timeout
        self.get_calls += 1
        if self.exception is not None:
            raise self.exception
        return self.value


@dataclass
class FakeProcess:
    """Minimal object exposed through the diagnostic ``Pool._pool`` list."""

    pid: int = 4242
    alive: bool = True
    exitcode: int | None = None

    def is_alive(self) -> bool:
        return self.alive


class RecordingPool:
    """Pool fake that records every ``apply_async`` submission."""

    def __init__(
        self,
        *args: Any,
        async_results: Iterable[FakeAsyncResult] | None = None,
        submission_exception: BaseException | None = None,
        **kwargs: Any,
    ) -> None:
        self.init_args = args
        self.init_kwargs = kwargs
        self.submission_exception = submission_exception
        self.async_results = deque(async_results or [FakeAsyncResult()])
        self.apply_async_calls: list[SimpleNamespace] = []
        self.terminate_calls = 0
        self.join_calls = 0
        self.close_calls = 0
        self._pool = [FakeProcess()]

    def apply_async(
        self,
        worker: Callable[..., Any],
        args: tuple[Any, ...] = (),
        callback: Callable[[Any], None] | None = None,
        error_callback: Callable[[BaseException], None] | None = None,
    ) -> FakeAsyncResult:
        if self.submission_exception is not None:
            raise self.submission_exception
        if not self.async_results:
            raise AssertionError("No FakeAsyncResult configured for this submission")

        result = self.async_results.popleft()
        self.apply_async_calls.append(
            SimpleNamespace(
                worker=worker,
                args=args,
                callback=callback,
                error_callback=error_callback,
                async_result=result,
            )
        )
        return result

    def terminate(self) -> None:
        self.terminate_calls += 1

    def join(self) -> None:
        self.join_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class ScriptedQueue:
    """Queue whose ``get`` calls return a pre-programmed sequence."""

    def __init__(self, items: Iterable[Any] = ()) -> None:
        self.items = deque(items)
        self.get_timeouts: list[float | None] = []
        self.put_items: list[Any] = []

    def get(self, timeout: float | None = None) -> Any:
        self.get_timeouts.append(timeout)
        if not self.items:
            raise Empty
        item = self.items.popleft()
        if isinstance(item, BaseException):
            raise item
        return item

    def put(self, item: Any) -> None:
        self.put_items.append(item)


class RecordingManager:
    """Manager fake returning the supplied listener and publisher queues."""

    def __init__(self, listener_queue: ScriptedQueue, publisher_queue: ScriptedQueue) -> None:
        self.queues = deque([listener_queue, publisher_queue])
        self.queue_calls = 0
        self.shutdown_calls = 0

    def Queue(self) -> ScriptedQueue:  # noqa: N802 - match multiprocessing API
        self.queue_calls += 1
        if not self.queues:
            raise AssertionError("Manager.Queue() called more than twice")
        return self.queues.popleft()

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class RecordingThread:
    """Thread fake used for both listener and publisher service threads."""

    instances: list["RecordingThread"] = []

    def __init__(self, queue: Any) -> None:
        self.queue = queue
        self.start_calls = 0
        self.stop_calls = 0
        self.join_timeouts: list[float | None] = []
        self.__class__.instances.append(self)

    def start(self) -> None:
        self.start_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)


class RecordingTimer:
    """Timer fake that records scheduling without starting a real thread."""

    instances: list["RecordingTimer"] = []

    def __init__(
        self,
        interval: float,
        function: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs or {}
        self.daemon = False
        self.start_calls = 0
        self.__class__.instances.append(self)

    def start(self) -> None:
        self.start_calls += 1


class RecordingLogger:
    """Logger double useful when testing helper functions in isolation."""

    def __init__(self) -> None:
        self.debug_calls: list[tuple[Any, ...]] = []
        self.info_calls: list[tuple[Any, ...]] = []
        self.warning_calls: list[tuple[Any, ...]] = []
        self.error_calls: list[tuple[Any, ...]] = []
        self.exception_calls: list[tuple[Any, ...]] = []
        self.critical_calls: list[tuple[Any, ...]] = []

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.debug_calls.append((*args, kwargs))

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.info_calls.append((*args, kwargs))

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.warning_calls.append((*args, kwargs))

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.error_calls.append((*args, kwargs))

    def exception(self, *args: Any, **kwargs: Any) -> None:
        self.exception_calls.append((*args, kwargs))

    def critical(self, *args: Any, **kwargs: Any) -> None:
        self.critical_calls.append((*args, kwargs))


def make_thread_class(name: str) -> type[RecordingThread]:
    """Create a thread-fake subclass with its own instance registry."""
    return type(name, (RecordingThread,), {"instances": []})


def make_geo_message(
    *,
    product: str = "CT",
    start_time: Any,
    end_time: Any = None,
    include_pge: bool = True,
) -> SimpleNamespace:
    """Build a representative accepted NWCSAF/Geo Posttroll-like message."""
    uid = (
        f"S_NWC_{product}_MSG3_MSG-N-VISIR_"
        f"{start_time:%Y%m%dT%H%M%SZ}_PLAX.nc"
    )
    data = {
        "platform_name": "Meteosat-10",
        "nominal_time": start_time,
        "uri": f"/CT/{uid}",
        "uid": uid,
        "sensor": ["seviri"],
    }
    if end_time is not None:
        data["end_time"] = end_time
    if include_pge:
        data["pge"] = product
    return SimpleNamespace(type="file", data=data)


def make_polar_message(
    *,
    product: str = "CT",
    start_time: Any,
    orbit_number: int = 12345,
) -> SimpleNamespace:
    """Build a representative polar Posttroll-like message."""
    uid = (
        f"S_NWC_{product}_noaa20_{orbit_number:05d}_"
        f"{start_time:%Y%m%dT%H%M%SZ}_{start_time:%Y%m%dT%H%M%SZ}.nc"
    )
    return SimpleNamespace(
        type="file",
        data={
            "pge": product,
            "platform_name": "NOAA-20",
            "orbit_number": orbit_number,
            "start_time": start_time,
            "end_time": start_time,
            "uri": f"/PPS/{uid}",
            "uid": uid,
            "sensor": ["viirs"],
        },
    )
