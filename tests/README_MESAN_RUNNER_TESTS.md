# Pytest suite for the supervised MESAN live runner

This suite targets the `mesan_composite_runner.py` rewrite that retains and
collects the objects returned by `Pool.apply_async()`.

It assumes the rewrite exports these names:

- `PendingPoolJob` and `StuckPoolJob`
- `submit_pool_job()`
- `collect_pool_results()`
- `monitor_pending_pool_jobs()`
- the revised `ctype_composite_worker()` and `ctth_composite_worker()`
- the revised `mesan_live_runner()`

The test suite is based on repository commit
`df02585b9c26584a097efee3acbadce20bb56cf4` and the accompanying
`mesan_asyncresult_rewrite_sections.py` file.

## Install

Copy the files under `tests/` into the repository's existing `tests/`
directory. The archive supplied with this README already has that layout.

From the repository root, install the package and test dependencies:

```bash
python -m pip install -e '.[test]'
```

## Run the deterministic unit tests

```bash
python -m pytest -q \
  tests/test_mesan_composite_runner_async.py \
  tests/test_mesan_composite_runner_workers.py \
  tests/test_mesan_composite_runner_live.py
```

The tests do not start Posttroll, Satpy, Manager processes, service threads,
or real composite generation. Those dependencies are replaced with
controllable fakes, so failures are deterministic and fast.

The live-loop tests feed a scripted sequence of messages into a fake listener
queue. A final `None` item uses the runner's shutdown sentinel, allowing the
otherwise infinite loop to exit normally.

## Run the optional real-pool smoke tests

Two additional tests create a real one-process `multiprocessing` pool using
the `spawn` context. They verify that both a normal result and a child-process
exception flow through the callback queue and `AsyncResult.get()`.

```bash
MESAN_RUN_MULTIPROCESSING_TESTS=1 \
python -m pytest -q \
  tests/test_mesan_composite_runner_pool_smoke.py
```

These are smoke tests rather than unit tests. They are skipped unless the
environment variable is set, which keeps the normal test suite stable on
restricted CI workers.

## Coverage of the rewrite

The suite checks the following contracts:

### AsyncResult supervision

- successful and failed callback notifications;
- retention of each returned `AsyncResult`;
- duplicate-submission rejection;
- synchronous `apply_async()` failure;
- parent-side `.get()` for success and remote exceptions;
- fallback collection when a callback event is absent;
- callback-before-ready and callback-before-registration races;
- stale completion tokens;
- stuck-job warnings, warning rate limiting, and hard timeout;
- diagnostic pool-worker snapshots;
- safe job-registry removal.

### Worker entry points

- CT and CTTH success paths;
- publication only when a composite file exists;
- no-result behavior;
- returned, picklable result records;
- configured and default area/time-window behavior;
- legacy non-datetime registry values;
- re-raising processing exceptions so the parent can observe them.

### Live runner

- CT and CTTH routing;
- product inference from `uid` when `pge` is absent;
- geostationary and polar scene/key construction;
- unchanged one-worker pool settings;
- one-second listener polling;
- result collection on later loop iterations;
- duplicate suppression while a job remains pending;
- `ready2run()` rejection;
- malformed-message isolation and continuation;
- unsupported-product cleanup;
- cleanup after synchronous submission failure;
- cleanup after an unexpected per-message exception;
- hard-timeout propagation;
- orderly pool, thread, and Manager shutdown;
- five-minute registry timer creation without starting a real timer thread.

## Validation performed when the suite was generated

Against a local harness containing the supplied rewrite sections:

```text
39 passed, 2 skipped
```

With the optional real multiprocessing tests enabled:

```text
2 passed
```

Coverage reported 96% for the complete harness module. Its 11 uncovered lines
were placeholder functions used only to stand in for unrelated project
components; every executable line in the supplied rewrite sections was
exercised.

This validation does **not** claim that the complete upstream repository or
its existing tests passed here. The full repository and its runtime satellite
stack were not available in the execution environment. Run both this suite
and the project's pre-existing tests in the same environment used for MESAN
development or CI:

```bash
python -m pytest -q
```

## Expected warnings

On Python 3.13 or newer, the existing use of `datetime.utcnow()` may emit a
deprecation warning. That warning does not indicate a failure in the
AsyncResult rewrite. It can be addressed separately by making the runner's
UTC timestamps timezone-aware throughout rather than changing only one call.
