"""Unit tests for the measurement toolkit (T1-T6 + models and sink)."""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
from datetime import datetime, timezone

import numpy as np
import pytest

from rgw_ingest_bench import metrics
from rgw_ingest_bench.metrics import (
    CounterSet,
    EventLoopLagProbe,
    LatencyRecorder,
    LatencySummary,
    PeriodicSampler,
    RttStats,
    RunResult,
    append_result,
    collect_env_info,
    git_sha,
    package_versions,
    read_rss_mib,
    throughput_summary,
)


# --------------------------------------------------------------------------- T1
def test_counterset_basic() -> None:
    counters = CounterSet()
    counters.incr("a")
    counters.incr("a", 4)
    counters.incr("b", 2)
    assert counters.snapshot() == {"a": 5, "b": 2}


def test_counterset_thread_safe() -> None:
    """Concurrent increments from many threads sum exactly (the lock works)."""
    counters = CounterSet()
    n_threads, per_thread = 8, 2000

    def worker() -> None:
        for _ in range(per_thread):
            counters.incr("hits")

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert counters.snapshot()["hits"] == n_threads * per_thread


# --------------------------------------------------------------------------- T2
def test_latency_recorder_quantiles() -> None:
    recorder = LatencyRecorder("put")
    data = [1.0, 2.0, 3.0, 4.0, 100.0]
    for value in data:
        recorder.record(value)
    summary = recorder.summarize()
    arr = np.asarray(data)
    assert summary.op == "put"
    assert summary.count == 5
    assert summary.p50_ms == pytest.approx(float(np.quantile(arr, 0.5)))
    assert summary.p95_ms == pytest.approx(float(np.quantile(arr, 0.95)))
    assert summary.p99_ms == pytest.approx(float(np.quantile(arr, 0.99)))
    assert summary.max_ms == 100.0


def test_latency_recorder_empty_is_nan_free() -> None:
    summary = LatencyRecorder("get").summarize()
    assert summary.count == 0
    for value in (summary.p50_ms, summary.p95_ms, summary.p99_ms, summary.max_ms):
        assert value == 0.0
        assert not math.isnan(value)


# --------------------------------------------------------------------------- T3
async def test_periodic_sampler_collects() -> None:
    ticks = {"n": 0}

    def probe() -> float:
        ticks["n"] += 1
        return float(ticks["n"])

    async with PeriodicSampler({"c": probe}, interval_s=0.005) as sampler:
        await asyncio.sleep(0.06)
    rows = sampler.to_df().to_dicts()
    assert len(rows) >= 3
    assert {row["name"] for row in rows} == {"c"}


async def test_periodic_sampler_survives_failing_probe(caplog) -> None:
    """A raising probe is logged once and skipped; the sampler keeps running."""

    def bad() -> float:
        raise RuntimeError("boom")

    good_ticks = {"n": 0}

    def good() -> float:
        good_ticks["n"] += 1
        return 1.0

    with caplog.at_level(logging.ERROR):
        async with PeriodicSampler(
            {"bad": bad, "good": good}, interval_s=0.005
        ) as sampler:
            await asyncio.sleep(0.05)

    failures = [r for r in caplog.records if "probe 'bad' failed" in r.getMessage()]
    assert len(failures) == 1  # logged once despite many failing calls
    rows = sampler.to_df().to_dicts()
    assert sum(row["name"] == "good" for row in rows) >= 2  # survived
    assert sum(row["name"] == "bad" for row in rows) == 0  # no bad samples kept


# --------------------------------------------------------------------------- T4
async def test_loop_lag_probe_detects_block() -> None:
    async with EventLoopLagProbe(interval_s=0.02) as probe:
        await asyncio.sleep(0.03)  # let a few clean samples land
        time.sleep(0.3)  # block the event loop ~300 ms
        await asyncio.sleep(0.05)  # let the overrun be recorded
    assert probe.lags_ms  # samples were taken
    assert probe.max_lag_ms() > 250


def test_loop_lag_probe_max_empty() -> None:
    assert EventLoopLagProbe().max_lag_ms() == 0.0


async def test_cancel_none_is_noop() -> None:
    """The shared _cancel helper returns cleanly when there is no task."""
    await metrics._cancel(None)


# --------------------------------------------------------------------------- T5
def test_rss_probe_proc_path() -> None:
    """On Linux the /proc/self/statm path is taken and yields a positive RSS."""
    assert read_rss_mib() > 0


def test_rss_probe_getrusage_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """With /proc absent, the getrusage fallback is used."""

    class _NoProcPath:
        def __init__(self, *args: object) -> None:
            pass

        def exists(self) -> bool:
            return False

    monkeypatch.setattr(metrics, "Path", _NoProcPath)
    assert read_rss_mib() > 0


# --------------------------------------------------------------------------- T6
def test_env_info_complete() -> None:
    rtt = RttStats(median_ms=1.0, iqr_ms=0.1, samples=21)
    env = collect_env_info(
        store_kind="rgw",
        rtt=rtt,
        corpus_tier="small",
        corpus_seed=42,
        netem_nominal="1ms",
    )
    assert set(env.package_versions) == set(metrics.TRACKED_PACKAGES)
    assert all(value for value in env.package_versions.values())
    assert env.store_kind == "rgw"
    assert env.rtt == rtt
    assert env.netem_nominal == "1ms"
    assert env.corpus_tier == "small"
    assert env.corpus_seed == 42
    assert env.python and env.platform and env.kernel


def test_git_sha_matches_repo() -> None:
    import subprocess

    expected = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    assert git_sha() == expected


def test_git_sha_unknown_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: object, **kwargs: object) -> None:
        raise OSError("git not found")

    monkeypatch.setattr(metrics.subprocess, "run", boom)
    assert git_sha() == "unknown"


def test_package_versions_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    from importlib.metadata import PackageNotFoundError

    def boom(name: str) -> str:
        raise PackageNotFoundError(name)

    monkeypatch.setattr(metrics.importlib.metadata, "version", boom)
    versions = package_versions()
    assert set(versions) == set(metrics.TRACKED_PACKAGES)
    assert all(value == "unknown" for value in versions.values())


# ----------------------------------------------------------------- sink / summary
def _make_result() -> RunResult:
    env = collect_env_info(
        store_kind="minio",
        rtt=RttStats(median_ms=1.0, iqr_ms=0.0, samples=1),
        corpus_tier="tiny",
        corpus_seed=7,
    )
    return RunResult(
        run_id="run-1",
        started_at=datetime.now(timezone.utc),
        command="seed",
        variant=None,
        params={"jobs": 4},
        wall_s=1.5,
        files=6,
        files_per_s=4.0,
        counters={"put_count": 6},
        latencies=[
            LatencySummary(op="put", count=6, p50_ms=1, p95_ms=2, p99_ms=3, max_ms=4)
        ],
        env=env,
    )


def test_append_result_creates_dir_and_appends(tmp_path) -> None:
    path = tmp_path / "nested" / "results.jsonl"
    append_result(_make_result(), path)
    append_result(_make_result(), path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    reloaded = RunResult.model_validate_json(lines[0])
    assert reloaded.command == "seed"
    assert reloaded.env.store_kind == "minio"


def test_throughput_summary_math() -> None:
    summary = throughput_summary(files=10, total_bytes=2 * 2**20, elapsed_s=2.0)
    assert summary["files"] == 10
    assert summary["bytes"] == 2 * 2**20
    assert summary["gib"] == round(2 * 2**20 / 2**30, 3)
    assert summary["mib_per_s"] == pytest.approx(1.0)
    assert summary["files_per_s"] == pytest.approx(5.0)
    assert set(summary) == {
        "files",
        "bytes",
        "gib",
        "elapsed_s",
        "mib_per_s",
        "files_per_s",
    }
