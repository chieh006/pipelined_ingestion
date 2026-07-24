"""Measurement toolkit: collectors, summary models, and the JSONL sink.

Three layers, per parent §7.1:

* **collectors** — mutable helpers used *during* a run (:class:`CounterSet`,
  :class:`LatencyRecorder`, :class:`PeriodicSampler`, :class:`EventLoopLagProbe`);
* **summary / environment models** — frozen Pydantic snapshots computed at run
  end (:class:`RttStats`, :class:`LatencySummary`, :class:`EnvInfo`,
  :class:`RunResult`);
* **sink** — :func:`append_result`, one JSONL row per run.

``seed`` (this PR) is the first real caller; every variant in PR 3+ reuses the
same toolkit, so seeding-throughput regressions become visible for free.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import logging
import os
import platform
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Literal

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

#: Packages whose versions are recorded per result row (parent §11 drift guard).
TRACKED_PACKAGES: tuple[str, ...] = (
    "s3fs",
    "aiobotocore",
    "botocore",
    "numpy",
    "polars",
    "pydantic",
)

#: Default sampling interval for the async probes, in seconds.
DEFAULT_INTERVAL_S: float = 0.1


# --------------------------------------------------------------------------- #
# Collectors (mutable, live during a run)
# --------------------------------------------------------------------------- #
class CounterSet:
    """Named monotonic counters (``bytes_uploaded``, ``put_count``, ...).

    Increments are lock-guarded: the asyncio loop is single-threaded, but a
    thread-pool writer offload (``seed`` here, PR 5 later) must not race counts.
    """

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def incr(self, name: str, n: int = 1) -> None:
        """Add ``n`` to the counter ``name`` (created at zero on first use)."""
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + n

    def snapshot(self) -> dict[str, int]:
        """Return a copy of the current counter values."""
        with self._lock:
            return dict(self._counts)


class LatencyRecorder:
    """Append-only per-operation latency samples in milliseconds.

    Parameters
    ----------
    op : str
        Operation name (e.g. ``"put"``) recorded into the summary.
    """

    def __init__(self, op: str) -> None:
        self.op = op
        self._samples: list[float] = []

    def record(self, ms: float) -> None:
        """Append one latency sample (milliseconds)."""
        self._samples.append(ms)

    def summarize(self) -> "LatencySummary":
        """Summarise the samples as percentiles via :func:`numpy.quantile`.

        Returns
        -------
        LatencySummary
            ``count``/p50/p95/p99/max. An empty recorder returns a NaN-free
            summary with ``count == 0`` and zero percentiles.
        """
        if not self._samples:
            return LatencySummary(
                op=self.op, count=0, p50_ms=0.0, p95_ms=0.0, p99_ms=0.0, max_ms=0.0
            )
        arr = np.asarray(self._samples, dtype=float)
        p50, p95, p99 = (float(q) for q in np.quantile(arr, [0.5, 0.95, 0.99]))
        return LatencySummary(
            op=self.op,
            count=int(arr.size),
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            max_ms=float(arr.max()),
        )


def read_rss_mib() -> float:
    """Return this process's resident set size in MiB.

    Reads ``/proc/self/statm`` when present (the real path on Linux/WSL, where
    benchmarks run); otherwise falls back to :func:`resource.getrusage`
    (peak-only). No ``psutil`` dependency.

    Returns
    -------
    float
        Resident memory in mebibytes.
    """
    statm = Path("/proc/self/statm")
    if statm.exists():
        resident_pages = int(statm.read_text().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE") / 2**20
    import resource  # Linux/macOS only; imported lazily so Windows import is fine

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class PeriodicSampler:
    """Sample named probes on a fixed interval from an async task.

    Parameters
    ----------
    probes : dict of str to callable
        Probe name to a zero-arg callable returning a ``float`` (e.g.
        ``{"rss_mib": read_rss_mib}``). PR 5 plugs queue-depth / writer probes
        in here; nothing else changes.
    interval_s : float, optional
        Seconds between sampling passes. Defaults to 0.1.

    Notes
    -----
    Use as an async context manager. A probe that raises is logged once (per
    name) and skipped; the sampler keeps running. Samples are held as parallel
    ``(t_s, name, value)`` lists and exported with :meth:`to_df`.
    """

    def __init__(
        self,
        probes: dict[str, Callable[[], float]],
        *,
        interval_s: float = DEFAULT_INTERVAL_S,
    ) -> None:
        self.probes = probes
        self.interval_s = interval_s
        self._t: list[float] = []
        self._name: list[str] = []
        self._value: list[float] = []
        self._logged: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._start: float = 0.0

    async def __aenter__(self) -> "PeriodicSampler":
        self._start = time.perf_counter()
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc: object) -> bool:
        await _cancel(self._task)
        return False

    async def _run(self) -> None:
        while True:
            now = time.perf_counter() - self._start
            for name, probe in self.probes.items():
                try:
                    value = probe()
                except Exception:  # a broken probe must not kill the sampler
                    if name not in self._logged:
                        logger.exception(f"probe '{name}' failed; suppressing repeats")
                        self._logged.add(name)
                    continue
                self._t.append(now)
                self._name.append(name)
                self._value.append(value)
            await asyncio.sleep(self.interval_s)

    def to_df(self) -> pl.DataFrame:
        """Return the collected samples as a Polars DataFrame."""
        return pl.DataFrame(
            {"t_s": self._t, "name": self._name, "value": self._value}
        )


class EventLoopLagProbe:
    """Record event-loop scheduling drift by timing fixed sleeps.

    Each pass sleeps ``interval_s`` and records ``(actual - interval_s)`` in
    milliseconds: a blocked loop makes the sleep overrun, so the drift spikes.
    Use as an async context manager.
    """

    def __init__(self, *, interval_s: float = DEFAULT_INTERVAL_S) -> None:
        self.interval_s = interval_s
        self._lags_ms: list[float] = []
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "EventLoopLagProbe":
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc: object) -> bool:
        await _cancel(self._task)
        return False

    async def _run(self) -> None:
        while True:
            start = time.perf_counter()
            await asyncio.sleep(self.interval_s)
            drift_s = (time.perf_counter() - start) - self.interval_s
            self._lags_ms.append(drift_s * 1000.0)

    @property
    def lags_ms(self) -> list[float]:
        """list of float: A copy of the recorded drift samples (ms)."""
        return list(self._lags_ms)

    def max_lag_ms(self) -> float:
        """Return the largest recorded drift (ms), or 0.0 if none."""
        return max(self._lags_ms) if self._lags_ms else 0.0


async def _cancel(task: asyncio.Task[None] | None) -> None:
    """Cancel ``task`` and await it, swallowing the ``CancelledError``."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --------------------------------------------------------------------------- #
# Summary & environment models (frozen)
# --------------------------------------------------------------------------- #
class RttStats(BaseModel):
    """Round-trip-time summary from the TCP connect-time probe."""

    model_config = ConfigDict(frozen=True)

    median_ms: float
    iqr_ms: float
    samples: int


class LatencySummary(BaseModel):
    """Percentile summary of one operation's latency samples."""

    model_config = ConfigDict(frozen=True)

    op: str
    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


class EnvInfo(BaseModel):
    """Programmatically captured run environment (parent §7.1 'Environment')."""

    model_config = ConfigDict(frozen=True)

    python: str
    platform: str
    kernel: str
    package_versions: dict[str, str]
    git_sha: str
    store_kind: Literal["rgw", "minio"]
    rtt: RttStats
    netem_nominal: str | None
    corpus_tier: str
    corpus_seed: int


class RunResult(BaseModel):
    """One JSONL row: a single benchmark run (or one ``seed``)."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    started_at: datetime
    command: str
    variant: str | None
    params: dict[str, Any]
    wall_s: float
    files: int
    files_per_s: float
    counters: dict[str, int]
    latencies: list[LatencySummary]
    env: EnvInfo


def package_versions() -> dict[str, str]:
    """Return installed versions of :data:`TRACKED_PACKAGES`.

    Returns
    -------
    dict of str to str
        Package name to version; ``"unknown"`` if a package is not installed.
    """
    versions: dict[str, str] = {}
    for name in TRACKED_PACKAGES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "unknown"
    return versions


def git_sha() -> str:
    """Return the current git commit SHA, or ``"unknown"`` outside a repo.

    Returns
    -------
    str
        The 40-char commit SHA, or ``"unknown"`` if git is unavailable or the
        working directory is not a repository.
    """
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.SubprocessError, OSError):
        return "unknown"
    return completed.stdout.strip()


def collect_env_info(
    *,
    store_kind: Literal["rgw", "minio"],
    rtt: RttStats,
    corpus_tier: str,
    corpus_seed: int,
    netem_nominal: str | None = None,
) -> EnvInfo:
    """Assemble an :class:`EnvInfo` snapshot for the current process.

    Parameters
    ----------
    store_kind : {"rgw", "minio"}
        Which store this run hit (never inferred from numbers).
    rtt : RttStats
        Measured RTT to the endpoint (never the nominal netem value).
    corpus_tier : str
        Tier label of the corpus.
    corpus_seed : int
        Generation seed of the corpus.
    netem_nominal : str or None, optional
        The nominal netem delay if one was applied, informational only.

    Returns
    -------
    EnvInfo
        The captured environment.
    """
    return EnvInfo(
        python=platform.python_version(),
        platform=platform.platform(),
        kernel=platform.release(),
        package_versions=package_versions(),
        git_sha=git_sha(),
        store_kind=store_kind,
        rtt=rtt,
        netem_nominal=netem_nominal,
        corpus_tier=corpus_tier,
        corpus_seed=corpus_seed,
    )


def throughput_summary(
    files: int, total_bytes: int, elapsed_s: float
) -> dict[str, Any]:
    """Build the six-key machine-readable throughput summary.

    Shared by ``generate`` and ``seed`` so both emit an identical schema. The
    exact integer ``bytes`` is the accounting source; ``gib`` is that same value
    in gibibytes for readability.

    Parameters
    ----------
    files : int
        Number of files produced/uploaded.
    total_bytes : int
        Exact total byte count.
    elapsed_s : float
        Wall-clock seconds for the operation (already floored above zero).

    Returns
    -------
    dict
        ``{"files", "bytes", "gib", "elapsed_s", "mib_per_s", "files_per_s"}``.
    """
    return {
        "files": files,
        "bytes": total_bytes,
        "gib": round(total_bytes / 2**30, 3),
        "elapsed_s": round(elapsed_s, 6),
        "mib_per_s": round(total_bytes / elapsed_s / 2**20, 3),
        "files_per_s": round(files / elapsed_s, 3),
    }


def append_result(result: RunResult, path: Path) -> None:
    """Append one :class:`RunResult` as a JSON line, creating parent dirs.

    Parameters
    ----------
    result : RunResult
        The row to append.
    path : pathlib.Path
        Destination JSONL file; its parent directory is created if needed.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(result.model_dump_json())
        handle.write("\n")
