"""``seed`` — streamed generate-and-upload of a corpus into an object store.

The flow (parent design §5):

1. **Manifest upfront.** Because size is a closed-form function of the spec,
   the full manifest is computed *before* any bytes exist — so an interrupted
   run can be ``--resume``\\ d and verification is a pure manifest-vs-LIST check.
2. **Streamed upload.** A thread pool streams each file's
   :func:`~rgw_ingest_bench.fakeraw.iter_file_chunks` straight into an s3fs
   multipart write; nothing stages on local disk. Peak memory ≈ ``jobs ×
   chunk_size``, independent of tier.
3. **Verify.** A paged LIST is joined against the manifest with Polars; any
   count or per-key size mismatch aborts the seed.

Determinism is inherited from :func:`~rgw_ingest_bench.fakeraw.footer_flags`:
every byte is a pure function of ``(seed, file_id)``, so ``--jobs 1`` and
``--jobs 32`` produce byte-identical objects. ``seed`` also dogfoods
:mod:`~rgw_ingest_bench.metrics` by emitting a :class:`RunResult` row.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
from botocore.exceptions import BotoCoreError, ClientError

from .config import S3Config, make_fs
from .fakeraw import DEFAULT_CHUNK_SIZE, FakeRawSpec, footer_flags, iter_file_chunks
from .layout import expected_size
from .manifest import ManifestEntry, write_manifest
from .metrics import (
    CounterSet,
    LatencyRecorder,
    RunResult,
    append_result,
    collect_env_info,
    throughput_summary,
)
from .probe import endpoint_host_port, probe_rtt

logger = logging.getLogger(__name__)

DEFAULT_JOBS: int = 16
DEFAULT_MAX_RETRIES: int = 3
#: Extra connections above ``jobs`` so aiobotocore never serialises (parent §6.5).
POOL_HEADROOM: int = 4
#: Object key of the provenance copy of the manifest, uploaded into the bucket.
MANIFEST_COPY_NAME: str = "_manifest.jsonl"
#: Base backoff (seconds); attempt ``k`` waits ``BACKOFF_BASE_S * 2**k``.
BACKOFF_BASE_S: float = 0.1
_LOG_EVERY: int = 1000
#: Exception types treated as transient (retried); 5xx ``ClientError`` too.
_RETRYABLE_TYPES: tuple[type[BaseException], ...] = (OSError, BotoCoreError)


class SeedError(RuntimeError):
    """Raised when a seed cannot complete (upload exhausted retries, or verify
    found a count/size mismatch). A partially seeded corpus is useless, so this
    aborts the whole run rather than dead-lettering."""


def plan_manifest(spec: FakeRawSpec) -> list[ManifestEntry]:
    """Compute the complete manifest before any bytes are generated.

    Parameters
    ----------
    spec : FakeRawSpec
        Corpus spec.

    Returns
    -------
    list of ManifestEntry
        One entry per file, sizes from :func:`layout.expected_size` and footer
        flags from the single upfront draw.
    """
    flags = footer_flags(spec)
    entries: list[ManifestEntry] = []
    for file_id in range(spec.n_files):
        has_footer = bool(flags[file_id])
        entries.append(
            ManifestEntry(
                path=f"{file_id:08d}.raw",
                size=expected_size(
                    spec.img_width, spec.img_height, spec.n_channels, has_footer
                ),
                has_footer=has_footer,
                file_id=file_id,
            )
        )
    return entries


def _is_retryable(exc: BaseException) -> bool:
    """Return whether ``exc`` is a transient failure worth retrying."""
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return status >= 500
    return isinstance(exc, _RETRYABLE_TYPES)


def _stream_to_key(
    fs: Any, key: str, spec: FakeRawSpec, entry: ManifestEntry, chunk_size: int
) -> None:
    """Stream one file's bytes into ``key`` via a single s3fs multipart write."""
    with fs.open(key, "wb") as s3file:
        for chunk in iter_file_chunks(
            spec, entry.file_id, entry.has_footer, chunk_size=chunk_size
        ):
            s3file.write(chunk)


def _upload_file(
    fs: Any,
    key: str,
    spec: FakeRawSpec,
    entry: ManifestEntry,
    *,
    chunk_size: int,
    max_retries: int,
) -> None:
    """Upload one file, retrying transient failures with exponential backoff.

    Raises
    ------
    SeedError
        If the upload still fails after ``max_retries`` retries, or fails with a
        non-transient error.
    """
    attempt = 0
    while True:
        try:
            _stream_to_key(fs, key, spec, entry, chunk_size)
            return
        except Exception as exc:
            if attempt >= max_retries or not _is_retryable(exc):
                raise SeedError(
                    f"upload failed for '{key}' after {attempt + 1} attempt(s): {exc}"
                ) from exc
            logger.warning(f"retrying '{key}' after transient error: {exc}")
            time.sleep(BACKOFF_BASE_S * 2**attempt)
            attempt += 1


def _upload_file_timed(
    fs: Any,
    key: str,
    spec: FakeRawSpec,
    entry: ManifestEntry,
    *,
    chunk_size: int,
    max_retries: int,
) -> float:
    """Upload one file and return its wall time in milliseconds."""
    start = time.perf_counter()
    _upload_file(fs, key, spec, entry, chunk_size=chunk_size, max_retries=max_retries)
    return (time.perf_counter() - start) * 1000.0


def _run_upload_pool(
    fs: Any,
    bucket: str,
    spec: FakeRawSpec,
    entries: list[ManifestEntry],
    *,
    jobs: int,
    chunk_size: int,
    max_retries: int,
    counters: CounterSet,
    put_latency: LatencyRecorder,
) -> None:
    """Upload ``entries`` concurrently, recording per-PUT latency and counters.

    Counters and latency are updated on the main thread as futures complete, so
    the recorders never need their own locks.
    """
    if not entries:
        return
    total = len(entries)
    start = time.perf_counter()
    done = 0
    bytes_done = 0
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        future_to_entry = {
            pool.submit(
                _upload_file_timed,
                fs,
                f"{bucket}/{entry.path}",
                spec,
                entry,
                chunk_size=chunk_size,
                max_retries=max_retries,
            ): entry
            for entry in entries
        }
        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            put_latency.record(future.result())
            counters.incr("put_count")
            counters.incr("bytes_uploaded", entry.size)
            done += 1
            bytes_done += entry.size
            if done % _LOG_EVERY == 0:
                rate = bytes_done / (time.perf_counter() - start) / 2**20
                logger.info(f"seeded {done}/{total} files ({rate:.1f} MiB/s)")


def _existing_sizes(fs: Any, bucket: str) -> dict[str, int]:
    """Return ``{object_key: size}`` for the bucket (cache invalidated first)."""
    fs.invalidate_cache(bucket)
    sizes: dict[str, int] = {}
    for info in fs.ls(bucket, detail=True):
        key = info["name"].rsplit("/", 1)[-1]
        sizes[key] = info["size"]
    return sizes


def _upload_manifest_copy(fs: Any, bucket: str, manifest_path: Path) -> None:
    """Upload a provenance copy of the manifest to ``<bucket>/_manifest.jsonl``."""
    with fs.open(f"{bucket}/{MANIFEST_COPY_NAME}", "wb") as handle:
        handle.write(manifest_path.read_bytes())


def _verify_upload(fs: Any, bucket: str, entries: list[ManifestEntry]) -> None:
    """Assert every corpus object is present with exactly its manifest size.

    Raises
    ------
    SeedError
        If the count of ``.raw`` objects differs from the manifest, or any key
        is missing or the wrong size. The message names the offending keys.
    """
    listing = _existing_sizes(fs, bucket)
    corpus = {key: size for key, size in listing.items() if key.endswith(".raw")}
    if len(corpus) != len(entries):
        raise SeedError(
            f"verify: expected {len(entries)} objects in '{bucket}', "
            f"found {len(corpus)}"
        )
    manifest_df = pl.DataFrame(
        {
            "path": [entry.path for entry in entries],
            "expected_size": [entry.size for entry in entries],
        }
    )
    listed_df = pl.DataFrame(
        {"path": list(corpus.keys()), "actual_size": list(corpus.values())}
    )
    joined = manifest_df.join(listed_df, on="path", how="left")
    mismatched = joined.filter(
        pl.col("actual_size").is_null()
        | (pl.col("actual_size") != pl.col("expected_size"))
    )
    if mismatched.height:
        keys = mismatched.get_column("path").to_list()
        raise SeedError(
            f"verify: size/missing mismatch for {mismatched.height} key(s): "
            f"{keys[:5]}"
        )


def seed_corpus(
    spec: FakeRawSpec,
    cfg: S3Config,
    *,
    tier: str,
    jobs: int = DEFAULT_JOBS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    resume: bool = False,
    verify: bool = True,
    max_retries: int = DEFAULT_MAX_RETRIES,
    manifest_dir: Path = Path("manifests"),
    results_path: Path = Path("results/seed.jsonl"),
    netem_nominal: str | None = None,
) -> dict[str, Any]:
    """Seed a corpus into the store and record a :class:`RunResult`.

    Parameters
    ----------
    spec : FakeRawSpec
        Corpus spec (geometry, count, seed).
    cfg : S3Config
        Endpoint and credentials.
    tier : str
        Tier label, used in the manifest filename and recorded in ``EnvInfo``.
    jobs : int, optional
        Upload thread-pool size. Defaults to 16.
    chunk_size : int, optional
        Streaming slab size in bytes. Defaults to 4 MiB.
    resume : bool, optional
        Skip keys already present at the manifest size. Defaults to False.
    verify : bool, optional
        Run the post-upload LIST-vs-manifest verification. Defaults to True.
    max_retries : int, optional
        Per-file transient-failure retries. Defaults to 3.
    manifest_dir : pathlib.Path, optional
        Directory for the local manifest. Defaults to ``manifests/``.
    results_path : pathlib.Path, optional
        JSONL file the ``RunResult`` is appended to. Defaults to
        ``results/seed.jsonl``.
    netem_nominal : str or None, optional
        Nominal netem delay, recorded for provenance only.

    Returns
    -------
    dict
        The throughput summary (see
        :func:`~rgw_ingest_bench.metrics.throughput_summary`).

    Raises
    ------
    SeedError
        If an upload cannot complete or verification fails.
    ConnectionError
        If the RTT probe cannot reach the endpoint.
    """
    started_at = datetime.now(timezone.utc)
    fs = make_fs(cfg, max_pool=jobs + POOL_HEADROOM)

    entries = plan_manifest(spec)
    manifest_path = Path(manifest_dir) / f"{tier}-seed{spec.seed}.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    write_manifest(entries, manifest_path)

    # Probe RTT first: it is the cheapest network touch, records the measured
    # latency for the result row, and fails fast with a clean ConnectionError if
    # the store is unreachable (before any confusing s3 error).
    host, port = endpoint_host_port(str(cfg.endpoint_url))
    rtt = probe_rtt(host, port)

    fs.makedirs(cfg.bucket, exist_ok=True)

    if resume:
        existing = _existing_sizes(fs, cfg.bucket)
        todo = [entry for entry in entries if existing.get(entry.path) != entry.size]
    else:
        todo = entries

    counters = CounterSet()
    put_latency = LatencyRecorder("put")

    start = time.perf_counter()
    _run_upload_pool(
        fs,
        cfg.bucket,
        spec,
        todo,
        jobs=jobs,
        chunk_size=chunk_size,
        max_retries=max_retries,
        counters=counters,
        put_latency=put_latency,
    )
    elapsed_s = max(time.perf_counter() - start, 1e-9)

    _upload_manifest_copy(fs, cfg.bucket, manifest_path)

    if verify:
        _verify_upload(fs, cfg.bucket, entries)

    total_bytes = sum(entry.size for entry in entries)
    stats = throughput_summary(len(entries), total_bytes, elapsed_s)

    result = RunResult(
        run_id=str(uuid4()),
        started_at=started_at,
        command="seed",
        variant=None,
        params={
            "jobs": jobs,
            "resume": resume,
            "chunk_size": chunk_size,
            "bucket": cfg.bucket,
            "uploaded": len(todo),
        },
        wall_s=elapsed_s,
        files=len(entries),
        files_per_s=stats["files_per_s"],
        counters=counters.snapshot(),
        latencies=[put_latency.summarize()],
        env=collect_env_info(
            store_kind=cfg.kind,
            rtt=rtt,
            corpus_tier=tier,
            corpus_seed=spec.seed,
            netem_nominal=netem_nominal,
        ),
    )
    append_result(result, Path(results_path))
    return stats
