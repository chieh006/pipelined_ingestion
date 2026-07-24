"""Unit tests for the seed orchestration against moto (T9-T11 + branches)."""

from __future__ import annotations

import json
import logging

import pytest
from botocore.exceptions import ClientError

from rgw_ingest_bench import seed
from rgw_ingest_bench.config import S3Config, make_fs
from rgw_ingest_bench.fakeraw import FakeRawSpec
from rgw_ingest_bench.metrics import RunResult
from rgw_ingest_bench.seed import (
    SeedError,
    _upload_file,
    _verify_upload,
    plan_manifest,
    seed_corpus,
)


def _listing(cfg: S3Config) -> dict[str, int]:
    """Return ``{object_key: size}`` for every object in the bucket."""
    fs = make_fs(cfg)
    fs.invalidate_cache(cfg.bucket)
    return {
        info["name"].rsplit("/", 1)[-1]: info["size"]
        for info in fs.ls(cfg.bucket, detail=True)
    }


def _corpus_bytes(cfg: S3Config) -> dict[str, bytes]:
    """Return ``{object_key: bytes}`` for every ``.raw`` object in the bucket."""
    fs = make_fs(cfg)
    fs.invalidate_cache(cfg.bucket)
    out: dict[str, bytes] = {}
    for info in fs.ls(cfg.bucket, detail=True):
        key = info["name"].rsplit("/", 1)[-1]
        if key.endswith(".raw"):
            out[key] = fs.cat_file(info["name"])
    return out


# --------------------------------------------------------------------------- T9
def test_seed_moto(s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path) -> None:
    stats = seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        jobs=4,
        manifest_dir=tmp_path / "manifests",
        results_path=tmp_path / "results" / "seed.jsonl",
    )
    entries = plan_manifest(tiny_spec)
    listing = _listing(s3_cfg)
    raw = {key: size for key, size in listing.items() if key.endswith(".raw")}

    assert len(raw) == tiny_spec.n_files
    for entry in entries:
        assert raw[entry.path] == entry.size
    assert "_manifest.jsonl" in listing

    row = json.loads(
        (tmp_path / "results" / "seed.jsonl").read_text().splitlines()[0]
    )
    RunResult.model_validate(row)
    assert row["command"] == "seed"
    assert row["files"] == tiny_spec.n_files
    assert row["counters"]["put_count"] == tiny_spec.n_files
    assert stats["bytes"] == sum(entry.size for entry in entries)
    assert stats["files"] == tiny_spec.n_files


# -------------------------------------------------------------------------- T10
def test_seed_verify_catches_corruption(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path
) -> None:
    seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        manifest_dir=tmp_path / "m",
        results_path=tmp_path / "r" / "s.jsonl",
    )
    fs = make_fs(s3_cfg)
    with fs.open(f"{s3_cfg.bucket}/00000000.raw", "wb") as handle:
        handle.write(b"truncated")  # wrong size, same object count

    with pytest.raises(SeedError) as excinfo:
        _verify_upload(fs, s3_cfg.bucket, plan_manifest(tiny_spec))
    assert "00000000.raw" in str(excinfo.value)


def test_verify_count_mismatch(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path
) -> None:
    """A missing object trips the count check (distinct from size mismatch)."""
    seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        manifest_dir=tmp_path / "m",
        results_path=tmp_path / "r" / "s.jsonl",
    )
    fs = make_fs(s3_cfg)
    fs.rm(f"{s3_cfg.bucket}/00000000.raw")
    with pytest.raises(SeedError) as excinfo:
        _verify_upload(fs, s3_cfg.bucket, plan_manifest(tiny_spec))
    assert "expected" in str(excinfo.value)


# -------------------------------------------------------------------------- T11
def test_seed_resume_uploads_only_missing(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path, monkeypatch
) -> None:
    seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        manifest_dir=tmp_path / "m",
        results_path=tmp_path / "r" / "s.jsonl",
    )
    fs = make_fs(s3_cfg)
    fs.rm(f"{s3_cfg.bucket}/00000000.raw")
    fs.rm(f"{s3_cfg.bucket}/00000001.raw")

    uploaded: list[str] = []
    original = seed._stream_to_key

    def spy(fs_, key, spec, entry, chunk_size):
        uploaded.append(key.rsplit("/", 1)[-1])
        return original(fs_, key, spec, entry, chunk_size)

    monkeypatch.setattr(seed, "_stream_to_key", spy)
    seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        resume=True,
        manifest_dir=tmp_path / "m2",
        results_path=tmp_path / "r2" / "s.jsonl",
    )
    assert set(uploaded) == {"00000000.raw", "00000001.raw"}


def test_seed_resume_noop_when_complete(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path, monkeypatch
) -> None:
    """A resume over a complete corpus uploads nothing (empty-pool branch)."""
    seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        manifest_dir=tmp_path / "m",
        results_path=tmp_path / "r" / "s.jsonl",
    )
    uploads = {"n": 0}

    def spy(*args, **kwargs):
        uploads["n"] += 1

    monkeypatch.setattr(seed, "_stream_to_key", spy)
    stats = seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        resume=True,
        manifest_dir=tmp_path / "m2",
        results_path=tmp_path / "r2" / "s.jsonl",
    )
    assert uploads["n"] == 0
    assert stats["files"] == tiny_spec.n_files


def test_seed_determinism_across_jobs(
    moto_endpoint: str, tiny_spec: FakeRawSpec, tmp_path
) -> None:
    """--jobs 1 and --jobs 8 produce byte-identical objects."""
    cfg1 = S3Config(
        endpoint_url=moto_endpoint,
        access_key="t",
        secret_key="t",
        bucket="detbench-jobs1",
        kind="minio",
    )
    cfg8 = S3Config(
        endpoint_url=moto_endpoint,
        access_key="t",
        secret_key="t",
        bucket="detbench-jobs8",
        kind="minio",
    )
    seed_corpus(tiny_spec, cfg1, tier="t", jobs=1,
                manifest_dir=tmp_path / "m1", results_path=tmp_path / "r1" / "s.jsonl")
    seed_corpus(tiny_spec, cfg8, tier="t", jobs=8,
                manifest_dir=tmp_path / "m8", results_path=tmp_path / "r8" / "s.jsonl")
    assert _corpus_bytes(cfg1) == _corpus_bytes(cfg8)


# --------------------------------------------------------------- branch coverage
def test_seed_no_verify_skips(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path
) -> None:
    stats = seed_corpus(
        tiny_spec,
        s3_cfg,
        tier="tiny",
        verify=False,
        manifest_dir=tmp_path / "m",
        results_path=tmp_path / "r" / "s.jsonl",
    )
    assert stats["files"] == tiny_spec.n_files


def test_seed_progress_log(
    s3_cfg: S3Config, tiny_spec: FakeRawSpec, tmp_path, monkeypatch, caplog
) -> None:
    monkeypatch.setattr(seed, "_LOG_EVERY", 1)
    with caplog.at_level(logging.INFO):
        seed_corpus(
            tiny_spec,
            s3_cfg,
            tier="tiny",
            jobs=2,
            manifest_dir=tmp_path / "m",
            results_path=tmp_path / "r" / "s.jsonl",
        )
    assert any("seeded" in record.getMessage() for record in caplog.records)


def _client_error(status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "X", "Message": "m"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "PutObject",
    )


def test_is_retryable() -> None:
    assert seed._is_retryable(_client_error(503)) is True
    assert seed._is_retryable(_client_error(404)) is False
    assert seed._is_retryable(OSError("connection reset")) is True
    assert seed._is_retryable(ValueError("permanent")) is False


def test_upload_file_retries_then_succeeds(
    monkeypatch, tiny_spec: FakeRawSpec
) -> None:
    calls = {"n": 0}

    def flaky(fs, key, spec, entry, chunk_size):
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("transient")

    monkeypatch.setattr(seed, "_stream_to_key", flaky)
    monkeypatch.setattr(seed.time, "sleep", lambda _s: None)
    entry = plan_manifest(tiny_spec)[0]
    _upload_file(None, "bucket/k", tiny_spec, entry, chunk_size=1024, max_retries=3)
    assert calls["n"] == 3


def test_upload_file_exhausts_retries(monkeypatch, tiny_spec: FakeRawSpec) -> None:
    def always_fail(fs, key, spec, entry, chunk_size):
        raise OSError("store down")

    monkeypatch.setattr(seed, "_stream_to_key", always_fail)
    monkeypatch.setattr(seed.time, "sleep", lambda _s: None)
    entry = plan_manifest(tiny_spec)[0]
    with pytest.raises(SeedError) as excinfo:
        _upload_file(None, "bucket/k", tiny_spec, entry, chunk_size=1024, max_retries=2)
    assert "after 3 attempt" in str(excinfo.value)


def test_upload_file_non_retryable_aborts_immediately(
    monkeypatch, tiny_spec: FakeRawSpec
) -> None:
    def bad(fs, key, spec, entry, chunk_size):
        raise ValueError("permanent")

    monkeypatch.setattr(seed, "_stream_to_key", bad)
    entry = plan_manifest(tiny_spec)[0]
    with pytest.raises(SeedError) as excinfo:
        _upload_file(None, "bucket/k", tiny_spec, entry, chunk_size=1024, max_retries=3)
    assert "after 1 attempt" in str(excinfo.value)
