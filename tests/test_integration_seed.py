"""Integration tests against a live object store / netem (I1-I3).

These drive the real CLI as a subprocess against a live store (MinIO in CI, RGW
for headline numbers), pointed at it via the ``BENCH_S3_*`` environment. They
are marked ``minio`` / ``netem`` and skipped unless the prerequisites are
present, so the fast gate never touches a socket. They register no coverage.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from rgw_ingest_bench.config import S3Config, make_fs
from rgw_ingest_bench.fakeraw import FakeRawSpec
from rgw_ingest_bench.seed import SeedError, _verify_upload, plan_manifest


def _require_live_store() -> None:
    if not os.environ.get("BENCH_S3_ENDPOINT"):
        pytest.skip("no live store: set BENCH_S3_* (see make minio-up)")


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "rgw_ingest_bench", *args],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )


def _cfg(bucket: str) -> S3Config:
    return S3Config.from_env(bucket=bucket)


def _list_sizes(bucket: str) -> dict[str, int]:
    fs = make_fs(_cfg(bucket))
    fs.invalidate_cache(bucket)
    return {
        info["name"].rsplit("/", 1)[-1]: info["size"]
        for info in fs.ls(bucket, detail=True)
    }


def _corpus_bytes(bucket: str) -> dict[str, bytes]:
    fs = make_fs(_cfg(bucket))
    fs.invalidate_cache(bucket)
    out: dict[str, bytes] = {}
    for info in fs.ls(bucket, detail=True):
        key = info["name"].rsplit("/", 1)[-1]
        if key.endswith(".raw"):
            out[key] = fs.cat_file(info["name"])
    return out


@pytest.fixture
def live_bucket() -> str:
    """A unique bucket name, removed after the test."""
    bucket = f"itbench-{uuid.uuid4().hex[:8]}"
    yield bucket
    try:
        fs = make_fs(_cfg(bucket))
        if fs.exists(bucket):
            fs.rm(bucket, recursive=True)
    except Exception:  # best-effort cleanup; compose down -v is the hard reset
        pass


# --------------------------------------------------------------------------- I1
@pytest.mark.minio
def test_seed_throughput_cli(live_bucket: str, tmp_path: Path) -> None:
    """The CLI throughput check: --json accounting is accurate and self-consistent."""
    _require_live_store()
    wall_start = time.perf_counter()
    proc = _run_cli(
        "seed", "--tier", "small", "--bucket", live_bucket, "--seed", "42",
        "--manifest-out", str(tmp_path / "m"),
        "--results-dir", str(tmp_path / "r"), "--json",
    )
    wall = time.perf_counter() - wall_start
    assert proc.returncode == 0, proc.stderr
    stats = json.loads(proc.stdout.strip())

    sizes = _list_sizes(live_bucket)
    raw = {key: size for key, size in sizes.items() if key.endswith(".raw")}
    assert stats["files"] == len(raw)
    assert stats["bytes"] == sum(raw.values())  # upload accounting is correct
    assert stats["gib"] == round(stats["bytes"] / 2**30, 3)
    assert stats["mib_per_s"] == pytest.approx(
        stats["bytes"] / stats["elapsed_s"] / 2**20, rel=0.01
    )
    assert stats["elapsed_s"] <= wall

    floor = os.environ.get("BENCH_MIN_SEED_MIB_PER_S")
    if floor:
        assert stats["mib_per_s"] >= float(floor)

    row = json.loads((tmp_path / "r" / "seed.jsonl").read_text().splitlines()[0])
    assert row["files_per_s"] == stats["files_per_s"]
    assert row["counters"]["bytes_uploaded"] == stats["bytes"]
    assert row["wall_s"] == pytest.approx(stats["elapsed_s"], rel=1e-3)


# --------------------------------------------------------------------------- I2
@pytest.mark.minio
def test_seed_correctness_minio(live_bucket: str, tmp_path: Path) -> None:
    """T9-T11 against real multipart: accounting, verify, resume, determinism."""
    _require_live_store()
    spec = FakeRawSpec(n_files=40, img_width=64, img_height=64, n_channels=1, seed=7)
    common = [
        "seed", "--n-files", "40", "--width", "64", "--height", "64",
        "--channels", "1", "--seed", "7", "--bucket", live_bucket,
        "--manifest-out", str(tmp_path / "m"), "--results-dir", str(tmp_path / "r"),
    ]

    # T9 — accounting: object count, per-key sizes, manifest copy.
    assert _run_cli(*common, "--jobs", "8").returncode == 0
    sizes = _list_sizes(live_bucket)
    raw = {k: v for k, v in sizes.items() if k.endswith(".raw")}
    assert len(raw) == 40
    assert "_manifest.jsonl" in sizes
    for entry in plan_manifest(spec):
        assert raw[entry.path] == entry.size

    # T10 — verify catches corruption of a real object and names the key.
    fs = make_fs(_cfg(live_bucket))
    with fs.open(f"{live_bucket}/00000000.raw", "wb") as handle:
        handle.write(b"corrupt")
    with pytest.raises(SeedError) as excinfo:
        _verify_upload(fs, live_bucket, plan_manifest(spec))
    assert "00000000.raw" in str(excinfo.value)

    # T11a — resume re-uploads only the wrong-sized + missing keys (self-healing
    # the truncated one), so verify then passes. params.uploaded proves the count.
    fs.rm(f"{live_bucket}/00000005.raw")
    resumed = _run_cli(*common, "--jobs", "4", "--resume")
    assert resumed.returncode == 0, resumed.stderr
    last_row = json.loads(
        (tmp_path / "r" / "seed.jsonl").read_text().splitlines()[-1]
    )
    assert last_row["params"]["uploaded"] == 2  # 00000000 (bad size) + 00000005

    # T11b — determinism: --jobs 1 vs --jobs 8 produce byte-identical objects.
    bucket_j1, bucket_j8 = f"{live_bucket}-j1", f"{live_bucket}-j8"
    det = [
        "seed", "--n-files", "10", "--width", "64", "--height", "64",
        "--channels", "1", "--seed", "7",
        "--manifest-out", str(tmp_path / "md"), "--results-dir", str(tmp_path / "rd"),
    ]
    try:
        assert _run_cli(*det, "--bucket", bucket_j1, "--jobs", "1").returncode == 0
        assert _run_cli(*det, "--bucket", bucket_j8, "--jobs", "8").returncode == 0
        assert _corpus_bytes(bucket_j1) == _corpus_bytes(bucket_j8)
    finally:
        for extra in (bucket_j1, bucket_j8):
            fs_extra = make_fs(_cfg(extra))
            if fs_extra.exists(extra):
                fs_extra.rm(extra, recursive=True)


# --------------------------------------------------------------------------- I3
@pytest.mark.netem
def test_rtt_probe_netem(tmp_path: Path) -> None:
    """netem loop through the CLI: rtt-probe reads ~2d added, clears back."""
    if os.environ.get("BENCH_NETEM") != "1" or os.geteuid() != 0:
        pytest.skip("netem test needs BENCH_NETEM=1 and root (sudo tc)")
    _require_live_store()
    netem = str(Path(__file__).resolve().parent.parent / "scripts" / "netem.sh")

    # The script's own default resolves to docker0, which a Compose fixture never
    # uses (its containers sit on a per-project bridge), so the delay would land
    # on an idle interface and the assertion below would fail for the wrong
    # reason. Default to loopback -- the leg a localhost endpoint really crosses
    # -- while leaving NETEM_IFACE overridable for a bridge-delayed setup.
    env = {**os.environ, "NETEM_IFACE": os.environ.get("NETEM_IFACE", "lo")}

    def probe_median() -> float:
        proc = _run_cli("rtt-probe", "--json")
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout.strip())["median_ms"]

    subprocess.run([netem, "clear"], check=False, env=env)
    baseline = probe_median()
    try:
        subprocess.run([netem, "set", "5ms"], check=True, env=env)
        delayed = probe_median()
        assert delayed >= baseline + 8  # ~2 x 5 ms added, allowing slack
    finally:
        subprocess.run([netem, "clear"], check=True, env=env)
    assert probe_median() < baseline + 4  # back to baseline
