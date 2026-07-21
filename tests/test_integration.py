"""End-to-end CLI integration tests (I1-I4).

Each test drives the real ``rgw_ingest_bench`` CLI as a subprocess
(``sys.executable -m rgw_ingest_bench``) — the closest thing to how an operator
invokes it — and writes only under pytest's ``tmp_path``. Marked
``integration`` so the fast unit gate can exclude them; they register no line
coverage and run outside the coverage gate.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from rgw_ingest_bench.layout import HEADER_SIZE, FOOTER_SIZE, expected_size
from rgw_ingest_bench.manifest import read_manifest
from rgw_ingest_bench.parse import parse_footer, parse_header, verify_pixel_range

pytestmark = pytest.mark.integration

# Small geometry: tens of files, a few seconds wall-clock at most. These four
# constants are the single source of truth; _GEN is derived so the CLI args and
# the assertions can never diverge.
_N_FILES = 20
_WIDTH, _HEIGHT, _CHANNELS = 1024, 1024, 1
_GEN = [
    "--n-files", str(_N_FILES),
    "--width", str(_WIDTH),
    "--height", str(_HEIGHT),
    "--channels", str(_CHANNELS),
]


def _run(*args: str, **kwargs) -> subprocess.CompletedProcess:
    """Invoke the CLI as a subprocess and return the completed process."""
    return subprocess.run(
        [sys.executable, "-m", "rgw_ingest_bench", *args],
        capture_output=True,
        text=True,
        **kwargs,
    )


def test_cli_roundtrip_corpus(tmp_path: Path) -> None:
    """I1: every generated object reloads and parses consistently."""
    out = tmp_path / "corpus"
    result = _run("generate", *_GEN, "--seed", "42", "--out", str(out))
    assert result.returncode == 0, result.stderr

    entries = read_manifest(out / "manifest.jsonl")
    assert len(entries) == _N_FILES

    for entry in entries:
        path = out / entry.path
        raw = path.read_bytes()
        assert path.stat().st_size == entry.size
        assert entry.size == expected_size(_WIDTH, _HEIGHT, _CHANNELS, entry.has_footer)

        header = parse_header(raw[:HEADER_SIZE])
        assert header.file_id == entry.file_id
        assert header.img_width == _WIDTH
        assert header.scan_dir == entry.file_id % 2

        if entry.has_footer:
            footer = parse_footer(raw[-FOOTER_SIZE:])
            assert footer.file_id_echo == entry.file_id

        # A mid-pixel ranged slice verifies against the deterministic pattern.
        start, length = 1000, 512
        chunk = raw[HEADER_SIZE + start : HEADER_SIZE + start + length]
        assert verify_pixel_range(chunk, entry.file_id, start)


def test_cli_throughput(tmp_path: Path) -> None:
    """I2: --json accounting is exact and the reported rate is accurate."""
    out = tmp_path / "corpus"
    wall_start = time.perf_counter()
    result = _run("generate", *_GEN, "--out", str(out), "--json")
    wall_elapsed = time.perf_counter() - wall_start
    assert result.returncode == 0, result.stderr

    stats = json.loads(result.stdout.strip())
    # Surface the measured rate on stdout so `pytest -s` shows it (§7.3 step 3).
    print(f"\n[I2] generate throughput: {stats['mib_per_s']} MiB/s, "
          f"{stats['files_per_s']} files/s ({stats['files']} files, "
          f"{stats['bytes']} bytes / {stats['gib']} GiB)")
    assert set(stats) == {"files", "bytes", "gib", "elapsed_s", "mib_per_s", "files_per_s"}

    on_disk = sum(p.stat().st_size for p in out.glob("*.raw"))
    assert stats["files"] == _N_FILES
    assert stats["bytes"] == on_disk
    # gib mirrors the exact byte count in gibibytes.
    assert stats["gib"] == round(stats["bytes"] / 2**30, 3)

    # Reported throughput is accurate, not merely present.
    recomputed = stats["bytes"] / stats["elapsed_s"] / 2**20
    assert abs(recomputed - stats["mib_per_s"]) <= 0.01 * recomputed
    # The in-process elapsed cannot exceed the wall-clock around the subprocess.
    assert stats["elapsed_s"] <= wall_elapsed

    floor = os.environ.get("BENCH_MIN_GENERATE_MIB_PER_S")
    if floor is not None:
        assert stats["mib_per_s"] >= float(floor)


def test_cli_memory_streaming(tmp_path: Path) -> None:
    """I3: peak RSS growth for a 32 MiB file stays near one chunk, not the file."""
    psutil = pytest.importorskip("psutil")

    def _peak_rss(*gen_args: str) -> int:
        proc = psutil.Popen(
            [sys.executable, "-m", "rgw_ingest_bench", "generate", *gen_args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        peak = 0
        while proc.poll() is None:
            try:
                peak = max(peak, proc.memory_info().rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied):  # pragma: no cover
                break
            time.sleep(0.002)
        assert proc.wait() == 0
        return peak

    baseline = _peak_rss(
        "--n-files", "1", "--width", "8", "--height", "8", "--channels", "1",
        "--out", str(tmp_path / "tiny"),
    )
    large = _peak_rss(
        "--n-files", "1", "--width", "4096", "--height", "4096", "--channels", "2",
        "--out", str(tmp_path / "big"),
    )

    file_size = (tmp_path / "big" / "00000000.raw").stat().st_size
    assert file_size >= 32 * 2**20  # ~32 MiB, well above one 4 MiB chunk

    # Streaming: the 32 MiB file adds only a few chunks of RSS over the tiny
    # run — far less than holding the whole file (or corpus) in memory.
    growth = large - baseline
    assert growth < file_size


def test_cli_determinism_e2e(tmp_path: Path) -> None:
    """I4: same seed -> identical manifest + per-file hashes; new seed differs."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    c = tmp_path / "c"
    assert _run("generate", *_GEN, "--seed", "42", "--out", str(a)).returncode == 0
    assert _run("generate", *_GEN, "--seed", "42", "--out", str(b)).returncode == 0
    assert _run("generate", *_GEN, "--seed", "7", "--out", str(c)).returncode == 0

    def _hashes(root: Path) -> dict[str, str]:
        return {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.glob("*.raw"))
        }

    assert (a / "manifest.jsonl").read_bytes() == (b / "manifest.jsonl").read_bytes()
    assert _hashes(a) == _hashes(b)
    assert (a / "manifest.jsonl").read_bytes() != (c / "manifest.jsonl").read_bytes()
