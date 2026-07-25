"""Unit tests for the seed / rtt-probe CLI wiring (T13, T15 + error paths)."""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator

import pytest

from rgw_ingest_bench import cli
from rgw_ingest_bench.config import ENV_FIELDS, S3Config
from rgw_ingest_bench.seed import plan_manifest

_JSON_KEYS = {"files", "bytes", "gib", "elapsed_s", "mib_per_s", "files_per_s"}


@pytest.fixture(autouse=True)
def _clear_bench_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in ENV_FIELDS.values():
        monkeypatch.delenv(env_name, raising=False)


@pytest.fixture
def listener() -> Iterator[tuple[str, int]]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    try:
        yield sock.getsockname()
    finally:
        sock.close()


def _closed_endpoint() -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    host, port = sock.getsockname()
    sock.close()
    return f"http://{host}:{port}"


def _set_store_env(monkeypatch: pytest.MonkeyPatch, cfg: S3Config) -> None:
    monkeypatch.setenv("BENCH_S3_ENDPOINT", str(cfg.endpoint_url).rstrip("/"))
    monkeypatch.setenv("BENCH_S3_ACCESS_KEY", cfg.access_key)
    monkeypatch.setenv("BENCH_S3_SECRET_KEY", cfg.secret_key.get_secret_value())
    monkeypatch.setenv("BENCH_S3_KIND", cfg.kind)


# ------------------------------------------------------------------------- T15
def test_cli_seed_json(
    s3_cfg: S3Config, tiny_spec, tmp_path, capsys, monkeypatch
) -> None:
    _set_store_env(monkeypatch, s3_cfg)
    code = cli.main(
        [
            "seed", "--n-files", "6", "--width", "8", "--height", "8",
            "--channels", "1", "--footer-ratio", "0.5", "--seed", "7",
            "--bucket", "bronze", "--jobs", "2",
            "--manifest-out", str(tmp_path / "m"),
            "--results-dir", str(tmp_path / "r"), "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert set(payload) == _JSON_KEYS

    entries = plan_manifest(tiny_spec)
    assert payload["files"] == 6
    assert payload["bytes"] == sum(entry.size for entry in entries)
    assert payload["gib"] == round(payload["bytes"] / 2**30, 3)
    assert payload["mib_per_s"] == pytest.approx(
        payload["bytes"] / payload["elapsed_s"] / 2**20, rel=0.01
    )
    assert payload["files_per_s"] == pytest.approx(
        payload["files"] / payload["elapsed_s"], rel=0.01
    )

    row = json.loads((tmp_path / "r" / "seed.jsonl").read_text().splitlines()[0])
    assert row["files_per_s"] == payload["files_per_s"]
    assert row["counters"]["bytes_uploaded"] == payload["bytes"]
    assert row["wall_s"] == pytest.approx(payload["elapsed_s"], rel=1e-3)


def test_cli_seed_human_summary(
    s3_cfg: S3Config, tmp_path, capsys, monkeypatch
) -> None:
    _set_store_env(monkeypatch, s3_cfg)
    code = cli.main(
        [
            "seed", "--n-files", "3", "--width", "8", "--height", "8",
            "--channels", "1", "--seed", "7", "--jobs", "2",
            "--manifest-out", str(tmp_path / "m"),
            "--results-dir", str(tmp_path / "r"),
        ]
    )
    assert code == 0
    assert "seed:" in capsys.readouterr().out


# ------------------------------------------------------------------------- T13
def test_cli_seed_bad_tier() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(
            ["seed", "--tier", "huge", "--endpoint", "http://x:1",
             "--access-key", "a", "--secret-key", "s"]
        )
    assert excinfo.value.code != 0


def test_cli_seed_missing_endpoint() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["seed", "--tier", "small"])
    assert excinfo.value.code != 0


def test_cli_seed_conflicting_spec() -> None:
    with pytest.raises(SystemExit):
        cli.main(
            ["seed", "--tier", "small", "--n-files", "4",
             "--endpoint", "http://x:1", "--access-key", "a", "--secret-key", "s"]
        )


def test_cli_seed_clean_conflicts_with_resume(capsys) -> None:
    """``--clean`` empties the bucket, so pairing it with ``--resume`` is invalid."""
    with pytest.raises(SystemExit) as excinfo:
        cli.main(
            ["seed", "--tier", "small", "--clean", "--resume",
             "--endpoint", "http://x:1", "--access-key", "a", "--secret-key", "s"]
        )
    assert excinfo.value.code != 0
    assert "not allowed with argument" in capsys.readouterr().err


def test_cli_seed_clean_forwarded(s3_cfg, tiny_spec, tmp_path, monkeypatch) -> None:
    """The CLI flag reaches ``seed_corpus`` as ``clean=True``."""
    seen = {}

    def spy(spec, cfg, **kwargs):
        seen.update(kwargs)
        return {"files": 0, "bytes": 0, "gib": 0.0, "elapsed_s": 1.0,
                "mib_per_s": 0.0, "files_per_s": 0.0}

    monkeypatch.setattr(cli, "seed_corpus", spy)
    code = cli.main(
        ["seed", "--n-files", "2", "--width", "8", "--height", "8",
         "--channels", "1", "--clean", "--json",
         "--endpoint", str(s3_cfg.endpoint_url),
         "--access-key", "a", "--secret-key", "s",
         "--manifest-out", str(tmp_path / "m"), "--results-dir", str(tmp_path / "r")]
    )
    assert code == 0
    assert seen["clean"] is True


def test_cli_seed_unreachable_returns_1(tmp_path, monkeypatch, capsys) -> None:
    """A store that refuses the connection makes seed exit non-zero cleanly."""
    monkeypatch.setenv("BENCH_S3_ENDPOINT", _closed_endpoint())
    monkeypatch.setenv("BENCH_S3_ACCESS_KEY", "a")
    monkeypatch.setenv("BENCH_S3_SECRET_KEY", "s")
    monkeypatch.setenv("BENCH_S3_KIND", "minio")
    code = cli.main(
        ["seed", "--n-files", "2", "--width", "8", "--height", "8",
         "--channels", "1", "--manifest-out", str(tmp_path / "m"),
         "--results-dir", str(tmp_path / "r")]
    )
    assert code == 1


# ---------------------------------------------------------------------- rtt-probe
def test_cli_rtt_probe_human(listener: tuple[str, int], capsys) -> None:
    host, port = listener
    code = cli.main(["rtt-probe", "--endpoint", f"http://{host}:{port}",
                     "--samples", "3"])
    assert code == 0
    out = capsys.readouterr().out
    assert "rtt-probe" in out and "median" in out


def test_cli_rtt_probe_json(listener: tuple[str, int], capsys) -> None:
    host, port = listener
    code = cli.main(["rtt-probe", "--endpoint", f"http://{host}:{port}",
                     "--samples", "3", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["samples"] == 3
    assert "median_ms" in payload and "iqr_ms" in payload


def test_cli_rtt_probe_from_env(listener: tuple[str, int], monkeypatch) -> None:
    host, port = listener
    monkeypatch.setenv("BENCH_S3_ENDPOINT", f"http://{host}:{port}")
    assert cli.main(["rtt-probe", "--samples", "2"]) == 0


def test_cli_rtt_probe_no_endpoint() -> None:
    with pytest.raises(SystemExit):
        cli.main(["rtt-probe"])


def test_cli_rtt_probe_bad_url() -> None:
    with pytest.raises(SystemExit):
        cli.main(["rtt-probe", "--endpoint", "http:///no-host"])


def test_cli_rtt_probe_unreachable_returns_1(capsys) -> None:
    code = cli.main(["rtt-probe", "--endpoint", _closed_endpoint(), "--samples", "2"])
    assert code == 1
