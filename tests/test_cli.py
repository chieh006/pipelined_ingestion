"""Unit tests for the CLI (T12) and spec building."""

from __future__ import annotations

import argparse
import json

import pytest

from rgw_ingest_bench import cli
from rgw_ingest_bench.manifest import read_manifest

_JSON_KEYS = {"files", "bytes", "gib", "elapsed_s", "mib_per_s", "files_per_s"}


def _args(**over) -> argparse.Namespace:
    """Build a ``generate`` Namespace with all-None defaults."""
    base = dict(
        tier=None,
        n_files=None,
        width=None,
        height=None,
        channels=None,
        footer_ratio=None,
        seed=None,
        out=None,
        as_json=False,
    )
    base.update(over)
    return argparse.Namespace(**base)


@pytest.fixture
def parser() -> argparse.ArgumentParser:
    return cli._build_parser()


def test_build_spec_tier(parser) -> None:
    """The tier branch loads a preset with default footer_ratio and seed."""
    spec = cli._build_spec(_args(tier="small"), parser)
    assert (spec.n_files, spec.img_width, spec.img_height, spec.n_channels) == (
        10_000,
        256,
        256,
        1,
    )
    assert spec.footer_ratio == 0.9
    assert spec.seed == 42


def test_build_spec_explicit(parser) -> None:
    """The explicit branch builds a spec from the four geometry flags."""
    spec = cli._build_spec(_args(n_files=4, width=8, height=16, channels=2), parser)
    assert (spec.n_files, spec.img_width, spec.img_height, spec.n_channels) == (
        4,
        8,
        16,
        2,
    )


def test_build_spec_overrides(parser) -> None:
    """footer_ratio and seed overrides win over the tier defaults."""
    spec = cli._build_spec(_args(tier="small", footer_ratio=0.25, seed=99), parser)
    assert spec.footer_ratio == 0.25
    assert spec.seed == 99


def test_build_spec_conflict(parser) -> None:
    """--tier with an explicit flag is a usage error (exit non-zero)."""
    with pytest.raises(SystemExit) as exc:
        cli._build_spec(_args(tier="small", n_files=4), parser)
    assert exc.value.code != 0


@pytest.mark.parametrize("over", [{}, {"n_files": 4, "width": 8}])
def test_build_spec_incomplete(parser, over: dict) -> None:
    """Neither tier nor a full explicit spec is a usage error."""
    with pytest.raises(SystemExit):
        cli._build_spec(_args(**over), parser)


def test_build_spec_invalid_values(parser) -> None:
    """Out-of-range explicit values surface as a usage error, not a traceback."""
    with pytest.raises(SystemExit):
        cli._build_spec(_args(n_files=0, width=8, height=8, channels=1), parser)


def test_cli_generate_creates_corpus(tmp_path, capsys) -> None:
    """T12: generate writes n_files + a manifest and prints a human summary."""
    out = tmp_path / "corpus"
    code = cli.main(
        [
            "generate",
            "--n-files",
            "4",
            "--width",
            "8",
            "--height",
            "8",
            "--channels",
            "1",
            "--out",
            str(out),
        ]
    )
    assert code == 0
    assert len(list(out.glob("*.raw"))) == 4
    assert len(read_manifest(out / "manifest.jsonl")) == 4
    assert "generate:" in capsys.readouterr().out


def test_cli_generate_json(tmp_path, capsys) -> None:
    """T12: --json emits one stats object with correct files/bytes accounting."""
    out = tmp_path / "corpus"
    code = cli.main(
        [
            "generate",
            "--n-files",
            "4",
            "--width",
            "8",
            "--height",
            "8",
            "--channels",
            "1",
            "--out",
            str(out),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert set(payload) == _JSON_KEYS
    assert payload["files"] == 4
    assert payload["bytes"] == sum(p.stat().st_size for p in out.glob("*.raw"))
    # gib is the exact byte count expressed in gibibytes (bytes stays exact).
    assert payload["gib"] == round(payload["bytes"] / 2**30, 3)


def test_cli_unknown_tier(tmp_path) -> None:
    """T12 (unhappy): an unknown tier exits non-zero via argparse."""
    with pytest.raises(SystemExit) as exc:
        cli.main(["generate", "--tier", "huge", "--out", str(tmp_path / "c")])
    assert exc.value.code != 0


def test_cli_missing_args(tmp_path) -> None:
    """T12 (unhappy): neither tier nor explicit spec exits non-zero."""
    with pytest.raises(SystemExit):
        cli.main(["generate", "--out", str(tmp_path / "c")])


def test_main_module_exposes_main() -> None:
    """The `python -m` entry shim imports and re-exports cli.main."""
    import rgw_ingest_bench.__main__ as entry

    assert entry.main is cli.main
