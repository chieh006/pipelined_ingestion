"""Unit tests for the corpus generator (T3, T8, T9, T10 and guards)."""

from __future__ import annotations

import logging

import numpy as np
import pytest
from pydantic import ValidationError

from rgw_ingest_bench import fakeraw
from rgw_ingest_bench.fakeraw import (
    FakeRawSpec,
    _build_footer,
    _build_header,
    _pack_section,
    expected_pixel_bytes,
    generate_corpus,
    generate_file,
)
from rgw_ingest_bench.layout import (
    FOOTER_FIELDS,
    HEADER_SIZE,
    classify_tail,
    expected_size,
    pixel_bytes,
    TailKind,
)
from rgw_ingest_bench.manifest import read_manifest, write_manifest


@pytest.mark.parametrize(
    "kwargs",
    [
        {"n_files": 0, "img_width": 8, "img_height": 8, "n_channels": 1},
        {"n_files": 4, "img_width": -1, "img_height": 8, "n_channels": 1},
        {"n_files": 4, "img_width": 8, "img_height": 0, "n_channels": 1},
        {"n_files": 4, "img_width": 8, "img_height": 8, "n_channels": 0},
        {"n_files": 4, "img_width": 8, "img_height": 8, "n_channels": 33},
        {
            "n_files": 4,
            "img_width": 8,
            "img_height": 8,
            "n_channels": 1,
            "footer_ratio": 1.5,
        },
    ],
)
def test_spec_validation(kwargs: dict) -> None:
    """T3: FakeRawSpec rejects out-of-range fields with a ValidationError."""
    with pytest.raises(ValidationError):
        FakeRawSpec(**kwargs)


def test_determinism_same_seed(tiny_spec: FakeRawSpec, tmp_path) -> None:
    """T8: same seed reproduces byte-identical files and manifest."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    write_manifest(list(generate_corpus(tiny_spec, dir_a)), dir_a / "manifest.jsonl")
    write_manifest(list(generate_corpus(tiny_spec, dir_b)), dir_b / "manifest.jsonl")

    for file_id in range(tiny_spec.n_files):
        name = f"{file_id:08d}.raw"
        assert (dir_a / name).read_bytes() == (dir_b / name).read_bytes()
    assert (dir_a / "manifest.jsonl").read_bytes() == (
        dir_b / "manifest.jsonl"
    ).read_bytes()


def test_determinism_different_seed(tiny_spec: FakeRawSpec, tmp_path) -> None:
    """T8 (unhappy): a different seed changes the footer-presence vector."""
    other = tiny_spec.model_copy(update={"seed": 123})
    flags_a = fakeraw.footer_flags(tiny_spec)
    flags_b = fakeraw.footer_flags(other)
    assert not np.array_equal(flags_a, flags_b)

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    list(generate_corpus(tiny_spec, dir_a))
    list(generate_corpus(other, dir_b))
    differing = [
        f"{i:08d}.raw"
        for i in range(tiny_spec.n_files)
        if (dir_a / f"{i:08d}.raw").read_bytes()
        != (dir_b / f"{i:08d}.raw").read_bytes()
    ]
    assert differing  # at least one file differs


@pytest.mark.parametrize("has_footer", [False, True])
def test_streaming_equivalence(tmp_path, has_footer: bool) -> None:
    """T9: chunked writing equals a one-shot reference; size matches schema."""
    spec = FakeRawSpec(n_files=1, img_width=32, img_height=32, n_channels=1)
    out_path = tmp_path / "f.raw"
    # chunk_size=7 does not divide the 1024-byte pixel region: exercises the
    # partial-slab path.
    entry = generate_file(spec, 3, has_footer, out_path, chunk_size=7)

    total = pixel_bytes(spec.img_width, spec.img_height, spec.n_channels)
    reference = _build_header(spec, 3) + expected_pixel_bytes(3, 0, total).tobytes()
    if has_footer:
        reference += _build_footer(3)

    assert out_path.read_bytes() == reference
    assert entry.size == expected_size(
        spec.img_width, spec.img_height, spec.n_channels, has_footer
    )


@pytest.mark.parametrize("ratio", [0.0, 1.0])
def test_footer_ratio_edges(tmp_path, ratio: float) -> None:
    """T10: ratio 0/1 gives all/none footers; sizes match has_footer on disk."""
    spec = FakeRawSpec(
        n_files=5, img_width=16, img_height=16, n_channels=1, footer_ratio=ratio
    )
    out_dir = tmp_path / "corpus"
    entries = list(generate_corpus(spec, out_dir))

    expect_footer = ratio == 1.0
    for entry in entries:
        assert entry.has_footer is expect_footer
        on_disk = (out_dir / entry.path).stat().st_size
        assert on_disk == entry.size
        assert on_disk == expected_size(
            spec.img_width, spec.img_height, spec.n_channels, expect_footer
        )
        kind = classify_tail(on_disk, spec.img_width, spec.img_height, spec.n_channels)
        assert kind is (TailKind.FOOTER if expect_footer else TailKind.NO_FOOTER)


def test_manifest_entries_match_reload(tiny_spec: FakeRawSpec, tmp_path) -> None:
    """Entries the generator yields survive a manifest round-trip unchanged."""
    out_dir = tmp_path / "corpus"
    yielded = list(generate_corpus(tiny_spec, out_dir))
    write_manifest(yielded, out_dir / "manifest.jsonl")
    assert read_manifest(out_dir / "manifest.jsonl") == yielded


def test_pack_section_length_guard() -> None:
    """Unhappy path: a mis-sized array value fails the byte-span check."""
    array_field = next(f for f in FOOTER_FIELDS if f.count > 1)
    with pytest.raises(ValueError, match="expected"):
        _pack_section(
            (array_field,),
            {array_field.name: np.zeros(5, np.float32)},
            HEADER_SIZE,
        )


def test_generate_corpus_logs_progress(tmp_path, monkeypatch, caplog) -> None:
    """The per-N progress log fires on the boundary file."""
    monkeypatch.setattr(fakeraw, "_LOG_EVERY", 2)
    spec = FakeRawSpec(n_files=4, img_width=8, img_height=8, n_channels=1)
    with caplog.at_level(logging.INFO, logger="rgw_ingest_bench.fakeraw"):
        list(generate_corpus(spec, tmp_path / "corpus"))
    assert "generated 2/4 files" in caplog.text
    assert "generated 4/4 files" in caplog.text
