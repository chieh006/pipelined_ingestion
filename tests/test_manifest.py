"""Unit tests for the seed manifest (T11)."""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from rgw_ingest_bench.fakeraw import FakeRawSpec, generate_corpus
from rgw_ingest_bench.manifest import (
    ManifestEntry,
    read_manifest,
    read_manifest_df,
    write_manifest,
)


@pytest.fixture
def corpus(tmp_path):
    """A tiny generated corpus plus its written manifest path."""
    spec = FakeRawSpec(
        n_files=5, img_width=16, img_height=16, n_channels=1, footer_ratio=0.5, seed=7
    )
    out_dir = tmp_path / "corpus"
    entries = list(generate_corpus(spec, out_dir))
    manifest_path = out_dir / "manifest.jsonl"
    write_manifest(entries, manifest_path)
    return out_dir, manifest_path, entries


def test_manifest_roundtrip(corpus) -> None:
    """T11: write then read returns equal entries."""
    _, manifest_path, entries = corpus
    assert read_manifest(manifest_path) == entries


def test_manifest_df_shape_and_dtypes(corpus) -> None:
    """T11: the DataFrame view has the expected columns and dtypes."""
    _, manifest_path, entries = corpus
    df = read_manifest_df(manifest_path)
    assert df.shape == (len(entries), 4)
    assert df.columns == ["path", "size", "has_footer", "file_id"]
    assert df.schema["size"] == pl.Int64
    assert df.schema["has_footer"] == pl.Boolean
    assert df.schema["file_id"] == pl.Int64
    assert df.schema["path"] == pl.String


def test_manifest_sizes_match_disk(corpus) -> None:
    """T11: each entry's size equals the file's actual on-disk size."""
    out_dir, _, entries = corpus
    for entry in entries:
        assert (out_dir / entry.path).stat().st_size == entry.size


def test_manifest_skips_blank_lines(corpus) -> None:
    """A trailing blank line is tolerated on read."""
    _, manifest_path, entries = corpus
    with manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    assert read_manifest(manifest_path) == entries


def test_manifest_corrupt_line_fails_loudly(corpus) -> None:
    """T11 (unhappy): a garbage line raises rather than parsing silently."""
    _, manifest_path, _ = corpus
    with manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("this is not json\n")
    with pytest.raises(ValidationError):
        read_manifest(manifest_path)


def test_manifest_entry_rejects_bad_types() -> None:
    """A ManifestEntry validates its field types."""
    with pytest.raises(ValidationError):
        ManifestEntry(path="x.raw", size="big", has_footer=True, file_id=1)
