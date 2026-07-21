"""Seed manifest: the per-object facts a later ranged variant needs.

The manifest is JSONL — one :class:`ManifestEntry` per line — written streamed
as the generator yields, so the whole corpus is never buffered in memory. Every
line is re-validated through Pydantic on read, so a truncated or hand-edited
manifest fails loudly rather than silently.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl
from pydantic import BaseModel


class ManifestEntry(BaseModel):
    """One corpus object: identity plus the facts a ranged read needs.

    Parameters
    ----------
    path : str
        Object path, stored **relative** and POSIX-style so it is valid both as
        a local relative path and as an S3 key, and compares equal across
        Windows and Linux.
    size : int
        On-disk size in bytes.
    has_footer : bool
        Whether the object carries a trailing footer.
    file_id : int
        Zero-based corpus index / logical file identifier.
    """

    path: str
    size: int
    has_footer: bool
    file_id: int


def write_manifest(entries: Iterable[ManifestEntry], path: Path) -> int:
    """Write a JSONL manifest, streamed one entry at a time.

    Parameters
    ----------
    entries : iterable of ManifestEntry
        Entries to serialise; consumed lazily so the corpus list is never
        buffered.
    path : pathlib.Path
        Destination file; overwritten if it exists.

    Returns
    -------
    int
        The number of entries written.
    """
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(entry.model_dump_json())
            handle.write("\n")
            count += 1
    return count


def read_manifest(path: Path) -> list[ManifestEntry]:
    """Read and re-validate a JSONL manifest.

    Parameters
    ----------
    path : pathlib.Path
        Manifest file to read.

    Returns
    -------
    list of ManifestEntry
        One entry per non-empty line.

    Raises
    ------
    pydantic.ValidationError
        If any line is not a valid :class:`ManifestEntry`.
    """
    entries: list[ManifestEntry] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            entries.append(ManifestEntry.model_validate_json(stripped))
    return entries


def read_manifest_df(path: Path) -> pl.DataFrame:
    """Read a JSONL manifest into a Polars DataFrame.

    Parameters
    ----------
    path : pathlib.Path
        Manifest file to read.

    Returns
    -------
    polars.DataFrame
        Columns ``path``, ``size``, ``has_footer``, ``file_id``. This is what
        the correctness gate and later analysis join against.
    """
    return pl.read_ndjson(path)
