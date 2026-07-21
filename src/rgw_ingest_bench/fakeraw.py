"""Streamed, deterministic synthetic ``.raw`` corpus generator.

Every byte of the corpus is a pure function of ``(spec.seed, file_id)``: there
is no hidden RNG state, so generation order — or a future re-run under a
different driver — cannot change output bytes. Files are written in streamed
chunks so a large-tier object never materialises whole in RAM.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterator

import numpy as np
from pydantic import BaseModel, Field

from .layout import (
    FOOTER_FIELDS,
    FOOTER_MAGIC,
    FOOTER_SIZE,
    HEADER_FIELDS,
    HEADER_SIZE,
    PATTERN_MODULUS,
    pixel_bytes,
)
from .manifest import ManifestEntry

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE: int = 4 * 2**20  # 4 MiB
_LOG_EVERY: int = 1000

# Number of points in each footer offset array, taken from the schema so the
# generator and the field table can never disagree.
_FOOTER_POINTS: int = next(f.count for f in FOOTER_FIELDS if f.name == "local_offset_x")


class FakeRawSpec(BaseModel):
    """Generation spec for one corpus tier.

    Parameters
    ----------
    n_files : int
        Number of files to generate (``> 0``).
    img_width, img_height : int
        Image geometry in pixels (``> 0``).
    n_channels : int
        Channels per pixel (``1``–``32``).
    footer_ratio : float, optional
        Fraction of files carrying a footer, in ``[0, 1]``. Defaults to 0.9.
    seed : int, optional
        Seed controlling footer presence. Defaults to 42.
    """

    n_files: int = Field(gt=0)
    img_width: int = Field(gt=0)
    img_height: int = Field(gt=0)
    n_channels: int = Field(gt=0, le=32)
    footer_ratio: float = Field(default=0.9, ge=0.0, le=1.0)
    seed: int = 42


TIER_SPECS: dict[str, FakeRawSpec] = {
    "small": FakeRawSpec(n_files=10_000, img_width=256, img_height=256, n_channels=1),
    "medium": FakeRawSpec(
        n_files=10_000, img_width=1024, img_height=1024, n_channels=1
    ),
    "large": FakeRawSpec(n_files=500, img_width=4096, img_height=4096, n_channels=2),
}


def expected_pixel_bytes(file_id: int, start: int, length: int) -> np.ndarray:
    """Return the deterministic pixel pattern for indices ``[start, start+length)``.

    ``byte[i] = (file_id * 31 + i) % 256`` as ``uint8``, so pixel samples span
    the **full 8-bit range 0-255**, emulating real image pixel values. The
    ``file_id * 31`` phase term makes the pattern unique per file, so a read from
    the wrong object still mismatches.

    Parameters
    ----------
    file_id : int
        Logical file identifier (pattern phase).
    start : int
        First pixel-region byte index.
    length : int
        Number of bytes to produce.

    Returns
    -------
    numpy.ndarray
        A ``uint8`` array of shape ``(length,)``.

    Notes
    -----
    The pattern has period ``PATTERN_MODULUS`` (256), so one period is built once
    and tiled — keeping working memory at roughly ``length`` bytes (``uint8``)
    rather than an ``int64`` index array several times the chunk size.

    Because the period is 256, ``verify_pixel_range`` no longer detects a misread
    whose shift is an exact multiple of 256 (those bytes repeat). This is the
    accepted trade-off of full-range pixel values; misdirected reads of *footer*
    files are still caught by ``footer_magic`` / ``file_id_echo``, and every
    variant's output is checked byte-for-byte by the correctness gate.
    """
    phase = (file_id * 31 + start) % PATTERN_MODULUS
    ramp = (np.arange(PATTERN_MODULUS, dtype=np.int64) + phase) % PATTERN_MODULUS
    period = ramp.astype(np.uint8)
    reps = length // PATTERN_MODULUS + 1
    return np.tile(period, reps)[:length]


def _pack_section(fields: tuple, values: dict[str, Any], size: int) -> bytes:
    """Stamp field values into a zeroed section buffer.

    Parameters
    ----------
    fields : tuple of Field
        The section's field table.
    values : dict
        Mapping of field name to scalar or array value.
    size : int
        Section size in bytes.

    Returns
    -------
    bytes
        The packed, fixed-size section.

    Raises
    ------
    ValueError
        If an encoded value does not match its field's byte span.
    """
    buffer = bytearray(size)
    for field in fields:
        # asarray without an explicit dtype lets Python ints land as int64,
        # then astype performs a modular (non-raising) cast to the field dtype —
        # so a full 32-bit channel_mask encodes without an overflow error.
        encoded = np.asarray(values[field.name]).astype(field.dtype).tobytes()
        if len(encoded) != field.n_bytes:
            raise ValueError(
                f"field '{field.name}' encoded to {len(encoded)} bytes, "
                f"expected {field.n_bytes}"
            )
        buffer[field.offset : field.end] = encoded
    return bytes(buffer)


def _build_header(spec: FakeRawSpec, file_id: int) -> bytes:
    """Build the 32 KiB header for one file (see determinism model §4.2)."""
    values = {
        "img_width": spec.img_width,
        "img_height": spec.img_height,
        "file_id": file_id,
        "pixel_size_x": 0.1 * (1 + file_id % 4),
        "scan_dir": file_id % 2,
        "channel_mask": (1 << spec.n_channels) - 1,
    }
    return _pack_section(HEADER_FIELDS, values, HEADER_SIZE)


def _build_footer(file_id: int) -> bytes:
    """Build the 32 KiB footer for one file (see determinism model §4.2)."""
    points = np.arange(_FOOTER_POINTS, dtype=np.int64)
    values = {
        "footer_magic": FOOTER_MAGIC,
        "file_id_echo": file_id,
        "n_points": _FOOTER_POINTS,
        "local_offset_x": (file_id + points * 1e-3).astype(np.float32),
        "local_offset_y": (file_id - points * 1e-3).astype(np.float32),
    }
    return _pack_section(FOOTER_FIELDS, values, FOOTER_SIZE)


def generate_file(
    spec: FakeRawSpec,
    file_id: int,
    has_footer: bool,
    out_path: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> ManifestEntry:
    """Write one synthetic ``.raw`` file and return its manifest entry.

    The header is built in one 32 KiB buffer, the pixel middle is written in
    ``chunk_size`` slabs, and the footer (if present) is appended — so peak
    memory is roughly one chunk regardless of file size.

    Parameters
    ----------
    spec : FakeRawSpec
        Corpus spec supplying geometry.
    file_id : int
        Logical file identifier.
    has_footer : bool
        Whether to append a footer.
    out_path : pathlib.Path
        Destination file path.
    chunk_size : int, optional
        Pixel write-slab size in bytes. Defaults to 4 MiB.

    Returns
    -------
    ManifestEntry
        Entry describing the written file.
    """
    total_pixels = pixel_bytes(spec.img_width, spec.img_height, spec.n_channels)
    with out_path.open("wb") as handle:
        handle.write(_build_header(spec, file_id))
        written = 0
        while written < total_pixels:
            slab = min(chunk_size, total_pixels - written)
            handle.write(expected_pixel_bytes(file_id, written, slab).tobytes())
            written += slab
        if has_footer:
            handle.write(_build_footer(file_id))
    return ManifestEntry(
        path=Path(out_path.name).as_posix(),
        size=out_path.stat().st_size,
        has_footer=has_footer,
        file_id=file_id,
    )


def _footer_flags(spec: FakeRawSpec) -> np.ndarray:
    """Draw the per-file footer-presence vector in one vectorized call."""
    draws = np.random.default_rng(spec.seed).random(spec.n_files)
    return draws < spec.footer_ratio


def generate_corpus(spec: FakeRawSpec, out_dir: Path) -> Iterator[ManifestEntry]:
    """Generate a full corpus, yielding one manifest entry per file.

    Parameters
    ----------
    spec : FakeRawSpec
        Corpus spec.
    out_dir : pathlib.Path
        Output directory; created if missing. Files are named ``{file_id:08d}.raw``.

    Yields
    ------
    ManifestEntry
        One entry per generated file, in ascending ``file_id`` order.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    flags = _footer_flags(spec)
    for file_id in range(spec.n_files):
        out_path = out_dir / f"{file_id:08d}.raw"
        entry = generate_file(spec, file_id, bool(flags[file_id]), out_path)
        if (file_id + 1) % _LOG_EVERY == 0:
            logger.info(f"generated {file_id + 1}/{spec.n_files} files")
        yield entry
