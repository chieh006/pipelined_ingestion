"""Read synthetic ``.raw`` sections back into validated models.

These are the exact functions every ingestion variant reuses: they take a
``bytes`` buffer (a local slice now, a ranged GET later) and never do I/O, which
is what makes them reusable and trivially testable. Decoding is driven entirely
by the :mod:`rgw_ingest_bench.layout` field tables — no hand-repeated offsets.
"""

from __future__ import annotations

import numpy as np
from pydantic import BaseModel

from .fakeraw import expected_pixel_bytes
from .layout import (
    FOOTER_FIELDS,
    FOOTER_MAGIC,
    FOOTER_SIZE,
    HEADER_FIELDS,
    HEADER_SIZE,
    Field,
)


class IntegrityError(Exception):
    """Raised when a footer's magic sentinel does not match."""


class HeaderFields(BaseModel):
    """Decoded header scalars."""

    img_width: int
    img_height: int
    file_id: int
    pixel_size_x: float
    scan_dir: int
    channel_mask: int


class FooterFields(BaseModel):
    """Decoded footer scalars and offset arrays."""

    file_id_echo: int
    n_points: int
    local_offset_x: list[float]
    local_offset_y: list[float]


_HEADER_BY_NAME = {field.name: field for field in HEADER_FIELDS}
_FOOTER_BY_NAME = {field.name: field for field in FOOTER_FIELDS}


def _read(buf: bytes, field: Field) -> np.ndarray:
    """Decode one field from ``buf`` using its dtype/offset/count."""
    return np.frombuffer(buf, dtype=field.dtype, count=field.count, offset=field.offset)


def parse_header(buf: bytes) -> HeaderFields:
    """Decode a header buffer.

    Parameters
    ----------
    buf : bytes
        Buffer of exactly ``HEADER_SIZE`` bytes.

    Returns
    -------
    HeaderFields
        The decoded header scalars.

    Raises
    ------
    ValueError
        If ``buf`` is not exactly ``HEADER_SIZE`` bytes.
    """
    if len(buf) != HEADER_SIZE:
        raise ValueError(f"header buffer must be {HEADER_SIZE} bytes, got {len(buf)}")
    return HeaderFields(
        img_width=int(_read(buf, _HEADER_BY_NAME["img_width"])[0]),
        img_height=int(_read(buf, _HEADER_BY_NAME["img_height"])[0]),
        file_id=int(_read(buf, _HEADER_BY_NAME["file_id"])[0]),
        pixel_size_x=float(_read(buf, _HEADER_BY_NAME["pixel_size_x"])[0]),
        scan_dir=int(_read(buf, _HEADER_BY_NAME["scan_dir"])[0]),
        channel_mask=int(_read(buf, _HEADER_BY_NAME["channel_mask"])[0]),
    )


def parse_footer(buf: bytes) -> FooterFields:
    """Decode a footer buffer, verifying its magic sentinel first.

    Parameters
    ----------
    buf : bytes
        Buffer of exactly ``FOOTER_SIZE`` bytes.

    Returns
    -------
    FooterFields
        The decoded footer scalars and offset arrays.

    Raises
    ------
    ValueError
        If ``buf`` is not exactly ``FOOTER_SIZE`` bytes.
    IntegrityError
        If the leading ``footer_magic`` does not match ``FOOTER_MAGIC`` — e.g.
        the buffer is actually pixel bytes or garbage.
    """
    if len(buf) != FOOTER_SIZE:
        raise ValueError(f"footer buffer must be {FOOTER_SIZE} bytes, got {len(buf)}")
    magic = int(_read(buf, _FOOTER_BY_NAME["footer_magic"])[0])
    if magic != FOOTER_MAGIC:
        raise IntegrityError(
            f"bad footer magic: {magic & 0xFFFFFFFF:#010x} != {FOOTER_MAGIC:#010x}"
        )
    return FooterFields(
        file_id_echo=int(_read(buf, _FOOTER_BY_NAME["file_id_echo"])[0]),
        n_points=int(_read(buf, _FOOTER_BY_NAME["n_points"])[0]),
        local_offset_x=_read(buf, _FOOTER_BY_NAME["local_offset_x"])
        .astype(float)
        .tolist(),
        local_offset_y=_read(buf, _FOOTER_BY_NAME["local_offset_y"])
        .astype(float)
        .tolist(),
    )


def verify_pixel_range(buf: bytes, file_id: int, start: int) -> bool:
    """Check a pixel-region slice against the expected deterministic pattern.

    Parameters
    ----------
    buf : bytes
        The fetched pixel bytes.
    file_id : int
        Logical file identifier the bytes should belong to.
    start : int
        Pixel-region byte index of ``buf[0]``.

    Returns
    -------
    bool
        ``True`` iff every byte matches ``expected_pixel_bytes(file_id, start,
        len(buf))``.
    """
    expected = expected_pixel_bytes(file_id, start, len(buf))
    actual = np.frombuffer(buf, dtype=np.uint8)
    return bool(np.array_equal(actual, expected))
