"""Shared fixed-offset schema for synthetic ``.raw`` files.

This module is the single source of truth for section sizes, field offsets,
and the footer-presence arithmetic. It is imported by both the corpus
generator (:mod:`rgw_ingest_bench.fakeraw`) and the parsers
(:mod:`rgw_ingest_bench.parse`) so the two sides can never drift.

The module performs **no I/O**. Field tables are validated at import time:
an overlapping or out-of-bounds entry raises :class:`ValueError` immediately,
not at analysis time.

Notes
-----
The schema here is the *synthetic* clean-room shape described in the parent
design (§3.1): a 32 KiB fixed-offset header, a large opaque pixel middle, and
an optional 32 KiB trailing footer. It is deliberately **not** the production
offset table.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

import numpy as np
from pydantic import BaseModel, ConfigDict

HEADER_SIZE: Final[int] = 32 * 1024  # 32_768
FOOTER_SIZE: Final[int] = 32 * 1024  # 32_768
PATTERN_MODULUS: Final[int] = 256  # full 8-bit pixel range 0-255; see fakeraw §4.3
FOOTER_MAGIC: Final[int] = 0x0F007E4A  # fixed footer sentinel


class Field(BaseModel):
    """One fixed-offset scalar or array field within a section.

    Parameters
    ----------
    name : str
        Field identifier, unique within its section.
    dtype : str
        NumPy dtype string, e.g. ``"<i4"`` or ``"<f4"``.
    offset : int
        Byte offset of the field within its section.
    count : int, optional
        Number of scalar elements; ``> 1`` marks an array field. Defaults to 1.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    dtype: str
    offset: int
    count: int = 1

    @property
    def itemsize(self) -> int:
        """int: Byte size of a single element of ``dtype``."""
        return np.dtype(self.dtype).itemsize

    @property
    def n_bytes(self) -> int:
        """int: Total byte span of the field (``itemsize * count``)."""
        return self.itemsize * self.count

    @property
    def end(self) -> int:
        """int: Exclusive end offset of the field within its section."""
        return self.offset + self.n_bytes


class TailKind(StrEnum):
    """Classification of an object's trailing 32 KiB (see :func:`classify_tail`)."""

    FOOTER = "footer"
    NO_FOOTER = "no_footer"
    CORRUPT = "corrupt"


HEADER_FIELDS: Final[tuple[Field, ...]] = (
    Field(name="img_width", dtype="<i4", offset=0),
    Field(name="img_height", dtype="<i4", offset=4),
    Field(name="file_id", dtype="<i4", offset=20),
    Field(name="pixel_size_x", dtype="<f4", offset=48),
    Field(name="scan_dir", dtype="<i4", offset=92),
    Field(name="channel_mask", dtype="<i4", offset=17720),
)

FOOTER_FIELDS: Final[tuple[Field, ...]] = (
    Field(name="footer_magic", dtype="<i4", offset=0),
    Field(name="file_id_echo", dtype="<i4", offset=4),
    Field(name="n_points", dtype="<i4", offset=8),
    Field(name="local_offset_x", dtype="<f4", offset=128, count=4070),
    Field(name="local_offset_y", dtype="<f4", offset=16408, count=4070),
)


def validate_fields(
    fields: tuple[Field, ...], section_size: int, section_name: str
) -> None:
    """Assert a field table is in-bounds and free of overlaps.

    Parameters
    ----------
    fields : tuple of Field
        The fields making up one section.
    section_size : int
        Byte size of the section the fields live in.
    section_name : str
        Human-readable section name, used in error messages.

    Raises
    ------
    ValueError
        If any field extends past ``section_size`` or two fields overlap.
    """
    ordered = sorted(fields, key=lambda f: f.offset)
    prev_end = 0
    prev_name = None
    for field in ordered:
        if field.offset < 0 or field.end > section_size:
            raise ValueError(
                f"{section_name} field '{field.name}' spans "
                f"[{field.offset}, {field.end}) outside [0, {section_size})"
            )
        if field.offset < prev_end:
            raise ValueError(
                f"{section_name} field '{field.name}' at offset {field.offset} "
                f"overlaps '{prev_name}' ending at {prev_end}"
            )
        prev_end = field.end
        prev_name = field.name


validate_fields(HEADER_FIELDS, HEADER_SIZE, "header")
validate_fields(FOOTER_FIELDS, FOOTER_SIZE, "footer")


def pixel_bytes(width: int, height: int, channels: int) -> int:
    """Return the byte size of the pixel middle section.

    Parameters
    ----------
    width, height, channels : int
        Image geometry.

    Returns
    -------
    int
        ``width * height * channels``.
    """
    return width * height * channels


def expected_size(width: int, height: int, channels: int, has_footer: bool) -> int:
    """Return the total on-disk size of one ``.raw`` object.

    Parameters
    ----------
    width, height, channels : int
        Image geometry.
    has_footer : bool
        Whether the object carries a trailing footer.

    Returns
    -------
    int
        ``HEADER_SIZE + width*height*channels`` plus ``FOOTER_SIZE`` when
        ``has_footer`` is true.
    """
    size = HEADER_SIZE + pixel_bytes(width, height, channels)
    if has_footer:
        size += FOOTER_SIZE
    return size


def classify_tail(size: int, width: int, height: int, channels: int) -> TailKind:
    """Decide what the trailing 32 KiB of an object is, from its total size.

    Parameters
    ----------
    size : int
        Total on-disk size of the object in bytes.
    width, height, channels : int
        Image geometry the size is checked against.

    Returns
    -------
    TailKind
        ``FOOTER`` if ``size == HEADER + W*H*C + FOOTER``; ``NO_FOOTER`` if
        ``size == HEADER + W*H*C`` (the trailing bytes are pixels); ``CORRUPT``
        otherwise (an integrity error).
    """
    if size == expected_size(width, height, channels, has_footer=True):
        return TailKind.FOOTER
    if size == expected_size(width, height, channels, has_footer=False):
        return TailKind.NO_FOOTER
    return TailKind.CORRUPT
