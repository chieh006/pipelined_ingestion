"""Unit tests for reading sections back (T4, T5, T6, T7)."""

from __future__ import annotations

import numpy as np
import pytest

from rgw_ingest_bench.fakeraw import FakeRawSpec, expected_pixel_bytes, generate_file
from rgw_ingest_bench.layout import (
    FOOTER_SIZE,
    HEADER_SIZE,
    classify_tail,
    pixel_bytes,
    TailKind,
)
from rgw_ingest_bench.parse import (
    IntegrityError,
    parse_footer,
    parse_header,
    verify_pixel_range,
)


def _write(spec: FakeRawSpec, file_id: int, has_footer: bool, tmp_path):
    """Generate one file and return (path, raw bytes)."""
    path = tmp_path / f"{file_id:08d}.raw"
    generate_file(spec, file_id, has_footer, path)
    return path, path.read_bytes()


def test_roundtrip_header(tiny_spec: FakeRawSpec, tmp_path) -> None:
    """T4: parsed header fields equal the generation formulas."""
    file_id = 3
    _, raw = _write(tiny_spec, file_id, has_footer=True, tmp_path=tmp_path)
    header = parse_header(raw[:HEADER_SIZE])

    assert header.img_width == tiny_spec.img_width
    assert header.img_height == tiny_spec.img_height
    assert header.file_id == file_id
    assert header.scan_dir == file_id % 2
    assert header.channel_mask == (1 << tiny_spec.n_channels) - 1
    # float32 storage: compare through np.float32, not Python floats.
    assert np.float32(header.pixel_size_x) == np.float32(0.1 * (1 + file_id % 4))


def test_roundtrip_footer(wide_spec: FakeRawSpec, tmp_path) -> None:
    """T5: footer fields (incl. arrays) round-trip for a footer file."""
    file_id = 1
    _, raw = _write(wide_spec, file_id, has_footer=True, tmp_path=tmp_path)
    footer = parse_footer(raw[-FOOTER_SIZE:])

    assert footer.file_id_echo == file_id
    assert footer.n_points == 4070
    assert len(footer.local_offset_x) == 4070
    for i in (0, 1, 2039, 4069):
        assert np.float32(footer.local_offset_x[i]) == np.float32(file_id + i * 1e-3)
        assert np.float32(footer.local_offset_y[i]) == np.float32(file_id - i * 1e-3)


def test_footerless_tail_is_pixels(wide_spec: FakeRawSpec, tmp_path) -> None:
    """T5: a footerless file's trailing 32 KiB classify and verify as pixels."""
    file_id = 0
    path, raw = _write(wide_spec, file_id, has_footer=False, tmp_path=tmp_path)
    size = path.stat().st_size

    assert classify_tail(
        size, wide_spec.img_width, wide_spec.img_height, wide_spec.n_channels
    ) is TailKind.NO_FOOTER

    total_pixels = pixel_bytes(
        wide_spec.img_width, wide_spec.img_height, wide_spec.n_channels
    )
    tail_start = total_pixels - FOOTER_SIZE
    tail = raw[-FOOTER_SIZE:]
    assert verify_pixel_range(tail, file_id, tail_start)


def test_pattern_guard_detects_shift(wide_spec: FakeRawSpec, tmp_path) -> None:
    """T6: a correct pixel range verifies; non-256 shifts and wrong file_id fail."""
    file_id = 2
    _, raw = _write(wide_spec, file_id, has_footer=False, tmp_path=tmp_path)
    pixels = raw[HEADER_SIZE:]
    start = 8192
    length = 1024
    correct = pixels[start : start + length]

    assert verify_pixel_range(correct, file_id, start) is True

    # A range read from a shifted offset, checked against the true start, fails —
    # for any shift that is NOT an exact multiple of the 256-byte period.
    for shift in (1, -1, 3, 255):
        shifted = pixels[start + shift : start + shift + length]
        assert verify_pixel_range(shifted, file_id, start) is False

    # The correct bytes checked against the wrong file_id also fail.
    assert verify_pixel_range(correct, file_id + 1, start) is False


def test_pattern_guard_blind_to_256_multiple_shift(
    wide_spec: FakeRawSpec, tmp_path
) -> None:
    """T6 (documented limitation): full-range (mod 256) values make a shift that
    is an exact multiple of 256 invisible to the pixel guard."""
    file_id = 2
    _, raw = _write(wide_spec, file_id, has_footer=False, tmp_path=tmp_path)
    pixels = raw[HEADER_SIZE:]
    start, length = 8192, 1024

    # A 256-aligned shift produces byte-identical values, so the guard passes —
    # the accepted trade-off of emulating full 0-255 pixel values. Such misreads
    # are instead caught by footer magic / file_id_echo and the correctness gate.
    for shift in (256, 4096):
        shifted = pixels[start + shift : start + shift + length]
        assert verify_pixel_range(shifted, file_id, start) is True


def test_footer_magic_guard() -> None:
    """T7: parse_footer rejects pixel bytes; short buffers raise ValueError."""
    pixels = expected_pixel_bytes(0, 0, FOOTER_SIZE).tobytes()
    with pytest.raises(IntegrityError, match="magic"):
        parse_footer(pixels)

    with pytest.raises(ValueError, match="footer buffer"):
        parse_footer(b"\x00" * 10)
    with pytest.raises(ValueError, match="header buffer"):
        parse_header(b"\x00" * 10)
