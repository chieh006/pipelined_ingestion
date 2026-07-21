"""Unit tests for the shared offset schema (T1, T2)."""

from __future__ import annotations

import pytest

from rgw_ingest_bench.fakeraw import TIER_SPECS
from rgw_ingest_bench.layout import (
    FOOTER_FIELDS,
    FOOTER_SIZE,
    HEADER_FIELDS,
    HEADER_SIZE,
    Field,
    TailKind,
    classify_tail,
    expected_size,
    pixel_bytes,
    validate_fields,
)


def test_layout_tables_valid() -> None:
    """T1: real tables are in-bounds and non-overlapping; the validator bites."""
    # Every field sits inside its section.
    for field in HEADER_FIELDS:
        assert 0 <= field.offset
        assert field.end <= HEADER_SIZE
    for field in FOOTER_FIELDS:
        assert field.end <= FOOTER_SIZE

    # The two float32[4070] arrays fit within the footer.
    array_fields = [f for f in FOOTER_FIELDS if f.count > 1]
    assert len(array_fields) == 2
    assert all(f.n_bytes == 4070 * 4 for f in array_fields)

    # Re-validating the shipped tables must not raise.
    validate_fields(HEADER_FIELDS, HEADER_SIZE, "header")
    validate_fields(FOOTER_FIELDS, FOOTER_SIZE, "footer")


def test_validator_rejects_overlap() -> None:
    """T1 (unhappy): overlapping fields are rejected at validation."""
    overlapping = (
        Field(name="a", dtype="<i4", offset=0),
        Field(name="b", dtype="<i4", offset=2),  # [2,6) overlaps [0,4)
    )
    with pytest.raises(ValueError, match="overlaps"):
        validate_fields(overlapping, HEADER_SIZE, "header")


def test_validator_rejects_out_of_bounds() -> None:
    """T1 (unhappy): a field spilling past the section is rejected."""
    too_big = (Field(name="a", dtype="<f4", offset=HEADER_SIZE - 2),)
    with pytest.raises(ValueError, match="outside"):
        validate_fields(too_big, HEADER_SIZE, "header")


@pytest.mark.parametrize("tier", sorted(TIER_SPECS))
@pytest.mark.parametrize("has_footer", [False, True])
def test_size_arithmetic(tier: str, has_footer: bool) -> None:
    """T2: expected_size matches HEADER + W*H*C (+FOOTER) for every combo."""
    spec = TIER_SPECS[tier]
    middle = pixel_bytes(spec.img_width, spec.img_height, spec.n_channels)
    want = HEADER_SIZE + middle + (FOOTER_SIZE if has_footer else 0)
    got = expected_size(spec.img_width, spec.img_height, spec.n_channels, has_footer)
    assert got == want


def test_classify_tail_exact_and_corrupt() -> None:
    """T2: classify_tail is exact at the two valid sizes, CORRUPT otherwise."""
    w, h, c = 256, 256, 1
    no_footer = expected_size(w, h, c, has_footer=False)
    footer = expected_size(w, h, c, has_footer=True)

    assert classify_tail(footer, w, h, c) is TailKind.FOOTER
    assert classify_tail(no_footer, w, h, c) is TailKind.NO_FOOTER

    # Off by +/-1 at either valid size, and near the footer boundary, are CORRUPT.
    for bad in (no_footer - 1, no_footer + 1, footer - 1, footer + 1):
        assert classify_tail(bad, w, h, c) is TailKind.CORRUPT
