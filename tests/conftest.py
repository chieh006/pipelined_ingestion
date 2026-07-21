"""Shared pytest fixtures for the corpus-foundation test suite."""

from __future__ import annotations

import pytest

from rgw_ingest_bench.fakeraw import FakeRawSpec


@pytest.fixture
def tiny_spec() -> FakeRawSpec:
    """A small spec that keeps the unit suite fast (<2 s).

    8x8x1 geometry, 6 files, half carrying footers, fixed seed.
    """
    return FakeRawSpec(
        n_files=6,
        img_width=8,
        img_height=8,
        n_channels=1,
        footer_ratio=0.5,
        seed=7,
    )


@pytest.fixture
def wide_spec() -> FakeRawSpec:
    """A 256x256x1 spec whose pixel region exceeds one 32 KiB section.

    Needed by tests that read a full footer-sized slice out of the pixel middle.
    """
    return FakeRawSpec(
        n_files=3,
        img_width=256,
        img_height=256,
        n_channels=1,
        footer_ratio=0.5,
        seed=7,
    )
