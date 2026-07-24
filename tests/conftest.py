"""Shared pytest fixtures for the corpus-foundation and seeding test suites."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from rgw_ingest_bench.config import S3Config
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


@pytest.fixture(scope="session")
def _moto_server() -> Iterator[str]:
    """A session-wide in-process moto S3 server; yields its endpoint URL."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    if host == "0.0.0.0":  # bind-all address is not connectable by name on all OSes
        host = "127.0.0.1"
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def moto_endpoint(_moto_server: str) -> Iterator[str]:
    """Yield the moto endpoint with a clean slate for this test.

    moto keeps its backend state in process-global singletons, so every
    ``ThreadedMotoServer`` shares it. Each test therefore resets the server-side
    state (``/moto-api/reset``) and clears the s3fs client-side instance/listing
    cache, so no bucket or listing leaks between tests.
    """
    import requests
    import s3fs

    requests.post(f"{_moto_server}/moto-api/reset", timeout=10)
    s3fs.S3FileSystem.clear_instance_cache()
    try:
        yield _moto_server
    finally:
        s3fs.S3FileSystem.clear_instance_cache()


@pytest.fixture
def s3_cfg(moto_endpoint: str) -> S3Config:
    """An :class:`S3Config` pointing at the moto fixture (bucket ``bronze``)."""
    return S3Config(
        endpoint_url=moto_endpoint,
        access_key="test",
        secret_key="test",
        bucket="bronze",
        kind="minio",
    )
