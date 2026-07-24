"""Unit tests for the RTT probe (T8)."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest

from rgw_ingest_bench.probe import endpoint_host_port, probe_rtt


@pytest.fixture
def listener() -> Iterator[tuple[str, int]]:
    """A bound, listening TCP socket (never accepts) on an ephemeral port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    try:
        yield sock.getsockname()
    finally:
        sock.close()


def test_probe_rtt_against_listener(listener: tuple[str, int]) -> None:
    host, port = listener
    stats = probe_rtt(host, port, samples=5)
    assert stats.samples == 5
    assert stats.median_ms >= 0.0
    assert stats.iqr_ms >= 0.0


def test_probe_rtt_closed_port_raises() -> None:
    """A closed port yields a clean ConnectionError naming host:port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    host, port = sock.getsockname()
    sock.close()  # port is now closed
    with pytest.raises(ConnectionError) as excinfo:
        probe_rtt(host, port, samples=3)
    assert f"{host}:{port}" in str(excinfo.value)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://localhost:8000", ("localhost", 8000)),
        ("https://example.com", ("example.com", 443)),
        ("http://example.com", ("example.com", 80)),
        ("ftp://example.com", ("example.com", 80)),  # unknown scheme → 80 fallback
    ],
)
def test_endpoint_host_port(url: str, expected: tuple[str, int]) -> None:
    assert endpoint_host_port(url) == expected


def test_endpoint_host_port_no_host() -> None:
    with pytest.raises(ValueError):
        endpoint_host_port("http:///no-host")
