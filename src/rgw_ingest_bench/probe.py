"""RTT probe: measure the real round-trip time to the object store.

Parent §4 is emphatic — *measure* the actual RTT, never trust the nominal
netem value. The probe times TCP ``connect()`` (SYN → SYN-ACK ≈ one RTT), which
needs no server cooperation and so behaves identically for RGW and MinIO. Every
benchmark run records the measured :class:`~rgw_ingest_bench.metrics.RttStats`
in its result row; the ``rtt-probe`` CLI command exposes it for interactively
checking a netem setting.
"""

from __future__ import annotations

import socket
import time
from urllib.parse import urlparse

import numpy as np

from .metrics import RttStats

#: Per-connect timeout in seconds; a store on loopback answers far faster.
CONNECT_TIMEOUT_S: float = 2.0

#: Default scheme-to-port fallback when a URL omits an explicit port.
_DEFAULT_PORTS: dict[str, int] = {"http": 80, "https": 443}


def endpoint_host_port(endpoint_url: str) -> tuple[str, int]:
    """Split an endpoint URL into ``(host, port)``.

    Parameters
    ----------
    endpoint_url : str
        A URL such as ``http://localhost:8000``.

    Returns
    -------
    tuple of (str, int)
        Host and port; the port falls back to the scheme default (80/443) when
        the URL omits one.

    Raises
    ------
    ValueError
        If the URL has no host component.
    """
    parsed = urlparse(endpoint_url)
    if not parsed.hostname:
        raise ValueError(f"endpoint URL has no host: {endpoint_url!r}")
    port = parsed.port or _DEFAULT_PORTS.get(parsed.scheme, 80)
    return parsed.hostname, port


def probe_rtt(host: str, port: int, *, samples: int = 21) -> RttStats:
    """Measure TCP connect-time RTT to ``host:port``.

    Parameters
    ----------
    host : str
        Target host.
    port : int
        Target port.
    samples : int, optional
        Number of connect samples to take. Defaults to 21.

    Returns
    -------
    RttStats
        Median and inter-quartile range of the connect times (milliseconds),
        plus the sample count.

    Raises
    ------
    ConnectionError
        If the endpoint cannot be reached; the message names ``host:port`` and
        suggests ``make rgw-up`` (so the probe doubles as a fixture smoke test).
    """
    times_ms: list[float] = []
    for _ in range(samples):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT_S)
        start = time.perf_counter()
        try:
            sock.connect((host, port))
        except OSError as exc:
            raise ConnectionError(
                f"cannot reach {host}:{port} ({exc}); is a store up? try "
                "`make rgw-up` (or `make minio-up`)"
            ) from exc
        finally:
            sock.close()
        times_ms.append((time.perf_counter() - start) * 1000.0)

    arr = np.asarray(times_ms, dtype=float)
    q25, median, q75 = (float(q) for q in np.quantile(arr, [0.25, 0.5, 0.75]))
    return RttStats(median_ms=median, iqr_ms=q75 - q25, samples=samples)
