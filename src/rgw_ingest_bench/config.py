"""Endpoint configuration for the object-store fixture.

:class:`S3Config` is the single Pydantic model describing how to reach the
store (endpoint, credentials, bucket) plus the ``kind`` label that every result
row records so RGW numbers can never masquerade as MinIO numbers (parent §4).
:func:`make_fs` is the one place an :mod:`s3fs` filesystem is constructed, so
the parent §6.5 connection-pool sizing lives in a single code path.
"""

from __future__ import annotations

import os
from typing import Any, Literal

import s3fs
from pydantic import AnyHttpUrl, BaseModel, SecretStr, model_validator

#: Mapping of model field name to the environment variable that seeds it.
ENV_FIELDS: dict[str, str] = {
    "endpoint_url": "BENCH_S3_ENDPOINT",
    "access_key": "BENCH_S3_ACCESS_KEY",
    "secret_key": "BENCH_S3_SECRET_KEY",
    "bucket": "BENCH_S3_BUCKET",
    "kind": "BENCH_S3_KIND",
}

#: Connection fields that must be present for a usable config.
_REQUIRED = ("endpoint_url", "access_key", "secret_key")


class S3Config(BaseModel):
    """Connection settings for the object-store fixture.

    Parameters
    ----------
    endpoint_url : AnyHttpUrl
        Base URL of the store, e.g. ``http://localhost:8000``.
    access_key : str
        S3 access key (a test-only constant for the fixtures).
    secret_key : SecretStr
        S3 secret key; wrapped so it never leaks through ``repr``/logs.
    bucket : str, optional
        Target bucket. Defaults to ``"bronze"``.
    kind : {"rgw", "minio"}, optional
        Which store this endpoint is. Recorded into every result row so RGW and
        MinIO numbers stay distinguishable. Defaults to ``"rgw"``.
    """

    endpoint_url: AnyHttpUrl
    access_key: str
    secret_key: SecretStr
    bucket: str = "bronze"
    kind: Literal["rgw", "minio"] = "rgw"

    @model_validator(mode="before")
    @classmethod
    def _require_connection(cls, data: Any) -> Any:
        """Raise a friendly error (wrapped as ``ValidationError``) when unset.

        Missing endpoint/credentials is the common first-run mistake, so the
        message points at ``make rgw-up`` and the ``BENCH_S3_*`` variables
        rather than surfacing a bare "field required" list.
        """
        if isinstance(data, dict):
            missing = [name for name in _REQUIRED if not data.get(name)]
            if missing:
                raise ValueError(
                    f"missing S3 connection settings: {', '.join(missing)}. "
                    "Set BENCH_S3_ENDPOINT / BENCH_S3_ACCESS_KEY / "
                    "BENCH_S3_SECRET_KEY (or pass the matching CLI flags), then "
                    "bring a store up with `make rgw-up`."
                )
        return data

    @classmethod
    def from_env(cls, **overrides: Any) -> "S3Config":
        """Build from ``BENCH_S3_*`` environment variables plus CLI overrides.

        Parameters
        ----------
        **overrides
            Field-name keyword arguments (e.g. ``endpoint_url=...``). A non-None
            override wins over the environment; a ``None`` override is ignored
            so it never clobbers an env-provided value.

        Returns
        -------
        S3Config
            The validated configuration.

        Raises
        ------
        pydantic.ValidationError
            If a required connection setting is absent or malformed.
        """
        data: dict[str, Any] = {}
        for field, env_name in ENV_FIELDS.items():
            value = os.environ.get(env_name)
            if value is not None:
                data[field] = value
        for field, value in overrides.items():
            if value is not None:
                data[field] = value
        return cls(**data)


def make_fs(cfg: S3Config, *, max_pool: int = 64) -> s3fs.S3FileSystem:
    """Construct an :mod:`s3fs` filesystem with an explicit connection pool.

    The ``max_pool_connections`` size is set from day one (parent §6.5): ``seed``
    uses modest concurrency, but the same helper serves PR 5, which must pass
    ``max(N) + headroom`` so aiobotocore does not silently serialise requests.

    Parameters
    ----------
    cfg : S3Config
        Endpoint and credentials.
    max_pool : int, optional
        ``max_pool_connections`` for the underlying botocore client. Defaults
        to 64.

    Returns
    -------
    s3fs.S3FileSystem
        A filesystem client; construction is lazy and performs no I/O.
    """
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key.get_secret_value(),
        client_kwargs={"endpoint_url": str(cfg.endpoint_url).rstrip("/")},
        config_kwargs={"max_pool_connections": max_pool},
    )
