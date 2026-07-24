"""Unit tests for S3Config and make_fs (T7)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from rgw_ingest_bench.config import ENV_FIELDS, S3Config, make_fs


@pytest.fixture(autouse=True)
def _clear_bench_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure ambient BENCH_S3_* vars never leak into these tests."""
    for env_name in ENV_FIELDS.values():
        monkeypatch.delenv(env_name, raising=False)


def test_from_env_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BENCH_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("BENCH_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("BENCH_S3_SECRET_KEY", "sk")
    monkeypatch.setenv("BENCH_S3_BUCKET", "silver")
    monkeypatch.setenv("BENCH_S3_KIND", "minio")
    cfg = S3Config.from_env()
    assert str(cfg.endpoint_url).startswith("http://localhost:9000")
    assert cfg.access_key == "ak"
    assert cfg.secret_key.get_secret_value() == "sk"
    assert cfg.bucket == "silver"
    assert cfg.kind == "minio"


def test_from_env_missing_keys_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing credentials raise ValidationError naming the recovery command."""
    monkeypatch.setenv("BENCH_S3_ENDPOINT", "http://localhost:9000")
    with pytest.raises(ValidationError) as excinfo:
        S3Config.from_env()
    assert "make rgw-up" in str(excinfo.value)


def test_cli_override_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-None override wins over the env; a None override is ignored."""
    monkeypatch.setenv("BENCH_S3_ENDPOINT", "http://env-host:9000")
    monkeypatch.setenv("BENCH_S3_ACCESS_KEY", "env-ak")
    monkeypatch.setenv("BENCH_S3_SECRET_KEY", "env-sk")
    cfg = S3Config.from_env(access_key="cli-ak", secret_key=None)
    assert cfg.access_key == "cli-ak"  # override wins
    assert cfg.secret_key.get_secret_value() == "env-sk"  # None override ignored
    assert str(cfg.endpoint_url).startswith("http://env-host:9000")


def test_defaults() -> None:
    cfg = S3Config(
        endpoint_url="http://localhost:8000", access_key="ak", secret_key="sk"
    )
    assert cfg.bucket == "bronze"
    assert cfg.kind == "rgw"


def test_secret_does_not_leak() -> None:
    cfg = S3Config(
        endpoint_url="http://localhost:8000",
        access_key="ak",
        secret_key="super-secret-value",
    )
    assert "super-secret-value" not in repr(cfg)
    assert "super-secret-value" not in str(cfg)
    assert cfg.secret_key.get_secret_value() == "super-secret-value"


def test_direct_construction_missing_endpoint_raises() -> None:
    with pytest.raises(ValidationError) as excinfo:
        S3Config(access_key="ak", secret_key="sk")
    assert "endpoint_url" in str(excinfo.value)


def test_non_dict_input_defers_to_pydantic() -> None:
    """A non-dict input passes the connection guard through to pydantic.

    The ``isinstance(data, dict)`` guard exists so a non-mapping input yields a
    normal ValidationError rather than an AttributeError inside the validator.
    """
    with pytest.raises(ValidationError):
        S3Config.model_validate("not-a-mapping")


def test_make_fs_strips_slash_and_sets_pool() -> None:
    cfg = S3Config(
        endpoint_url="http://localhost:8000/", access_key="ak", secret_key="sk"
    )
    fs = make_fs(cfg, max_pool=33)
    assert fs.client_kwargs["endpoint_url"] == "http://localhost:8000"
    assert fs.config_kwargs["max_pool_connections"] == 33
