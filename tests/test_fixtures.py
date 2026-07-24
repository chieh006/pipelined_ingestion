"""Lightweight checks for the non-Python fixtures (T12).

These validate the compose file and netem script without unit-testing them:
digest-pinning is a regex on the YAML, ``docker compose config`` and
``shellcheck`` run only when those tools are usable, and ``bash -n`` always
checks the script parses. They register no coverage on the package.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_COMPOSE = _ROOT / "docker-compose.yml"
_NETEM = _ROOT / "scripts" / "netem.sh"


def test_images_are_digest_pinned() -> None:
    """Every image reference is pinned by @sha256 digest, not a floating tag."""
    images = re.findall(r"image:\s*(\S+)", _COMPOSE.read_text())
    assert images, "no image references found in docker-compose.yml"
    for image in images:
        assert re.search(r"@sha256:[0-9a-f]{64}$", image), f"not pinned: {image}"


def test_compose_config_parses() -> None:
    """`docker compose config` parses both profiles (skipped if unusable here)."""
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker CLI not available")
    for profile in ("rgw", "minio"):
        try:
            result = subprocess.run(
                [docker, "compose", "-f", str(_COMPOSE),
                 "--profile", profile, "config"],
                capture_output=True,
                text=True,
            )
        except OSError as exc:  # binary present but not launchable (e.g. WSL)
            pytest.skip(f"docker CLI not usable: {exc}")
        if result.returncode != 0:
            pytest.skip(f"docker compose not usable: {result.stderr.strip()[:60]}")
        assert profile in result.stdout


def test_netem_shellcheck_clean() -> None:
    shellcheck = shutil.which("shellcheck")
    if shellcheck is None:
        pytest.skip("shellcheck not available")
    result = subprocess.run(
        [shellcheck, str(_NETEM)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout


def test_netem_bash_syntax_ok() -> None:
    """`bash -n` always runs: the script must at least parse."""
    result = subprocess.run(
        ["bash", "-n", str(_NETEM)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
