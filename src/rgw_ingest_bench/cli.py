"""Command-line interface for the corpus foundation (``generate`` only).

``print`` is used here deliberately — this is console output of a CLI tool, not
application logging. Progress and diagnostics still go through :mod:`logging`.
The subcommand table is a dict so later PRs add ``seed``/``run`` as one entry.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Iterator

from pydantic import ValidationError

from .fakeraw import FakeRawSpec, TIER_SPECS, generate_corpus
from .manifest import ManifestEntry, write_manifest

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.jsonl"


def _build_parser() -> argparse.ArgumentParser:
    """Construct the top-level argument parser and its subcommands."""
    parser = argparse.ArgumentParser(
        prog="rgw_ingest_bench",
        description="RGW ingestion benchmark harness (PR 1: corpus generation).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser(
        "generate", help="generate a synthetic .raw corpus on local disk"
    )
    generate.add_argument(
        "--tier", choices=sorted(TIER_SPECS), help="preset corpus tier"
    )
    generate.add_argument("--n-files", type=int, help="explicit spec: number of files")
    generate.add_argument("--width", type=int, help="explicit spec: image width")
    generate.add_argument("--height", type=int, help="explicit spec: image height")
    generate.add_argument(
        "--channels", type=int, help="explicit spec: channels per pixel"
    )
    generate.add_argument(
        "--footer-ratio", type=float, help="override footer fraction [0,1]"
    )
    generate.add_argument("--seed", type=int, help="override generation seed")
    generate.add_argument("--out", type=Path, required=True, help="output directory")
    generate.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="emit one JSON stats object on stdout instead of the human summary",
    )
    return parser


def _build_spec(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> FakeRawSpec:
    """Turn parsed args into a validated :class:`FakeRawSpec`.

    ``--tier`` and the explicit-spec flags are mutually exclusive; invalid or
    conflicting combinations exit non-zero with a usage message via
    ``parser.error``.
    """
    explicit = {
        "n_files": args.n_files,
        "img_width": args.width,
        "img_height": args.height,
        "n_channels": args.channels,
    }
    explicit_given = [value is not None for value in explicit.values()]

    if args.tier is not None:
        if any(explicit_given):
            parser.error(
                "--tier cannot be combined with --n-files/--width/--height/--channels"
            )
        data: dict[str, Any] = TIER_SPECS[args.tier].model_dump()
    elif all(explicit_given):
        data = explicit
    else:
        parser.error(
            "provide --tier, or all of --n-files --width --height --channels"
        )

    if args.footer_ratio is not None:
        data["footer_ratio"] = args.footer_ratio
    if args.seed is not None:
        data["seed"] = args.seed

    try:
        return FakeRawSpec(**data)
    except ValidationError as exc:
        parser.error(f"invalid spec: {exc}")


def _summarize(files: int, total_bytes: int, elapsed_s: float) -> dict[str, Any]:
    """Build the machine-readable throughput summary object.

    ``bytes`` stays the exact integer byte count (used for exact accounting);
    ``gib`` is the same value in gibibytes (``bytes / 2**30``), rounded, for
    human readability — a convenience figure, not the accounting source.
    """
    mib_per_s = total_bytes / elapsed_s / 2**20
    files_per_s = files / elapsed_s
    return {
        "files": files,
        "bytes": total_bytes,
        "gib": round(total_bytes / 2**30, 3),
        "elapsed_s": round(elapsed_s, 6),
        "mib_per_s": round(mib_per_s, 3),
        "files_per_s": round(files_per_s, 3),
    }


def _format_summary(stats: dict[str, Any]) -> str:
    """Render the one-line human summary of a generate run."""
    return (
        f"generate: {stats['files']} files, {stats['bytes']} bytes "
        f"({stats['gib']:.3f} GiB), {stats['elapsed_s']:.2f}s, "
        f"{stats['mib_per_s']:.1f} MiB/s, {stats['files_per_s']:.1f} files/s"
    )


def _cmd_generate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Handle the ``generate`` subcommand."""
    spec = _build_spec(args, parser)
    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / MANIFEST_NAME

    totals = {"files": 0, "bytes": 0}

    def _accumulate(entries: Iterator[ManifestEntry]) -> Iterator[ManifestEntry]:
        for entry in entries:
            totals["files"] += 1
            totals["bytes"] += entry.size
            yield entry

    start = time.perf_counter()
    write_manifest(_accumulate(generate_corpus(spec, out_dir)), manifest_path)
    elapsed_s = max(time.perf_counter() - start, 1e-9)

    stats = _summarize(totals["files"], totals["bytes"], elapsed_s)
    if args.as_json:
        print(json.dumps(stats))
    else:
        print(_format_summary(stats))
    return 0


_COMMANDS: dict[str, Callable[[argparse.Namespace, argparse.ArgumentParser], int]] = {
    "generate": _cmd_generate,
}


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector; defaults to ``sys.argv[1:]`` when ``None``.

    Returns
    -------
    int
        Process exit code (0 on success).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _COMMANDS[args.command](args, parser)
