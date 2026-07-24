"""Command-line interface: ``generate`` (local), ``seed`` (upload), ``rtt-probe``.

``print`` is used deliberately for CLI console output; progress and diagnostics
go through :mod:`logging`. The subcommand table is a dict so a new command is
one entry. The tier / explicit-spec flags live in one shared parent parser, so
``generate`` and ``seed`` accept an identical spec group with no duplicated flag
definitions.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Iterator

from pydantic import ValidationError

from .config import S3Config
from .fakeraw import TIER_SPECS, FakeRawSpec, generate_corpus
from .manifest import ManifestEntry, write_manifest
from .metrics import throughput_summary
from .probe import endpoint_host_port, probe_rtt
from .seed import DEFAULT_JOBS, SeedError, seed_corpus

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.jsonl"
DEFAULT_RTT_SAMPLES = 21


def _spec_parent() -> argparse.ArgumentParser:
    """Build the parent parser holding the shared tier / explicit-spec flags.

    Both ``generate`` and ``seed`` inherit this via ``parents=[...]``, so the
    spec flag group is defined exactly once (PR 2 §2 change 2).
    """
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--tier", choices=sorted(TIER_SPECS), help="preset corpus tier")
    parent.add_argument("--n-files", type=int, help="explicit spec: number of files")
    parent.add_argument("--width", type=int, help="explicit spec: image width")
    parent.add_argument("--height", type=int, help="explicit spec: image height")
    parent.add_argument(
        "--channels", type=int, help="explicit spec: channels per pixel"
    )
    parent.add_argument(
        "--footer-ratio", type=float, help="override footer fraction [0,1]"
    )
    parent.add_argument("--seed", type=int, help="override generation seed")
    return parent


def _build_parser() -> argparse.ArgumentParser:
    """Construct the top-level parser and its subcommands."""
    parser = argparse.ArgumentParser(
        prog="rgw_ingest_bench",
        description="RGW ingestion benchmark harness (PR 2: seed, metrics, RTT).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    spec_parent = _spec_parent()

    generate = sub.add_parser(
        "generate",
        parents=[spec_parent],
        help="generate a synthetic .raw corpus on local disk",
    )
    generate.add_argument("--out", type=Path, required=True, help="output directory")
    generate.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="emit one JSON stats object on stdout instead of the human summary",
    )

    seed = sub.add_parser(
        "seed",
        parents=[spec_parent],
        help="stream-generate a corpus and upload it to a bucket",
    )
    seed.add_argument("--endpoint", help="store endpoint URL (else BENCH_S3_ENDPOINT)")
    seed.add_argument("--access-key", help="S3 access key (else BENCH_S3_ACCESS_KEY)")
    seed.add_argument("--secret-key", help="S3 secret key (else BENCH_S3_SECRET_KEY)")
    seed.add_argument("--bucket", help="target bucket (else BENCH_S3_BUCKET / bronze)")
    seed.add_argument(
        "--kind", choices=["rgw", "minio"], help="store kind (else BENCH_S3_KIND)"
    )
    seed.add_argument(
        "--jobs", type=int, default=DEFAULT_JOBS, help="upload concurrency"
    )
    seed.add_argument(
        "--resume", action="store_true", help="skip keys already present at size"
    )
    seed.add_argument(
        "--no-verify",
        action="store_true",
        dest="no_verify",
        help="skip the post-upload LIST-vs-manifest verification",
    )
    seed.add_argument(
        "--manifest-out",
        type=Path,
        default=Path("manifests"),
        help="directory for the local manifest",
    )
    seed.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="directory holding seed.jsonl",
    )
    seed.add_argument(
        "--netem-nominal", help="nominal netem delay, recorded for provenance only"
    )
    seed.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="emit one JSON stats object on stdout instead of the human summary",
    )

    rtt = sub.add_parser(
        "rtt-probe", help="measure TCP connect-time RTT to the endpoint"
    )
    rtt.add_argument("--endpoint", help="store endpoint URL (else BENCH_S3_ENDPOINT)")
    rtt.add_argument(
        "--samples", type=int, default=DEFAULT_RTT_SAMPLES, help="connect samples"
    )
    rtt.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="emit one JSON stats object on stdout",
    )
    return parser


def _build_spec(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> FakeRawSpec:
    """Turn parsed spec flags into a validated :class:`FakeRawSpec`.

    ``--tier`` and the explicit-spec flags are mutually exclusive; invalid or
    conflicting combinations exit non-zero with a usage message.
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
        parser.error("provide --tier, or all of --n-files --width --height --channels")

    if args.footer_ratio is not None:
        data["footer_ratio"] = args.footer_ratio
    if args.seed is not None:
        data["seed"] = args.seed

    try:
        return FakeRawSpec(**data)
    except ValidationError as exc:
        parser.error(f"invalid spec: {exc}")


def _format_summary(command: str, stats: dict[str, Any]) -> str:
    """Render the one-line human throughput summary of a run."""
    return (
        f"{command}: {stats['files']} files, {stats['bytes']} bytes "
        f"({stats['gib']:.3f} GiB), {stats['elapsed_s']:.2f}s, "
        f"{stats['mib_per_s']:.1f} MiB/s, {stats['files_per_s']:.1f} files/s"
    )


def _emit(stats: dict[str, Any], command: str, as_json: bool) -> None:
    """Print the throughput summary as JSON or the one-line human form."""
    if as_json:
        print(json.dumps(stats))
    else:
        print(_format_summary(command, stats))


def _cmd_generate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Handle the ``generate`` subcommand (local corpus, unchanged from PR 1)."""
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

    _emit(throughput_summary(totals["files"], totals["bytes"], elapsed_s),
          "generate", args.as_json)
    return 0


def _cmd_seed(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Handle the ``seed`` subcommand: stream-generate and upload a corpus."""
    spec = _build_spec(args, parser)
    tier = args.tier if args.tier is not None else "custom"
    try:
        cfg = S3Config.from_env(
            endpoint_url=args.endpoint,
            access_key=args.access_key,
            secret_key=args.secret_key,
            bucket=args.bucket,
            kind=args.kind,
        )
    except ValidationError as exc:
        parser.error(f"invalid S3 configuration: {exc}")

    try:
        stats = seed_corpus(
            spec,
            cfg,
            tier=tier,
            jobs=args.jobs,
            resume=args.resume,
            verify=not args.no_verify,
            manifest_dir=args.manifest_out,
            results_path=args.results_dir / "seed.jsonl",
            netem_nominal=args.netem_nominal,
        )
    except (SeedError, ConnectionError) as exc:
        logger.error(f"seed failed: {exc}")
        return 1

    _emit(stats, "seed", args.as_json)
    return 0


def _cmd_rtt_probe(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Handle the ``rtt-probe`` subcommand: measure RTT to the endpoint."""
    endpoint = args.endpoint or os.environ.get("BENCH_S3_ENDPOINT")
    if not endpoint:
        parser.error("no endpoint: pass --endpoint or set BENCH_S3_ENDPOINT")
    try:
        host, port = endpoint_host_port(endpoint)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        stats = probe_rtt(host, port, samples=args.samples)
    except ConnectionError as exc:
        logger.error(str(exc))
        return 1

    if args.as_json:
        print(
            json.dumps(
                {
                    "median_ms": stats.median_ms,
                    "iqr_ms": stats.iqr_ms,
                    "samples": stats.samples,
                }
            )
        )
    else:
        print(
            f"rtt-probe: {endpoint} — median {stats.median_ms:.3f} ms, "
            f"iqr {stats.iqr_ms:.3f} ms (n={stats.samples})"
        )
    return 0


_COMMANDS: dict[str, Callable[[argparse.Namespace, argparse.ArgumentParser], int]] = {
    "generate": _cmd_generate,
    "seed": _cmd_seed,
    "rtt-probe": _cmd_rtt_probe,
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
