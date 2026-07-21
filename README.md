# rgw-ingest-bench

Benchmark harness for RGW pipelined ingestion. **PR 1** delivers the corpus
foundation: a streamed, deterministic generator for synthetic `.raw` files, the
parsers that read them back, and a JSONL seed manifest — all on local disk, with
zero infrastructure (no Docker, no Ceph, no network).

## Quick start

Package management is [`uv`](https://docs.astral.sh/uv/). From the repo root:

```bash
uv sync --extra dev                                        # create .venv + install
uv run python -m rgw_ingest_bench generate --tier small --out ./corpus
uv run python -m rgw_ingest_bench generate --tier small --out ./corpus --json
# {"files": 10000, "bytes": ..., "elapsed_s": ..., "mib_per_s": ..., "files_per_s": ...}
```

`generate` streams the corpus to `--out`, writes `<out>/manifest.jsonl`, and
prints a one-line throughput summary (`--json` emits it as a single JSON object).

## Tests

```bash
# Fast gate: in-process unit suite + 100% line/branch coverage (no subprocess, <2s)
uv run pytest -m "not integration" --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100

# End-to-end CLI integration tests (each shells out to a real process)
uv run pytest -m integration -v
```
