# rgw-ingest-bench

Benchmark harness for RGW pipelined ingestion.

- **PR 1 — corpus foundation:** a streamed, deterministic generator for
  synthetic `.raw` files, the parsers that read them back, and a JSONL seed
  manifest — all on local disk, zero infrastructure.
- **PR 2 — environment, seeding & metrics:** an object-store fixture (Ceph RGW
  primary, MinIO for CI), a streamed `seed` command that uploads a corpus, a
  measured RTT probe + netem helper, and the `metrics.py` result models every
  benchmark variant (PR 3+) will populate. No benchmark variants yet.

## Quick start

Package management is [`uv`](https://docs.astral.sh/uv/). From the repo root:

```bash
uv sync --extra dev                                        # create .venv + install
```

### Generate a corpus locally (no infrastructure)

```bash
uv run python -m rgw_ingest_bench generate --tier small --out ./corpus --json
# {"files": 10000, "bytes": ..., "gib": ..., "elapsed_s": ..., "mib_per_s": ..., "files_per_s": ...}
```

### Seed a corpus into an object store

```bash
make rgw-up                                # Ceph RGW fixture (or: make minio-up)
export BENCH_S3_ENDPOINT=http://localhost:8000
export BENCH_S3_ACCESS_KEY=... BENCH_S3_SECRET_KEY=... BENCH_S3_KIND=rgw
uv run python -m rgw_ingest_bench seed --tier small --bucket bronze --seed 42 --json
# {"files": 10000, "bytes": ..., "gib": ..., "elapsed_s": ..., "mib_per_s": ..., "files_per_s": ...}
```

`seed` computes the manifest up front, streams each file straight into an S3
multipart upload (nothing stages on disk), verifies the upload against the
manifest, and appends a full `RunResult` row to `results/seed.jsonl`. It is
resumable (`--resume`) and deterministic across `--jobs`.

### Measure RTT / inject latency (Linux)

```bash
uv run python -m rgw_ingest_bench rtt-probe                # median TCP connect RTT
make netem-set DELAY=1ms && uv run python -m rgw_ingest_bench rtt-probe
make netem-clear
```

The `BENCH_S3_*` variables (`ENDPOINT`, `ACCESS_KEY`, `SECRET_KEY`, `BUCKET`,
`KIND`) configure the store; CLI flags (`--endpoint`, `--bucket`, ...) override
them. `make help` lists the fixture / netem / seed targets.

## Tests

```bash
# Fast gate: unit + in-process moto, 100% line/branch coverage (no Docker)
uv run pytest -m "not integration and not minio and not netem" \
       --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100

# End-to-end generate tests (subprocess)
uv run pytest -m integration -v

# Live-store seed tests (needs make minio-up + BENCH_S3_* env)
uv run pytest -m minio -v
```
