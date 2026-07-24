# Progress Log — Pipelined Ingestion Benchmark

Running log of work done on the RGW pipelined-ingestion benchmark project.

**Rule:** append-only, newest first; the **Next up** pointer stays pinned at the top, above the dated entries — one dated entry per date (a `- **YYYY-MM-DD** —` line, with concise sub-bullets grouping that day's changes when there are several).

---

**Next up:** finish the §8.3 walkthrough — step 4, `uv run pytest -m minio -k throughput -s`, to watch the measured MiB/s; then open **PR 2** for review and
implement PR 3 (V2 serial ranged reads + correctness gate) per
[pr3_v2_serial_ranged_and_gate.md](design/pr3_v2_serial_ranged_and_gate.md).

- **2026-07-23** — Ran the §8.3 `make`-path walkthrough (steps 1–3): replaced broken Docker Desktop WSL integration with **native Docker CE in WSL2** (systemd-managed), then `make minio-up` healthy and `pytest -m minio -v` → 2 passed.

- **2026-07-21** — Implemented **PR 1 and PR 2** on branch `pr1-corpus-foundation`, both at **100% line/branch coverage** on the fast gate.
  - **PR 1 (corpus foundation):** `uv`/`src` scaffold + deterministic generator, parsers, and `generate` CLI (synthetic pixels full 0–255; `--json` gained a readable `gib` beside exact `bytes`); 53 tests green; §7.3 walkthrough validated (`generate --tier small` → 10k files, 1.191 GiB).
  - **Version + docs:** made the package version single-source (hatch `dynamic` reading `__init__.py`, root CLAUDE.md §7); reconciled the PR 2 design doc with as-built PR 1.
  - **PR 2 (environment, seeding & metrics):** `config.py` (`S3Config`/`make_fs`, `BENCH_S3_*`), `metrics.py` (counters, latency/RSS/loop-lag/periodic samplers, frozen `RunResult`/`EnvInfo` + JSONL sink), `probe.py` (TCP-connect RTT), and a streamed `seed` command (upfront manifest → threaded multipart upload → LIST-vs-manifest verify → dogfooded `RunResult`; `--resume`, `--json`, byte-identical across `--jobs`).
  - **Refactors + infra:** three behaviour-preserving PR 1 touches (`iter_file_chunks`, shared argparse parent parser, public `footer_flags`); `docker-compose.yml` (RGW+MinIO profiles), `Makefile`, `scripts/netem.sh`; deps `s3fs`/`moto[server]`/`pytest-asyncio`; `__version__` → 0.2.0; tests T1–T15 (moto) + I1–I3 (marked live-store).
  - **Live-verified both fixtures** (Docker Desktop, real pinned digests): `pytest -m minio` green against MinIO *and* Ceph RGW; `seed --tier small` (10k, 1.191 GiB) ran **54 MiB/s** (MinIO) / **63 MiB/s** (RGW), each with a valid `RunResult` (correct `store_kind`, measured RTT, versions, git SHA).
  - **Smoke-test fixes:** RGW's beast frontend is on **8080** not the exposed 80 (→ `8000:8080`); the demo entrypoint needs `CEPH_PUBLIC_NETWORK` (static-IP net); corrected I2's test expectation (`--resume` self-heals wrong-sized objects, so verify passes).

- **2026-07-19** — Decided the PR split (5 core + 1 optional, ordered
  PR 1 corpus → PR 2 environment → V2 → V1 → V3 → sweep/report) and wrote
  all six PR implementation design docs in [docs/design/](design/).
