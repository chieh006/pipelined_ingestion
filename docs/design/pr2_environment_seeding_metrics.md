# PR 2 Implementation Design — Environment, Seeding & Metrics

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§4 test environment, §7 metrics, §11 risks, §14 fixture decision)
**Depends on:** [pr1_corpus_foundation.md](pr1_corpus_foundation.md) — the
generator, manifest, and layout modules land there; this PR consumes them.
**Scope:** everything needed to stand up an object store, put a corpus in it,
and record measurements — but **no benchmark variants yet** (V1–V3 are PR 3+).

---

## 1. Goal & non-goals

### 1.1 Goal

After this PR merges, the following works end-to-end on the WSL2 box:

```bash
make rgw-up                                          # Ceph RGW container, healthy
uv run python -m rgw_ingest_bench seed --tier medium --bucket bronze --seed 42
uv run python -m rgw_ingest_bench rtt-probe          # measured RTT to endpoint
make netem-set DELAY=1ms && make netem-clear         # latency injection helper
```

Deliverables:

1. **`docker-compose.yml`** — Ceph RGW (primary fixture) + MinIO (CI/correctness
   profile), both pinned by image digest.
2. **Makefile** — `rgw-up` / `rgw-down` / `minio-up` / `minio-down` /
   `netem-set` / `netem-clear` / `seed` / `test`.
3. **`seed` CLI command** — streamed generate-and-upload with concurrency,
   manifest emission, post-upload verification.
4. **RTT probe** (`rtt-probe` command + library function) and **netem helper
   script** — parent §4: *"measure actual RTT … never trust the nominal netem
   value."*
5. **`metrics.py`** — the Pydantic result models, counters, latency recorder,
   and periodic samplers that every variant (PR 3+) will populate.
6. **`config.py`** — `S3Config` (endpoint/credentials/bucket), Pydantic,
   env-driven.
7. **Version bump** — `src/rgw_ingest_bench/__init__.py` `__version__`
   **`0.1.0 → 0.2.0`** (MINOR: new `config.py` + `metrics.py`, `seed` /
   `rtt-probe` commands; behaviour-preserving refactors to `fakeraw.py` /
   `cli.py`, §2). This is the only file to touch: PR 1 wired `pyproject.toml`
   to hatch `dynamic = ["version"]`, so `__init__.py` is the single source of
   truth (root CLAUDE.md §7).

### 1.2 Non-goals (deferred)

| Deferred item | Lands in |
|---|---|
| Any variant (`v0`–`v3`), `run` command, correctness gate | PR 3+ |
| `S3File._fetch_range` instrumentation wrap | PR 4 (V1) |
| Queue-depth sampling *usage*, writer stats *population* | PR 5 (V3) — the probe/model slots are defined here |
| `sweep` / `report` commands, figures | PR 6 |
| Ceph server-side tuning beyond the one `osd_memory_target` knob | out of scope entirely (parent §1.3) |

---

## 2. Compatibility contract with PR 1

PR 2 **consumes** these PR 1 interfaces unchanged:

| PR 1 interface | Used by |
|---|---|
| `FakeRawSpec`, `TIER_SPECS` | `seed` flag parsing (same `--tier` / explicit-spec groups as `generate`) |
| `layout.expected_size(...)` | upfront manifest computation (§5.2) and post-upload verification |
| `ManifestEntry`, `write_manifest`, `read_manifest_df` | manifest emission and verification |
| footer-presence vector — pure function of `(seed, n_files, footer_ratio)` (PR 1 §4.2) | upfront manifest computation; upload-order independence |
| CLI subcommand registry dict (PR 1 §8) | `seed` and `rtt-probe` are two new entries |

PR 2 makes **three small, behaviour-preserving changes** to PR 1 code, and
nothing else — `generate`'s bytes, manifest, and CLI surface all stay unchanged:

**1. Streaming hook for `seed`:**

> **`fakeraw.iter_file_chunks(spec, file_id, has_footer, *, chunk_size) ->
> Iterator[bytes]`** — extracted from the body of `generate_file`, which
> becomes a thin wrapper (`open(out_path,'wb')` + write loop). Byte output is
> identical (asserted by PR 1's T9 streaming-equivalence test, which keeps
> passing untouched). This gives `seed` a way to stream a file's bytes
> straight into an S3 multipart upload without staging the corpus on local
> disk — `medium` is ~10.6 GiB and the WSL2 box shouldn't need that free.

**2. Shared CLI flags lifted into an `argparse` parent parser** (`cli.py`, §5.2):

> PR 1 defines the tier / explicit-spec flags inline on the `generate`
> subparser; `seed` needs the same group, so those definitions move **once**
> into a common parent parser that both subcommands inherit — no duplicated
> flag definitions. A pure refactor: `generate`'s flags, mutually-exclusive
> groups, and `--json` output stay byte-for-byte identical; only the
> definition *site* moves.

**3. Footer-vector helper promoted from private to public** (`fakeraw.py`):

> `_footer_flags` becomes public `footer_flags(spec)` — the pure
> `(seed, n_files, footer_ratio)` draw the table above lists as a consumed
> interface. `seed`'s upfront manifest (§5.1) recomputes footer presence from
> it, so a corpus and its uploaded copy carry identical footers with no
> duplicated draw logic. Rename only: `generate_corpus`'s single caller is
> updated and its behaviour is unchanged.

Determinism carries over intact: because every byte is a pure function of
`(seed, file_id)` and footer presence is one upfront vector draw, **parallel
upload order cannot change corpus content or the manifest** — same guarantee,
new transport.

---

## 3. Object-store fixture (`docker-compose.yml`)

Roles per parent §4 and the §14 resolution — **RGW is primary; MinIO is a
correctness/CI fallback, never headline numbers.**

### 3.1 Services

```yaml
services:
  rgw:                                # profile: ["rgw"]
    image: quay.io/ceph/demo@sha256:<pinned-digest>
    environment:
      # exact demo-container env keys (MON_IP, CEPH_PUBLIC_NETWORK,
      # CEPH_DEMO_ACCESS_KEY/SECRET_KEY, RGW_NAME, ...) finalized during the
      # §14 smoke test and pinned here; see §10 Open questions
      OSD_MEMORY_TARGET: "1073741824"   # 1 GiB — parent §11, 7 GiB WSL2 cap
    ports: ["8000:8000"]
    healthcheck: {test: curl -sf http://localhost:8000, interval: 5s, retries: 60}

  minio:                              # profile: ["minio"]
    image: minio/minio@sha256:<pinned-digest>
    command: server /data
    environment: {MINIO_ROOT_USER: bench, MINIO_ROOT_PASSWORD: bench-secret}
    ports: ["9000:9000"]
    healthcheck: {test: curl -sf http://localhost:9000/minio/health/live, interval: 2s, retries: 30}
```

Design points:

- **Compose profiles** keep the two stores mutually exclusive by default:
  `docker compose --profile rgw up -d` vs `--profile minio up -d`. Nothing
  starts with a bare `up`.
- **Digest pinning, not tags** (parent §14a: disposability + exact pinning is
  *why* the container beat microceph). `compose down -v` is the documented
  full reset.
- **No named volumes for RGW data** beyond what the demo image requires — the
  fixture is disposable by design; re-seed after reset.
- Both services publish plain HTTP on loopback; credentials are test-only
  constants, never real secrets.

### 3.2 Makefile

```make
rgw-up:      ## start Ceph RGW fixture, wait for healthy
rgw-down:    ## docker compose --profile rgw down -v   (full reset)
minio-up:    ## start MinIO (CI/correctness only)
minio-down:
seed:        ## uv run python -m rgw_ingest_bench seed --tier $(TIER) --bucket $(BUCKET)
netem-set:   ## sudo scripts/netem.sh set $(DELAY)     (e.g. DELAY=1ms)
netem-clear: ## sudo scripts/netem.sh clear
test:        ## uv run pytest with coverage flags
```

`rgw-up`/`minio-up` block until the healthcheck passes (compose
`--wait`), so `make rgw-up && make seed` is scriptable. Targets are thin —
all logic lives in compose/scripts/Python so Windows users can run the
underlying commands directly (Make is a convenience, not a dependency;
project cross-platform rule applies to the *Python* code, and all of it uses
`pathlib`).

---

## 4. `config.py` — endpoint configuration

```python
class S3Config(BaseModel):
    """Connection settings for the object-store fixture."""
    endpoint_url: AnyHttpUrl            # e.g. http://localhost:8000
    access_key: str
    secret_key: SecretStr
    bucket: str = "bronze"
    kind: Literal["rgw", "minio"] = "rgw"   # recorded into every result row

    @classmethod
    def from_env(cls, **overrides) -> "S3Config":
        """Build from BENCH_S3_* environment variables + CLI overrides."""
```

- Env variables (`BENCH_S3_ENDPOINT`, `BENCH_S3_ACCESS_KEY`,
  `BENCH_S3_SECRET_KEY`, `BENCH_S3_BUCKET`, `BENCH_S3_KIND`) with the compose
  files' test credentials documented in the README as defaults. CLI flags
  override env. Missing endpoint/keys → `ValidationError` with a message
  pointing at `make rgw-up`.
- `kind` is *declared* here and *cross-checked* later: every result row
  records it (parent §4: MinIO numbers must never masquerade as RGW numbers).
- One constructor for the s3fs filesystem lives here too:

```python
def make_fs(cfg: S3Config, *, max_pool: int = 64) -> s3fs.S3FileSystem:
    """s3fs client with explicit max_pool_connections (parent §6.5)."""
```

  The §6.5 pool-size gotcha is handled from day one — `seed` uses modest
  concurrency, but the helper takes `max_pool` so PR 5 passes `max(N) +
  headroom` through the same single code path.

New runtime dep: **`s3fs`** (pins `aiobotocore` transitively; both pinned in
`pyproject.toml` and captured into every result row, §7). Still no `pyarrow`,
no `pandas`.

---

## 5. `seed` command — streamed generate-and-upload

### 5.1 Flow

```
FakeRawSpec (tier/flags, seed)
     │
     ├─ 1. compute manifest UPFRONT  (no bytes generated yet)
     │      footer vector = rng(seed) draw          (PR 1 §4.2)
     │      size_i = expected_size(W, H, C, has_footer_i)
     │      → list[ManifestEntry], written to manifests/<tier>-seed<seed>.jsonl
     │
     ├─ 2. ensure bucket exists (fs.mkdirs, idempotent)
     │
     ├─ 3. upload pool: J workers over file_id = 0 … n-1
     │      worker: for chunk in iter_file_chunks(spec, fid, has_footer):
     │                  s3file.write(chunk)          # s3fs multipart, streamed
     │
     └─ 4. verify: paged LIST of bucket → polars join against manifest df
            count, per-key size must match exactly; mismatch → exit 1
```

Design points:

- **Manifest before bytes.** Because PR 1 made size a closed-form function of
  the spec, the complete manifest exists before the first upload — so a
  crashed/interrupted seed can be *resumed* (`--resume`: LIST the bucket,
  skip keys whose size already matches) and verification is a pure
  manifest-vs-LIST comparison. The manifest also gets a copy uploaded to the
  bucket (`_manifest.jsonl`) for provenance.
- **Concurrency = thread pool** (`--jobs`, default 16),
  `concurrent.futures.ThreadPoolExecutor` over blocking s3fs writes. Seeding
  is a one-time cost (parent §14 explicitly rules it out as a deciding
  criterion) — it needs to be *robust and streamed*, not maximally fast; the
  async machinery waits for V3 where it's the thing under test. Peak memory
  ≈ `jobs × chunk_size` (16 × 4 MiB = 64 MiB), independent of tier.
- **Determinism under parallelism** is inherited from PR 1 §4.2: workers only
  ever compute pure functions of `(seed, file_id)`, so `--jobs 1` and
  `--jobs 32` produce byte-identical objects.
- **Failure handling:** per-file retries (≤3, exponential backoff) on
  5xx/timeout; a file that still fails aborts the seed with a clear error —
  unlike benchmark runs (parent §6.3), a partially seeded corpus is useless,
  so no dead-letter here.
- Object keys are exactly PR 1's manifest `path` values
  (`{file_id:08d}.raw`, POSIX-style) prefixed by the bucket — the PR 1
  decision to store relative POSIX paths is what makes local paths and S3
  keys the same string.
- Progress: `logging.info` every 1 000 files with running MiB/s; final
  one-line summary via `print` — files, bytes (with the GiB equivalent),
  elapsed, **and throughput (MiB/s, files/s)** — so a `seed` run doubles as a
  quick throughput check
  (CLI console output, allowed). `--json` (§5.2) emits those same figures as
  one machine-readable object on stdout for scripting and the §8.2 throughput
  integration test.

### 5.2 CLI shape

```bash
uv run python -m rgw_ingest_bench seed --tier medium --bucket bronze --seed 42 \
       [--endpoint http://localhost:8000] [--jobs 16] [--resume] [--no-verify] \
       [--manifest-out manifests/] [--json]
```

Tier/explicit-spec flag groups are shared with `generate` via a common
`argparse` parent parser (defined once in `cli.py` — no duplicated flag
definitions; this is §2's behaviour-preserving change (2), which lifts PR 1's
inline `generate` flags into that parent parser). `seed` registers in the PR 1
subcommand dict; `generate`'s flags and `--json` output are unchanged — only
the definition site moves.

---

## 6. Latency: netem helper + RTT probe

### 6.1 `scripts/netem.sh` (Linux-only by nature)

```bash
scripts/netem.sh set 1ms     # tc qdisc replace dev <IFACE> root netem delay 1ms
scripts/netem.sh clear       # tc qdisc del dev <IFACE> root
scripts/netem.sh show
```

- `IFACE` resolution: default is the docker bridge / container veth (found via
  the container's `iflink`, so only benchmark traffic is delayed — the §14b
  argument for containers over microceph); `NETEM_IFACE=lo` overrides for the
  loopback case.
- Requires root → invoked via `sudo` from the Make targets; the script is
  ~30 lines, `set -euo pipefail`, shellcheck-clean. It is deliberately *not*
  Python: `tc` is Linux-only, and keeping it out of the package keeps the
  package cross-platform.
- Delay `d` per direction ⇒ RTT ≈ `2d` (parent §4). The nominal value is
  **never** recorded as truth — that is the probe's job:

### 6.2 RTT probe (`rtt-probe` command + `probe_rtt()` function)

```python
def probe_rtt(host: str, port: int, *, samples: int = 21) -> RttStats:
    """Median/IQR of TCP connect times to the endpoint, in milliseconds."""
```

- TCP connect-time probe (parent §4: "TCP connect/echo probe"): open a socket,
  time `connect()`, close; 21 samples; report `RttStats(median_ms, iqr_ms,
  samples)`. Connect-time ≈ one RTT (SYN → SYN-ACK) and needs no server-side
  cooperation, which keeps the probe identical for RGW and MinIO.
- Unreachable endpoint → clean error naming host:port and suggesting
  `make rgw-up` — this doubles as the fixture smoke test.
- Every benchmark run (PR 3+) calls `probe_rtt` at start and stores the
  result in its result row (§7); `rtt-probe` as a CLI command exists so netem
  settings can be verified interactively (`make netem-set DELAY=1ms` →
  `rtt-probe` should read ≈ 2 ms + baseline).

---

## 7. `metrics.py` — result models, counters, samplers

The measurement toolkit of parent §7.1, built now so PR 3's first variant has
somewhere to put numbers. Three layers: **collectors** (mutable, used during a
run), **summary models** (frozen Pydantic, computed at run end), and the
**JSONL sink**.

### 7.1 Collectors

```python
class CounterSet:
    """Named monotonic counters (bytes_fetched, get_count, head_count, ...).

    Lock-guarded increments — the asyncio loop is single-threaded, but PR 5's
    optional to_thread writer offload must not race the counts.
    """
    def incr(self, name: str, n: int = 1) -> None
    def snapshot(self) -> dict[str, int]

class LatencyRecorder:
    """Append-only per-operation latency samples (ms), one instance per op type."""
    def record(self, ms: float) -> None
    def summarize(self) -> LatencySummary        # p50/p95/p99/max/count via np.quantile

class PeriodicSampler:
    """Async task sampling named probes every `interval_s` (default 0.1).

    probes: dict[str, Callable[[], float]] — e.g. {"rss_mib": ..., "queue_depth": ...}.
    Start/stop via async context manager; cancellation-safe; samples kept as
    parallel lists (t, name, value) and exported to a polars DataFrame.
    """

class EventLoopLagProbe:
    """Async task: sleep(0.1) in a loop, record (actual - expected) drift ms."""
```

Notes:

- RSS probe reads `/proc/self/statm` when it exists, else falls back to
  `resource.getrusage(...).ru_maxrss` (peak-only). Both paths are exercised
  in tests; benchmarks themselves run on Linux/WSL2 so `/proc` is the real
  path. No `psutil` dependency.
- Raw latency samples are kept in memory (10 k files × ~3 ops × 8 bytes ≈
  nothing) and summarized at run end; full histograms can be dumped with
  `--dump-samples` for the PR 6 latency-inflation figure.
- Queue-depth and writer-stat *probes* are just entries in
  `PeriodicSampler.probes` — PR 5 plugs them in; nothing here changes.

### 7.2 Summary & environment models (frozen Pydantic)

```python
class RttStats(BaseModel):      median_ms: float; iqr_ms: float; samples: int
class LatencySummary(BaseModel):op: str; count: int; p50_ms: float; p95_ms: float; p99_ms: float; max_ms: float
class EnvInfo(BaseModel):
    """Everything parent §7.1 'Environment' demands, captured programmatically."""
    python: str; platform: str; kernel: str
    package_versions: dict[str, str]      # s3fs, aiobotocore, botocore, numpy, polars, pydantic
    git_sha: str                          # subprocess, "unknown" fallback
    store_kind: Literal["rgw", "minio"]
    rtt: RttStats                         # measured, never nominal
    netem_nominal: str | None             # e.g. "1ms" if provided, informational only
    corpus_tier: str; corpus_seed: int
class RunResult(BaseModel):
    """One JSONL row = one run (or one seed)."""
    run_id: str                           # uuid4
    started_at: datetime                  # UTC
    command: str                          # "seed" | "run" | ...
    variant: str | None                   # None for seed; "v1".."v3" later
    params: dict[str, Any]                # n_inflight, queue_bound, jobs, ...
    wall_s: float; files: int; files_per_s: float
    counters: dict[str, int]
    latencies: list[LatencySummary]
    env: EnvInfo
def append_result(result: RunResult, path: Path) -> None
    """Append one model_dump_json() line; parent dir created; atomic-append open."""
```

- Package versions via `importlib.metadata.version` — this is the parent
  §11 "defaults drift across versions" mitigation, recorded per-row, free.
- **`seed` dogfoods the whole stack**: it emits a `RunResult`
  (`command="seed"`, counters = bytes/PUT counts, one `LatencySummary` for
  PUTs, full `EnvInfo`) into `results/seed.jsonl`. Metrics code therefore has
  a real production caller in this same PR, not just tests — and seeding
  throughput regressions become visible for free (the `seed --json` summary
  surfaces the same `files_per_s` / byte-rate that §8.2's I1 asserts on).
- Analysis side stays `polars.read_ndjson` (PR 6); nothing in `metrics.py`
  imports pandas or pyarrow.

---

## 8. Test plan

Same regime as PR 1: `pytest` + fixtures, `tmp_path` everywhere, 100 %
line/branch on new Python code. New dev deps: **`moto[server]`** (in-process
S3 for seed tests — CI needs no Docker) and **`pytest-asyncio`** (sampler
tests). The shell script and compose file are validated by lightweight checks
(T12), not unit-tested. The plan splits into fast, moto-backed **unit tests**
(§8.1, which carry the 100 % coverage gate), **integration tests** against a
live object store (§8.2, where CLI throughput is verified), and a **run
walkthrough** (§8.3). The `minio` and `netem` markers are registered in
`pyproject.toml` `[tool.pytest.ini_options]`, so `-m "not minio and not netem"`
selects the fast gate.

### 8.1 Unit test matrix

| # | Test | Asserts (incl. unhappy paths) |
|---|---|---|
| T1 | `test_counterset` | incr/snapshot; concurrent incr from threads sums exactly (lock works) |
| T2 | `test_latency_recorder` | quantiles match `np.quantile` reference on known data; empty recorder summarize → count 0, NaN-free |
| T3 | `test_periodic_sampler` | collects ≥N samples at short interval; clean cancel; probe raising → logged once, sampler survives |
| T4 | `test_loop_lag_probe` | injected 300 ms blocking call → lag sample > 250 ms observed |
| T5 | `test_rss_probe` | `/proc` path parses on Linux; monkeypatched-away `/proc` → getrusage fallback used |
| T6 | `test_env_info` | all version keys present; git sha matches repo / "unknown" outside a repo (monkeypatched subprocess failure) |
| T7 | `test_s3config` | from_env happy path; missing key → ValidationError; CLI override beats env; secret repr does not leak |
| T8 | `test_rtt_probe` | against an ephemeral local listener: median plausible, sample count honored; closed port → clean error naming endpoint |
| T9 | `test_seed_moto` | seed a tiny spec into moto: object count == n_files, every size matches manifest, `_manifest.jsonl` uploaded, `RunResult` row appended |
| T10 | `test_seed_verify_catches_corruption` | truncate one moto object after upload → verify step exits nonzero and names the key |
| T11 | `test_seed_resume_and_determinism` | interrupt-then-`--resume` uploads only missing keys; `--jobs 1` vs `--jobs 8` → byte-identical objects |
| T12 | `test_fixture_files` | `docker compose config` parses both profiles (skipped if no docker CLI); images are digest-pinned (regex on the YAML); `netem.sh` passes shellcheck if available |
| T13 | `test_cli_seed_args` | bad tier, missing endpoint, conflicting flag groups → nonzero exit + usage; `rtt-probe` command wired |
| T14 | `test_iter_file_chunks_equivalence` | PR 1 refactor guard: chunks concatenated == `generate_file` output byte-for-byte (complements PR 1 T9) |
| T15 | `test_seed_json_summary` | `seed --json` (driven in-process via `cli.main([...])` against moto) prints one stats object `{files, bytes, gib, elapsed_s, mib_per_s, files_per_s}`; `files == n_files`, `bytes == Σ` manifest sizes, `gib == round(bytes/2**30, 3)`, rates self-consistent (`mib_per_s == bytes/elapsed_s/2**20`, `files_per_s == files/elapsed_s`) — covers the throughput-summary branch for the coverage gate; the figures equal the `results/seed.jsonl` `RunResult` row |

### 8.2 Integration tests (live object store)

The unit suite above runs against **moto** (in-process, no Docker): it proves
*correctness* but says nothing about *throughput* — moto never touches a socket.
These integration tests drive the **real CLI** (`sys.executable -m
rgw_ingest_bench …` as a subprocess) against a **live** store — MinIO for CI
(`make minio-up`), RGW for headline numbers (`make rgw-up`) — pointed at it via
the `BENCH_S3_*` env. Marked `@pytest.mark.minio`, run in CI's integration job
and skipped locally by default (parent §12's "MinIO/moto integration marks").

| # | Test | Asserts |
|---|---|---|
| I1 | `test_seed_throughput_cli` | **the CLI throughput check.** `seed --tier small --json` into the live bucket as a subprocess; parse the stats object `{files, bytes, gib, elapsed_s, mib_per_s, files_per_s}`. Assert `files == n_files` and `bytes == Σ` on-store `LIST` sizes (upload accounting is correct); `gib == round(bytes/2**30, 3)`; `mib_per_s == bytes/elapsed_s/2**20` within 1 % (**reported throughput is accurate**, not merely present); `elapsed_s ≤` the test's own wall-clock; an **opt-in floor** — when `BENCH_MIN_SEED_MIB_PER_S` is set, the measured `mib_per_s` must clear it; unset ⇒ accounting-only, so CI never flakes on hardware (suggested trusted-loopback value ≈ 5 MiB/s) — a serialized-upload / non-streaming regression trips wherever the floor is set; finally the stdout figures equal the `results/seed.jsonl` `RunResult` row (`wall_s`, `files_per_s`, byte counters) — CLI, sink, and store all agree. |
| I2 | `test_seed_correctness_minio` | the T9–T11 duplication against **real multipart**: object count/sizes/manifest + `RunResult` row (T9); truncate one object → verify exits nonzero naming the key (T10); interrupt-then-`--resume` uploads only the missing keys, and `--jobs 1` vs `--jobs 8` stay byte-identical (T11). Keeps moto honest about the multipart semantics RGW/MinIO actually enforce. |
| I3 | `test_rtt_probe_netem` | the §6 latency loop end-to-end through the CLI: with `scripts/netem.sh set <d>` applied, `rtt-probe` reads median ≈ `2·d` + baseline; after `clear`, back to baseline. Marked `@pytest.mark.netem` and **skipped unless** `BENCH_NETEM=1` and the process can `sudo tc` (root + Linux) — otherwise it stays the §9 manual acceptance step. |

**Throughput contract (mirrors PR 1's `generate --json`).** So a test — or an
operator — can *verify* seed throughput without scraping log lines, `seed` grows
the same machine-readable summary: `--json` prints exactly one JSON object to
stdout, `{"files", "bytes", "gib", "elapsed_s", "mib_per_s", "files_per_s"}`
(`gib` = the exact `bytes` in gibibytes, `round(bytes/2**30, 3)`), and
suppresses the human summary so stdout stays valid JSON. These are the same
numbers `seed` already records in the `results/seed.jsonl` `RunResult` row
(§7.2); I1 parses the object and cross-checks it against that row. The floor is
opt-in via `BENCH_MIN_SEED_MIB_PER_S` (the PR 1 §7.2 `BENCH_MIN_<command>_<rate>`
convention — subcommand + the rate it bounds) and a regression tripwire, **not**
a benchmark — real seed-throughput and latency-inflation numbers are the
variants' job (PR 3+) and the PR 6 figures.

### 8.3 Running the integration tests (walkthrough)

The unit/moto suite needs nothing but an install; the integration tests need a
live store. From the harness root:

```bash
uv sync --extra dev                  # moto, pytest-asyncio, pytest-cov, …
```

**1 — Fast gate (no Docker, no store): unit + moto, with coverage:**

```bash
uv run pytest -m "not minio and not netem" \
       --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100
```

**2 — Bring up a live store** (MinIO is the zero-Ceph path CI uses):

```bash
make minio-up                        # or: make rgw-up   (headline numbers)
export BENCH_S3_ENDPOINT=http://localhost:9000
export BENCH_S3_ACCESS_KEY=bench BENCH_S3_SECRET_KEY=bench-secret
export BENCH_S3_KIND=minio           # must match the store you started
```

**3 — Run the integration tests** (`seed` correctness + the throughput check):

```bash
uv run pytest -m minio -v
```

**4 — Just the throughput test (I1), watching the number** — `-s` un-captures
stdout so the measured MiB/s prints:

```bash
uv run pytest -m minio -k throughput -s
```

The absolute floor is opt-in: set `BENCH_MIN_SEED_MIB_PER_S` to enforce it on a
store/host you trust (leave it unset ⇒ accounting-only, so CI never flakes on
hardware variance):

```bash
BENCH_MIN_SEED_MIB_PER_S=50 uv run pytest -m minio -k throughput
# Windows PowerShell:  $env:BENCH_MIN_SEED_MIB_PER_S=50; uv run pytest -m minio -k throughput
```

**5 — Verify seed throughput by hand (exactly what I1 automates):**

```bash
uv run python -m rgw_ingest_bench seed --tier small --bucket bronze --seed 42 --json
# {"files": 10000, "bytes": 1719664640, "gib": 1.602, "elapsed_s": 21.3, "mib_per_s": 77.0, "files_per_s": 469.5}

uv run python -m rgw_ingest_bench seed --tier small --bucket bronze --json | jq .mib_per_s
```

**6 — (Linux, optional) verify the netem latency loop (I3):**

```bash
make netem-set DELAY=1ms
uv run python -m rgw_ingest_bench rtt-probe   # median ≈ 2 ms above baseline
make netem-clear
BENCH_NETEM=1 uv run pytest -m netem          # automates the above (needs sudo tc)
```

**7 — Tear down:**

```bash
make minio-down                      # or make rgw-down  (compose down -v: full reset)
```

Notes:

- The 100 % coverage gate runs *only* on `-m "not minio and not netem"`:
  subprocess/live-store tests register no lines, so the moto unit suite carries
  the gate (the `seed --json` branch is covered in-process by T15).
- `BENCH_S3_KIND` must match the store you actually started — a mismatch is
  precisely the RGW-vs-MinIO mislabel §4 guards against, and every result row
  records it.
- Integration tests write only under a unique per-run bucket/prefix and clean up
  after themselves; `compose down -v` is the hard reset if a run is killed.

---

## 9. Acceptance checklist (PR review gate)

- [ ] `make rgw-up && make seed TIER=small && make rgw-down` succeeds on the
      WSL2 box; RSS during seed stays ≈ flat (streamed path verified).
- [ ] `seed --tier medium` completes; verify step passes; rerun with
      `--resume` uploads zero files.
- [ ] `make netem-set DELAY=1ms` → `rtt-probe` reports ≈ 2 ms above baseline;
      `netem-clear` restores it (numbers recorded in the PR description).
- [ ] `results/seed.jsonl` row validates against `RunResult`, contains
      measured RTT + all package versions + git SHA.
- [ ] Fast gate green without Docker (`pytest -m "not minio and not netem"`,
      100 % coverage); `pytest -m minio` (live store) green in CI — including
      I1 `test_seed_throughput_cli`, whose `--json` throughput is self-consistent
      and matches the `results/seed.jsonl` row (verified by hand once via
      `seed --tier small --json`, §8.3).
- [ ] 100 % line/branch coverage on new Python; images digest-pinned; no
      pandas/pyarrow imports; secrets only test constants.

## 10. Open questions

1. **Ceph demo container env + digest** — parent §14 marked the smoke test
   pending. First task of this PR is that smoke test; the compose `<pinned-digest>`
   and exact env keys get filled in from it. Fallback order stands: demo
   container → microceph → MinIO (correctness only).
2. **Veth discovery robustness on WSL2** — if the container-veth lookup proves
   brittle under Docker Desktop's network plumbing, fall back to `NETEM_IFACE=lo`
   and note in results that *all* loopback traffic was delayed (acceptable:
   the box runs nothing else during benchmark runs).
3. **moto vs multipart fidelity** — moto's multipart implementation is good
   but not RGW; the `@pytest.mark.minio` duplicates exist precisely to catch
   drift. If moto misbehaves on streamed multipart, T9–T11 move to
   MinIO-only and the unit suite mocks `make_fs` instead.
4. **Where `results/` lives** relative to the harness-root decision (PR 1
   §10.1) — path is configurable (`--results-dir`), default `./results/`,
   so the answer doesn't block this PR.
