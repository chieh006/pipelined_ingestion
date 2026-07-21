# PR 3 Implementation Design — V2 (Serial Ranged GETs), Correctness Gate & `run` Skeleton

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§5 V2 definition, §6.4 silver schema, §7 metrics & correctness gate)
**Depends on:** [pr1_corpus_foundation.md](pr1_corpus_foundation.md) (layout,
parse, manifest), [pr2_environment_seeding_metrics.md](pr2_environment_seeding_metrics.md)
(config, fixture, metrics, seeded bucket).
**Scope:** the first benchmark variant end-to-end. V2 goes first because it is
the *simplest correct* implementation — three serial ops per file, no
concurrency, no readahead machinery — which makes it the reference every later
variant is proven against.

---

## 1. Goal & non-goals

### 1.1 Goal

After this PR merges, a complete measured benchmark run works:

```bash
make rgw-up && make seed TIER=medium
uv run python -m rgw_ingest_bench run --variant v2 --schema scalar --repeat 5
# → results/runs.jsonl rows (gate-verified) + out/v2-…parquet silver files
```

Deliverables:

1. **Variant framework** (`variants/base.py`) — the lifecycle skeleton
   (setup → timed execution → gate → result row) that V0/V1/V3 plug into in
   later PRs with zero changes to the skeleton.
2. **`variants/v2_serial_ranged.py`** — HEAD + GET header + GET footer,
   one file at a time (parent §5 row V2, ~64 KiB/file).
3. **`silver.py`** — the silver schema (both `scalar` and `full` modes) and a
   batched Parquet writer, shared by every variant (parent §5: *"all variants
   must produce identical silver output"*).
4. **`gate.py`** — the §7.3 correctness gate: expected silver rows computed
   *from the manifest + closed-form corpus formulas*, compared against the
   run's output; canonical content hash recorded per run.
5. **`run` CLI command** — repetitions, warmup discard, RTT probe, result
   rows; the skeleton `sweep` (PR 6) will drive.

### 1.2 Non-goals (deferred)

| Deferred item | Lands in |
|---|---|
| V0 (full GET), V1 (`S3File._fetch_range` instrumentation) | PR 4 |
| V3 (async pipeline, bounded queue, manifest mode, dead-letter) | PR 5 |
| `sweep` orchestration, variant-order alternation (§7.2), figures | PR 6 |
| RGW bucket-stats cross-check of byte counters (§7.1) | PR 4, with V1's instrumentation work |

---

## 2. Compatibility contract with PR 1 & PR 2

Consumed **unchanged**:

| Interface | From | Used for |
|---|---|---|
| `layout.HEADER_SIZE/FOOTER_SIZE`, `classify_tail`, `expected_size` | PR 1 | footer arithmetic on HEADed sizes |
| `parse.parse_header`, `parse.parse_footer` (incl. `footer_magic` / `IntegrityError`) | PR 1 | decoding fetched ranges — the same functions PR 1 tested against local files now run against RGW bytes |
| `manifest.read_manifest`, `read_manifest_df`, `ManifestEntry` | PR 1 | key list for the run; expected-row computation in the gate |
| PR 1 §4.2 value formulas (header fields, footer arrays as pure functions of `(seed, file_id)`) | PR 1 | the gate's expected silver frame (§5 below) |
| `S3Config`, `make_fs` | PR 2 | client construction (pool sizing already handled there) |
| `CounterSet`, `LatencyRecorder`, `probe_rtt`, `EnvInfo`, `append_result` | PR 2 | instrumentation + result rows |
| CLI subcommand registry | PR 1/2 | `run` is one new entry |

**Additive changes** (each flagged to reviewers, none breaking):

1. `RunResult` (PR 2 §7.2) gains two optional fields:
   `gate: GateResult | None = None` and `output_path: str | None = None`.
   Existing `seed.jsonl` rows remain valid (fields default to `None`).
2. New runtime dep: **`pyarrow`** (pinned) — first PR that writes Parquet.
   Still no pandas; analysis stays polars.
3. A shared `argparse` parent parser for store flags (`--endpoint`,
   `--bucket`, …) is factored out of `seed` so `run` reuses it — flag
   definitions still exist exactly once.

**Boundary rule enforced by the framework:** V2 receives only the manifest's
*key list*, never its sizes — parent §5 is explicit that V2 keeps the per-file
HEAD to model a client with no manifest. The manifest's sizes and formulas are
used exclusively by the gate, *after* the timed section. The framework makes
this structural (see `VariantInputs` below) rather than a convention.

---

## 3. Variant framework (`variants/base.py`)

The template that makes later PRs "drop in a file, add a registry entry":

```python
class VariantInputs(BaseModel):
    """What a variant is allowed to see during the timed section."""
    keys: list[str]                    # bucket-relative object keys, manifest order
    schema_mode: Literal["scalar", "full"]
    out_path: Path                     # silver parquet destination
    params: dict[str, Any]             # variant-specific knobs (n_inflight, ... later)

class VariantHarness:
    """Owns everything around the timed section.

    Lifecycle (template method `execute()`):
      1. build fs via make_fs(cfg)                     [untimed]
      2. fresh CounterSet + LatencyRecorders           [untimed]
      3. t0 = perf_counter(); variant.process(fs, inputs, metrics); wall = ...
      4. gate.verify_output(...)                       [untimed]
      5. assemble RunResult (wall, counters, latencies, gate, env) → append
    """

class Variant(Protocol):
    name: ClassVar[str]                                # "v2"
    def process(self, fs, inputs: VariantInputs, m: Metrics) -> VariantStats: ...

VARIANT_REGISTRY: dict[str, type[Variant]] = {"v2": V2SerialRanged}
```

- `VariantStats` carries `files_ok`, `files_failed` (dead-letter count —
  always 0 for V2, which aborts on persistent failure; the field exists now
  so §7.2's "invalid if dead-letter > 0" rule is implemented once, in the
  harness, and V3 inherits it).
- Only step 3 is timed (`time.perf_counter`); RTT probe, gate, and result
  serialization never pollute wall time.
- `Metrics` is a small bundle (one `CounterSet`, per-op `LatencyRecorder`s,
  an optional `PeriodicSampler`) constructed fresh per repetition — no state
  leaks between reps.

## 4. V2 — serial ranged GETs (`variants/v2_serial_ranged.py`)

Per-file sequence (parent §5: 3 serial round trips, ~64 KiB):

```
for key in inputs.keys:                                 # one at a time, no overlap
    size   = fs.info(key)["size"]                       # HEAD      — counters: head_count+1
    hdr    = fs.cat_file(key, 0, HEADER_SIZE)           # ranged GET — get_count+1, bytes+32768
    fields = parse_header(hdr)
    kind   = classify_tail(size, fields.img_width, fields.img_height, channels(fields))
    if kind is CORRUPT: raise IntegrityError(key)
    if kind is FOOTER:
        buf    = fs.cat_file(key, size - FOOTER_SIZE, size)   # get_count+1, bytes+32768
        footer = parse_footer(buf)                      # magic + file_id_echo checked
        if footer.file_id_echo != fields.file_id: raise IntegrityError(key)
    writer.write_row(fields, footer_or_none)            # batched SilverWriter (§6)
```

Design points:

- **`fs.cat_file(key, start, end)`** is the fetch primitive: it issues exactly
  one ranged GET with no block-cache/readahead machinery, so the byte counters
  measure precisely the requested ranges. (V1 in PR 4 will deliberately use
  the *other* s3fs path — `fs.open()` + seek/read — to expose the readahead;
  keeping V2 on `cat_file` is what makes the two comparable.)
- **`channels(fields)`** derives `C` from `channel_mask` (`bit_count()`), so
  the variant needs nothing from the spec — everything comes from bytes it
  fetched, exactly like a real no-manifest client.
- **Footerless files get no trailing GET** — with size in hand from the HEAD,
  `classify_tail` already knows the trailing bytes would be pixels (parent
  §2's discard case becomes a *skip* here; the fetch-the-pixels-by-mistake
  path is exercised in tests instead, T6).
- **Per-op latency**: `LatencyRecorder("head")` and `LatencyRecorder("get")`
  record around each call — this seeds the per-GET histogram machinery that
  V3's knee figure depends on.
- **Retries**: ≤3 attempts with exponential backoff on 5xx/timeout (same
  policy constants as `seed`, factored into a small shared helper); still
  failing → abort the repetition with a clear error. Serial V2 has no
  dead-letter lane — a run with failures is invalid anyway (§7.2).
- Expected per-file counters (asserted in tests): `head == 1`,
  `get == 1 + has_footer`, `bytes == 32768 · (1 + has_footer)`.

## 5. Correctness gate (`gate.py`)

Parent §7.3: *"sort the output Parquet by `file_id` and compare a content hash
against the manifest-derived expectation. Every variant must produce
byte-identical silver rows."*

The key enabler is PR 1's determinism model: silver rows are a **closed-form
function of `(seed, file_id, has_footer)`** — so the expectation needs the
manifest and the spec, never the object store:

```python
def expected_silver(manifest: list[ManifestEntry], spec: FakeRawSpec,
                    schema_mode: str) -> pl.DataFrame:
    """Vectorized reconstruction of the exact silver table (PR 1 §4.2 formulas)."""

class GateResult(BaseModel):
    passed: bool
    content_hash: str            # sha256 of canonical bytes (below)
    rows: int
    first_mismatch: str | None   # human-readable: file_id + column, for triage

def verify_output(parquet_path: Path, manifest, spec, schema_mode) -> GateResult
```

- **Canonical form** = read output with polars → sort by `file_id` → select
  columns in schema order with exact dtypes → Arrow IPC stream bytes →
  sha256. Byte-stability is guaranteed *within* this repo's pinned pyarrow
  version, which is the scope that matters: the hash's job is cross-variant
  and cross-run identity on the same pinned environment (versions are in
  every row's `EnvInfo`).
- **The pass/fail decision does not rest on the hash** — `verify_output`
  compares the canonical frame against `expected_silver` with exact equality
  (`.equals()`), which is authoritative and gives `first_mismatch` triage
  info. The hash is recorded for cheap cross-variant comparison ("V2 and V3
  rows identical" = same hash) without recomputing expectations.
- **Float exactness:** expected values are computed with the same
  float32 operations the generator used (PR 1 §10.3), so equality is exact —
  no tolerances, by construction. All expectation-building is vectorized
  polars/numpy (no Python row loops).
- Gate failure ⇒ `RunResult.gate.passed = False` **and** `run` exits nonzero
  after writing the row — parent §7.2: an invalid run is recorded as invalid,
  not discarded silently.

## 6. Silver schema & writer (`silver.py`)

One schema definition shared by all variants — divergence here would break
the "identical silver rows" invariant, so it lives in exactly one module:

| column | dtype | mode | source |
|---|---|---|---|
| `file_id` | int32 | both | header |
| `img_width`, `img_height` | int32 | both | header |
| `pixel_size_x` | float32 | both | header |
| `scan_dir`, `channel_mask` | int32 | both | header |
| `has_footer` | bool | both | tail classification |
| `n_points` | int32, nullable | both | footer (null if absent) |
| `local_offset_x`, `local_offset_y` | list<float32>, nullable | `full` only | footer arrays |

```python
def silver_schema(mode: Literal["scalar", "full"]) -> pa.Schema

class SilverWriter:
    """Batched pyarrow ParquetWriter: zstd, row groups of 1000 (parent §6.4).

    Accumulates columnar buffers; flushes a RecordBatch per 1000 rows and on
    close. Peak memory ≈ one batch even in `full` mode (~33 MiB), so V2 stays
    flat-RSS on the 7 GiB box. Context manager; the file is invalid until
    close() writes the Parquet footer (worth its writeup line, parent §6.3).
    """
```

V3 (PR 5) reuses `SilverWriter` inside its writer task and adds the
queue/flush-timing instrumentation there — the schema and encoding stay
defined here, once.

## 7. `run` CLI command

```bash
uv run python -m rgw_ingest_bench run --variant v2 --schema scalar --repeat 5 \
       [--warmup 1] [--tier medium] [--manifest manifests/medium-seed42.jsonl] \
       [--out-dir out/] [--results results/runs.jsonl] [--endpoint …] [--bucket …] [--json]
```

Flow per invocation:

1. Resolve `S3Config` (shared store-flag parent parser), load manifest,
   reconstruct `FakeRawSpec` from tier/seed flags (needed by the gate).
2. `probe_rtt` once → into `EnvInfo` (parent §4: measured, never nominal).
3. `--warmup` repetitions (default 1) run fully but are marked
   `params["warmup"] = True` in their rows — recorded, excluded by analysis
   (parent §7.2: "one warm-up run discarded").
4. For each measured repetition: fresh `Metrics` → `VariantHarness.execute()`
   → gate → `append_result`. Output files are
   `out/{variant}-{schema}-rep{k}-{run_id}.parquet` — never overwritten,
   `pathlib` throughout.
5. Exit nonzero if any repetition's gate failed or aborted.

After the repetitions, `run` prints a one-line human summary per measured rep
plus a median line — files, wall, files/s, MiB/s, ms/file, gate — so a run
doubles as a throughput read-out. `--json` emits that summary as a single
machine-readable object on stdout (medians over the measured, non-warmup reps)
and suppresses the human lines so stdout stays valid JSON — for scripting and
the §8.2 throughput integration test; per-rep detail always stays in
`results/runs.jsonl`.

Sanity expectation to record in the PR description (parent §5 cost model): at
netem RTT ≈ 2 ms, V2 pays 3 serial RTTs + ~64 KiB per file ⇒ roughly
6–8 ms/file, i.e. ~10 k files in ~60–80 s. Wildly different numbers mean the
harness (not the variant) is broken — that check is the point of landing V2
first.

## 8. Test plan

`pytest` + fixtures; moto for store-backed tests (no Docker in CI), MinIO
marks for fidelity; 100 % line/branch on new code. The e2e fixture chain
reuses PR 2's seed against moto: `tiny_seeded_bucket` (PR 1 `tiny_spec`,
footer_ratio 0.5 → both tail paths present). The plan splits into fast,
moto-backed **unit tests** (§8.1, which carry the 100 % coverage gate),
**integration tests** against a live store (§8.2, where CLI throughput is
verified), and a **run walkthrough** (§8.3). The `minio` / `netem` markers
(registered in `pyproject.toml` by PR 2) select those tiers; `-m "not minio and
not netem"` is the fast gate.

### 8.1 Unit test matrix

| # | Test | Asserts (incl. unhappy paths) |
|---|---|---|
| T1 | `test_v2_end_to_end_moto` | seed → run v2 → gate passes; parquet row count == n_files; hash stable across two identical runs |
| T2 | `test_v2_counters_exact` | head == n; get == n + n_footer; bytes == 32768·(n + n_footer); latency recorder counts match op counts |
| T3 | `test_v2_footerless_skips_get` | footerless file gets exactly 1 GET; `has_footer=False`, `n_points` null in output |
| T4 | `test_gate_expected_silver` | expected frame matches a generated-and-parsed local corpus exactly (both schema modes, incl. float32 exactness on `pixel_size_x`, array contents in `full`) |
| T5 | `test_gate_detects_wrong_output` | perturb one value / drop one row / permute rows in a parquet → `passed=False`, `first_mismatch` names file_id+column; row order alone must NOT fail (canonical sort) |
| T6 | `test_v2_integrity_paths` | corrupt object size (truncate) → `classify_tail` CORRUPT → abort; pixel bytes where footer expected → `footer_magic` IntegrityError; `file_id_echo` mismatch (copy another file's footer) → abort |
| T7 | `test_v2_retry_then_abort` | injected transient 500 (moto/monkeypatch) → retried and succeeds; persistent failure → repetition aborts, RunResult row written with gate=None and nonzero exit |
| T8 | `test_silver_writer` | batch boundaries (n = 999/1000/1001) → correct row groups; close-less writer → unreadable file (asserted); `full` mode arrays roundtrip; zstd + schema equality both modes |
| T9 | `test_harness_lifecycle` | registry lookup; fresh Metrics per rep (no leakage); only step 3 timed (probe/gate excluded — asserted via injected slow gate); dummy test-only variant plugs into the registry, proving the PR 4/5 extension seam |
| T10 | `test_run_cli` | warmup rows flagged; `--repeat 3` → 3 measured rows; unknown variant / missing manifest / bad schema → nonzero exit + usage; output filenames unique across reps |
| T11 | `test_runresult_backcompat` | PR 2 `seed.jsonl` rows (no gate/output_path) still parse under the extended model |
| T12 | `test_run_json_summary` | `run --variant v2 --json` (driven in-process via `cli.main([...])` against moto) prints one summary object `{variant, schema, files, bytes, gib, gate_passed, wall_s_median, files_per_s_median, mib_per_s_median, ms_per_file_median, run_ids}` (`gib = round(bytes/2**30, 3)`); `files == n_files`, `gate_passed` true, rates self-consistent (`files_per_s == files/wall_s`, `mib_per_s == bytes/wall_s/2**20`) — covers the throughput-summary branch for the coverage gate; the figures reconcile with the `results/runs.jsonl` rows |

### 8.2 Integration tests (live object store)

The unit suite runs against **moto** (in-process, no Docker): it proves the
gate, counters, and silver output are *correct*, but a moto "GET" never crosses
a socket, so it can measure neither *throughput* nor real range semantics. These
tests drive the **real CLI** (`sys.executable -m rgw_ingest_bench …` as a
subprocess) end-to-end — `seed` a small corpus, then `run --variant v2` —
against a **live** store (MinIO for CI via `make minio-up`, RGW for headline
numbers via `make rgw-up`), pointed at it with the `BENCH_S3_*` env. Marked
`@pytest.mark.minio`; run in CI's integration job, skipped locally by default.

| # | Test | Asserts |
|---|---|---|
| I1 | `test_run_v2_throughput_cli` | **the CLI throughput check.** Seed a `small`-ish corpus into the live bucket, then `run --variant v2 --schema scalar --repeat 2 --json` as a subprocess; parse the summary `{files, bytes, gib, gate_passed, wall_s_median, files_per_s_median, mib_per_s_median, ms_per_file_median, …}`. Assert `gate_passed` is true and `files == n_files`; `bytes ≈ 32768·(n + n_footer)` (the ~64 KiB/file V2 cost model, §4); `files_per_s_median == files/wall_s_median` and `mib_per_s_median == bytes/wall_s_median/2**20` within 1 % (**reported throughput is accurate**, not merely present); an **opt-in floor** — when `BENCH_MIN_RUN_FILES_PER_S` is set, the measured `files_per_s_median` must clear it; unset ⇒ accounting-only, so CI never flakes on hardware (suggested trusted-loopback value ≈ 20 files/s) — a serialized-stall or accidental full-GET regression trips wherever the floor is set; finally the stdout medians reconcile with the measured (non-warmup) rows in `results/runs.jsonl` — CLI, sink, and store all agree. |
| I2 | `test_run_v2_fidelity_minio` | the former T12: `run --variant v2` against real MinIO range semantics — gate passes, parquet row count == n_files, per-file counters exact (`head == n`, `get == n + n_footer`, `bytes == 32768·(n + n_footer)`), content hash stable across two runs. Catches HEAD/range behavior moto does not model (§10 Q1). |
| I3 | `test_run_v2_latency_band_netem` | mechanizes the §7 cost-model sanity check: with `scripts/netem.sh set <d>` applied, a short `run` reports `ms_per_file_median ≈ 3·(2·d)` + baseline (V2's three serial RTTs) — the "wildly different ⇒ harness broken" guard, automated. Marked `@pytest.mark.netem`, **skipped unless** `BENCH_NETEM=1` and the process can `sudo tc` (root + Linux); otherwise it stays the §9 manual step. |

**Throughput contract (mirrors PR 1 `generate --json` / PR 2 `seed --json`).**
So a test — or an operator — can *verify* run throughput without scraping log
lines, `run` grows a machine-readable summary: `--json` prints exactly one JSON
object to stdout (medians across the measured, non-warmup repetitions) and
suppresses the human lines so stdout stays valid JSON. Per-rep detail always
remains in `results/runs.jsonl`; I1 parses the summary and reconciles it against
those rows. The floor is opt-in via `BENCH_MIN_RUN_FILES_PER_S` (the PR 1 §7.2
`BENCH_MIN_<command>_<rate>` convention — here bounding `run`'s median
`files_per_s`) and a regression tripwire, **not** a benchmark — the real V2
throughput / latency-inflation numbers are the §7 sanity band (recorded in the
PR) and the PR 6 figures.

### 8.3 Running the integration tests (walkthrough)

The unit/moto suite needs only an install; the integration tests need a live
store with a seeded corpus. From the harness root:

```bash
uv sync --extra dev                  # moto, pytest-asyncio, pyarrow, pytest-cov, …
```

**1 — Fast gate (no Docker, no store): unit + moto, with coverage:**

```bash
uv run pytest -m "not minio and not netem" \
       --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100
```

**2 — Bring up a live store and point the client at it:**

```bash
make minio-up                        # or: make rgw-up   (headline numbers)
export BENCH_S3_ENDPOINT=http://localhost:9000
export BENCH_S3_ACCESS_KEY=bench BENCH_S3_SECRET_KEY=bench-secret
export BENCH_S3_KIND=minio           # must match the store you started
```

**3 — Run the integration tests** (they seed their own corpus, then run V2):

```bash
uv run pytest -m minio -v
```

**4 — Just the throughput test (I1), watching the numbers** — `-s` un-captures
stdout so the measured files/s + MiB/s print:

```bash
uv run pytest -m minio -k throughput -s
```

The absolute floor is opt-in: set `BENCH_MIN_RUN_FILES_PER_S` to enforce it on a
store/host you trust (leave it unset ⇒ accounting-only, so CI never flakes on
hardware variance):

```bash
BENCH_MIN_RUN_FILES_PER_S=200 uv run pytest -m minio -k throughput
# Windows PowerShell:  $env:BENCH_MIN_RUN_FILES_PER_S=200; uv run pytest -m minio -k throughput
```

**5 — Verify throughput by hand (exactly what I1 automates)** — seed once, then
run V2 with `--json` and read the rate off stdout:

```bash
make seed TIER=small BUCKET=bronze
uv run python -m rgw_ingest_bench run --variant v2 --schema scalar --repeat 2 \
       --tier small --bucket bronze --json
# {"variant":"v2","schema":"scalar","files":10000,"bytes":622592000,"gib":0.580,"gate_passed":true,
#  "wall_s_median":38.1,"files_per_s_median":262.5,"mib_per_s_median":15.6,"ms_per_file_median":3.81}

uv run python -m rgw_ingest_bench run --variant v2 --repeat 2 --tier small --json | jq .files_per_s_median
```

**6 — (Linux, optional) verify the §7 cost model under netem (I3):**

```bash
make netem-set DELAY=1ms
uv run python -m rgw_ingest_bench run --variant v2 --repeat 1 --tier small --json   # ms_per_file ≈ 6 ms (≈3 serial RTTs)
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
  the gate (the `run --json` branch is covered in-process by T12).
- Throughput here is latency-bound — V2 is serial, so `files_per_s` is really
  `1 / per-file-latency`: without netem it reflects loopback RTT, under netem it
  tracks the §7 band. I1's floor stays deliberately low so this hardware
  dependence never flakes CI.
- Integration tests seed into a unique per-run bucket/prefix and clean up after
  themselves; `compose down -v` is the hard reset if a run is killed.

## 9. Acceptance checklist (PR review gate)

- [ ] On the WSL2 box: `make rgw-up && make seed TIER=medium` then
      `run --variant v2 --repeat 5` → 5 gate-passed rows; wall time within
      the §7 sanity band at measured RTT (numbers pasted into the PR).
- [ ] `scalar` and `full` modes both gate-pass on the `small` tier; RSS flat
      in `full` mode (batched writer verified).
- [ ] Byte counters ≈ 64 KiB/file (footer_ratio-adjusted) — first real
      confirmation of parent H1's V2 prediction.
- [ ] Content hash identical across all 5 repetitions.
- [ ] Fast gate green without Docker (`pytest -m "not minio and not netem"`,
      100 % line/branch on new code); `pytest -m minio` green in CI — including
      I1 `test_run_v2_throughput_cli`, whose `--json` files/s and MiB/s are
      self-consistent and reconcile with `results/runs.jsonl` (verified by hand
      once via `run --variant v2 --tier small --json`, §8.3). No pandas;
      `print` only in CLI output.

## 10. Open questions

1. **`fs.info` vs a raw HEAD** — s3fs `info()` may serve cached results after
   a prior LIST in the same filesystem instance. The harness constructs a
   fresh `S3FileSystem` per repetition (framework step 1) precisely to keep
   HEAD honest; if s3fs listing-cache behavior still interferes (measured
   `head_count` < n), fall back to `fs.call_s3("head_object", ...)`.
   Resolved empirically by T2 against MinIO.
2. **Spec reconstruction for the gate** — currently from `--tier`/`--seed`
   flags; if that proves error-prone (wrong seed passed → confusing gate
   failure), embed the full `FakeRawSpec` JSON into the manifest header line
   in a follow-up (additive manifest change, PR 1 format keeps parsing).
3. **Where `channels()` lives** — `channel_mask.bit_count()` is layout
   knowledge; it lands in `layout.py` beside the size arithmetic (one-line
   addition to a PR 1 module, covered by T4).
