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
python -m rgw_ingest_bench run --variant v2 --schema scalar --repeat 5
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
python -m rgw_ingest_bench run --variant v2 --schema scalar --repeat 5 \
       [--warmup 1] [--tier medium] [--manifest manifests/medium-seed42.jsonl] \
       [--out-dir out/] [--results results/runs.jsonl] [--endpoint …] [--bucket …]
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

Sanity expectation to record in the PR description (parent §5 cost model): at
netem RTT ≈ 2 ms, V2 pays 3 serial RTTs + ~64 KiB per file ⇒ roughly
6–8 ms/file, i.e. ~10 k files in ~60–80 s. Wildly different numbers mean the
harness (not the variant) is broken — that check is the point of landing V2
first.

## 8. Test plan

`pytest` + fixtures; moto for store-backed tests (no Docker in CI), MinIO
marks for fidelity; 100 % line/branch on new code. The e2e fixture chain
reuses PR 2's seed against moto: `tiny_seeded_bucket` (PR 1 `tiny_spec`,
footer_ratio 0.5 → both tail paths present).

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
| T12 | `@pytest.mark.minio` e2e | T1 + T2 against live MinIO (range semantics fidelity vs moto) |

## 9. Acceptance checklist (PR review gate)

- [ ] On the WSL2 box: `make rgw-up && make seed TIER=medium` then
      `run --variant v2 --repeat 5` → 5 gate-passed rows; wall time within
      the §7 sanity band at measured RTT (numbers pasted into the PR).
- [ ] `scalar` and `full` modes both gate-pass on the `small` tier; RSS flat
      in `full` mode (batched writer verified).
- [ ] Byte counters ≈ 64 KiB/file (footer_ratio-adjusted) — first real
      confirmation of parent H1's V2 prediction.
- [ ] Content hash identical across all 5 repetitions.
- [ ] moto suite green without Docker; MinIO marks green in CI; 100 %
      line/branch on new code; no pandas; `print` only in CLI output.

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
