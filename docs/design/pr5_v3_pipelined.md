# PR 5 Implementation Design — V3 (Pipelined Ranged GETs)

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§5 V3 row & manifest mode, §6 the pipelined design, §6.5 pool gotcha, §7 metrics)
**Depends on:** [pr1_corpus_foundation.md](pr1_corpus_foundation.md),
[pr2_environment_seeding_metrics.md](pr2_environment_seeding_metrics.md),
[pr3_v2_serial_ranged_and_gate.md](pr3_v2_serial_ranged_and_gate.md),
[pr4_v1_buffered_instrumentation.md](pr4_v1_buffered_instrumentation.md).
**Scope:** the design under test — N-wide async fetchers → bounded queue →
single Parquet writer — plus its failure handling (retries, dead-letter,
sentinel shutdown), the §6.5 `max_pool_connections` fix with an
achieved-concurrency check, and the backpressure/shutdown test suite. The
biggest PR of the series, kept digestible because *everything that is not
concurrency already exists*: parsing (PR 1), metrics & samplers (PR 2),
harness/gate/writer (PR 3), wire tap (PR 4).

---

## 1. Goal & non-goals

### 1.1 Goal

```bash
python -m rgw_ingest_bench run --variant v3 --n-inflight 128 --queue-bound 1000 \
       --schema scalar --repeat 5
# → gate-passed rows; ~10k medium files in seconds at RTT≈2 ms (parent §5 model)
python -m rgw_ingest_bench run --variant v3 --queue-bound 100 --writer-delay-ms 5
# → the H4 backpressure demo: flat RSS, qsize pinned at bound
```

Deliverables:

1. **`variants/v3_pipelined.py`** — fetcher pool, bounded queue, single
   writer task; manifest mode by default, `--no-manifest` for the HEAD-tax
   run (parent §5).
2. **Failure machinery** — async retries (≤3, backoff + jitter), dead-letter
   JSONL, counted-never-crashing semantics (parent §6.3).
3. **Sentinel shutdown** — drain → fetchers exit → one `None` → writer
   flushes final row group and closes (parquet invalid until close).
4. **§6.5 fix + proof** — `max_pool_connections` sized to N + headroom, and
   an *achieved-concurrency* measurement from PR 4's wire-tap timestamps that
   catches silent capping.
5. **Knob wiring** — `--n-inflight`, `--queue-bound`, `--writer-delay-ms`,
   `--writer-offload`, `--uvloop`; queue-depth/RSS/loop-lag samplers and
   per-flush writer stats populated into the existing result models.
6. **Backpressure & shutdown tests** — the suite parent §12 sketches.

### 1.2 Non-goals (deferred)

| Deferred item | Lands in |
|---|---|
| V4 (sharded writers) — build-on-evidence only (parent §14, resolved 2026-07-16); this PR *produces* the evidence (queue-depth + flush stats under `full` schema) | future, gated |
| `sweep` orchestration (N-axis, queue-axis campaigns), figures | PR 6 |
| netem campaign execution (tooling exists since PR 2) | PR 6 / campaign |
| `full`-schema headline runs — the code path works (T15) but MVP is `scalar` (parent §8.1) | campaign |

---

## 2. Compatibility contract with PR 1–4

Consumed **unchanged**:

| Interface | From | Used for |
|---|---|---|
| `parse_header`, `parse_footer`, `classify_tail`, `verify_pixel_range`, `expected_pixel_bytes` | PR 1 | same parsing, now under concurrency; the §6.3 step-5 guard check finally uses `verify_pixel_range` in production code |
| `CounterSet` (lock-guarded — designed in PR 2 §7.1 *for this PR's* `to_thread` offload), `LatencyRecorder`, `PeriodicSampler`, `EventLoopLagProbe` | PR 2 | queue-depth/RSS/lag sampling slots exist; V3 plugs probes in, zero sampler changes |
| `VariantHarness` lifecycle, registry, dead-letter-invalidates-run rule | PR 3 | the rule was implemented harness-side in PR 3 precisely so V3 inherits it |
| `gate.verify_output` canonical sort | PR 3 | V3 is the first variant with nondeterministic row order — PR 3's T5 ("row order alone must NOT fail") was written for this moment |
| `BotocoreTap` with per-request `t_start`/`t_end` | PR 4 | achieved-concurrency computation (§6 below) — a computation on existing records, no new hooks (the PR 4 reuse seam) |

**Additive changes** (flagged, none breaking):

1. **`VariantInputs.manifest_sizes: dict[str, int] | None = None`** (PR 3
   model). The PR 3 boundary rule — variants see keys only — was V2's
   contract; V3's *defining feature* is manifest mode (parent §5), so sizes
   become an explicit, optional input. `None` for v1/v2 (unchanged) and for
   `--no-manifest` runs; the harness populates it from PR 1's manifest for
   manifest-mode V3 only. What stays out: `has_footer` and the value
   formulas — V3 never sees those; footer presence is decided by arithmetic
   on fetched bytes (§4), and expectations stay gate-only.
2. **`make_fs(cfg, max_pool=..., asynchronous=False)`** (PR 2) gains the
   `asynchronous` flag → returns an `S3FileSystem(asynchronous=True)` for use
   inside the loop (session established per s3fs's async contract at pipeline
   start). One constructor remains the single place pool sizing happens:
   V3 passes `max_pool = n_inflight + 8` headroom (§6.5 fix).
3. **`SilverWriter(on_flush=None)`** (PR 3) — optional callback
   `(rows: int, ms: float) -> None` invoked per row-group flush. PR 3's doc
   reserved this seam ("V3 adds the queue/flush-timing instrumentation");
   implementation is a two-line addition, existing callers unaffected.
4. **Async retry helper** beside PR 3's sync one, sharing the same policy
   constants (attempts/backoff/jitter) so V2, seed, and V3 retry identically.
5. Optional extra **`[uvloop]`** in `pyproject.toml`; imported only under
   `--uvloop` (parent §4/§11 loop-CPU mitigation), never a hard dep.

## 3. Topology & task structure

Parent §6.1/§6.2, made concrete with Python 3.12 primitives:

```
work: asyncio.Queue[WorkItem]      # prefilled: (key, size|None), all 10k — trivial RAM
rows: asyncio.Queue[Row | None]    # maxsize = --queue-bound  ← Knob B (memory)
N fetcher tasks                    # ← Knob A (network), N = --n-inflight
1 writer task                      # SilverWriter inside

async with asyncio.TaskGroup() as tg:
    writer = tg.create_task(writer_task(rows, ...))
    fetchers = [tg.create_task(fetcher_task(work, rows, ...)) for _ in range(N)]
    await asyncio.gather(*fetchers)       # work drained, all fetchers returned
    await rows.put(None)                  # exactly one sentinel
                                          # TaskGroup exit awaits writer
```

- **`TaskGroup`** gives structured failure semantics for free: an unexpected
  exception in *any* task cancels the rest and surfaces — no orphaned
  fetchers, no hung `queue.put` (cancellation unblocks it), no silent
  half-written parquet presented as success. The sentinel handles only the
  *clean* path; TaskGroup handles every dirty one.
- **Work distribution via shared queue**, not static slicing — parent §5's
  V4 note calls out natural load balancing as V3's property; a slow file
  never idles a fetcher.
- The variant's `process()` stays **sync** (PR 3 protocol untouched):
  it calls `asyncio.run(pipeline(...))` internally. The harness, timing,
  gate, and CLI don't know V3 is async.

## 4. Fetcher logic (parent §6.3, per work item)

```
1. size: from manifest_sizes (manifest mode) — or await HEAD (--no-manifest)
2. hdr, tail = await gather(_cat_file(key, 0, 32768),
                            _cat_file(key, size-32768, size))     # PARALLEL
3. fields = parse_header(hdr);  C = channels(fields)
4. tail_kind = classify_tail(size, W, H, C):
     FOOTER    → footer = parse_footer(tail)  [magic + file_id_echo, as V2]
     NO_FOOTER → tail is pixels: verify_pixel_range(tail, fields.file_id,
                 start = W·H·C − 32768) must pass, then DISCARD (parent §2)
     CORRUPT   → integrity error → dead-letter
5. await rows.put(row)          ← the backpressure point (suspends this fetcher)
```

Notes:

- **Both GETs launch before the header is parsed** — possible only because
  size is known (manifest or HEAD), the very optimization parent §2 flags.
  The trailing fetch is unconditional; ~`(1 − footer_ratio)` of files fetch
  32 KiB of pixels and discard them. That cost is *by design* and shows up
  honestly in byte counters (V3 bytes/file ≈ 64 KiB + a bit, still ~17×
  under V1 on `medium`).
- **The guard check (step 4, NO_FOOTER arm)** is the §6.3 step-5 pixel-pattern
  cross-check: a range-math bug (off-by-one start, wrong key) produces bytes
  that fail `verify_pixel_range` or `footer_magic` — caught per-file, routed
  to dead-letter, never silently wrong silver.
- **Retries**: each *file* (not each GET) wrapped by the async retry helper —
  ≤3 attempts, exponential backoff + jitter on 5xx/timeout. Exhausted →
  `DeadLetterRecord(key, error, attempts, ts)` appended to
  `out/…-deadletter.jsonl`, `dead_letter` counter incremented, fetcher moves
  on (parent §6.3: counted, never crashes the run). `VariantStats.files_failed`
  then triggers PR 3's harness rule: run recorded, marked invalid, exit ≠ 0.
- Per-GET latency → `LatencyRecorder("get")` at each await — under
  concurrency this becomes the p99-inflation-past-the-knee evidence (H3).

## 5. Writer task & backpressure

```
while (item := await rows.get()) is not None:
    if writer_delay_ms: await asyncio.sleep(delay)     # --writer-delay-ms (H4 demo)
    writer.write_row(item)                             # PR 3 SilverWriter, on_flush timed
writer.close()                                         # final partial row group + footer
```

- **Single consumer** — the defining serial stage (parent §5 topology). The
  queue bound is Knob B: completed rows parked in RAM ≤ `bound`; when the
  writer stalls, `rows.put` suspends fetchers and effective in-flight drops —
  backpressure *is* the mechanism, no explicit throttling code exists.
- `--writer-offload`: `write_row` batches flushed via `asyncio.to_thread`
  (parent §6.4 optional flag; pyarrow releases the GIL). This is why PR 2
  made `CounterSet` lock-guarded — flush-side counter updates may now happen
  off-loop. Default off; equivalence asserted by T14.
- Writer stats via the `on_flush` seam: rows/flush, ms/flush →
  `counters`/`params` + optional `--dump-samples` JSONL. Under the `full`
  schema these numbers are the **V4 trigger evidence** (parent §14): sustained
  `qsize ≈ bound` + flush-bound wall time ⇒ writer is the measured constraint.
- Samplers (PR 2 `PeriodicSampler`, 100 ms): probes registered for
  `queue_depth` (`rows.qsize`), `rss_mib`, plus `EventLoopLagProbe` — the
  H4 figure's raw data, and the loop-CPU-bound detector (parent §11) at
  high N / RTT≈0.

## 6. The §6.5 pool fix — and proving N was real

Two halves, config and proof:

- **Config:** V3 calls `make_fs(cfg, max_pool = n_inflight + 8,
  asynchronous=True)`. The value is also recorded into `params` and
  introspected from the live aiobotocore client config (T10) — belt and
  suspenders against a future s3fs plumbing change dropping the kwarg.
- **Proof (measured, not configured):** from PR 4's `BotocoreTap` records,
  compute the in-flight profile — sort `t_start`/`t_end` events, +1/−1
  cumulative sum (vectorized numpy) → `inflight_peak`, `inflight_p95`,
  recorded in every V3 row. Two enforcement layers:
  - **Hard failure, always on:** `inflight_peak ≤ 10 < n_inflight` — the
    exact signature of the default-pool cap (parent §6.5's "knee plot is
    fiction" scenario) → run invalid, loud error.
  - **Soft expectation:** `inflight_peak` well below `min(N, n_files)` is
    *recorded* but not fatal — at RTT≈0 the pipeline may legitimately never
    build N concurrent requests (fetches complete faster than tasks spawn),
    and that is a finding (loop-bound regime), not a bug. PR 6's sweep
    asserts monotonic knee behavior instead.

## 7. CLI & recorded parameters

```bash
run --variant v3 [--n-inflight 128] [--queue-bound 1000] [--no-manifest]
    [--writer-delay-ms 0] [--writer-offload] [--uvloop] [--dump-samples]
```

- Flags parse into the existing free-form `params` (no model change):
  `n_inflight, queue_bound, manifest_mode, writer_delay_ms, writer_offload,
  uvloop, max_pool_connections, inflight_peak, inflight_p95,
  flush_ms_mean, flush_rows_mean`.
- `--no-manifest`: harness passes `manifest_sizes=None`; fetchers HEAD per
  file (counter shows `head_count == n_files` vs `0` in manifest mode — the
  HEAD-tax quantification run, parent §5).
- Unknown-for-v3 flags on v1/v2 (e.g. `--n-inflight`) → argparse error, not
  silent ignore: knob flags live on a v3-specific subparser group.
- Expected sanity band for the PR description (parent §5 model): N=128,
  RTT≈2 ms, `medium`, scalar → ~10k files in low single-digit seconds;
  content hash equal to V1/V2's on the same corpus.

**Throughput surfaces through PR 3's `run --json`, unchanged.** V3 adds knob
flags but no new output surface: the medians summary (`files`,
`files_per_s_median`, `mib_per_s_median`, `wall_s_median`, …) is what an operator
reads to see the concurrency speedup, and the §8.2 I1/I2 tests parse the same
object. `bytes ≈` V2's (both ranged, ~64 KiB/file) — V3's story is *files/s*, not
bytes — so the shared `BENCH_MIN_RUN_FILES_PER_S` floor (PR 1 §7.2
`BENCH_MIN_<command>_<rate>` convention) is exactly the right axis, just set
higher than V2 since V3 is faster. `inflight_peak` / `inflight_p95` in `params`
(§6) let a run also report the achieved concurrency behind that speed.

## 8. Test plan

pytest + `pytest-asyncio`; moto **server** (PR 2 dep) as the async-capable
store — aiobotocore speaks real HTTP to it; MinIO marks for fidelity. Every
async test wrapped in `asyncio.wait_for` (no CI hangs). 100 % line/branch on
new code. The named suites parent §12 asks for — backpressure, sentinel
shutdown, fetch-byte counters — are T3/T4/T2. The plan splits into fast,
moto-server **unit tests** (§8.1, which carry the 100 % coverage gate),
**integration tests** against a live store (§8.2, where V3's concurrency
throughput is verified through the CLI), and a **run walkthrough** (§8.3).
`run --variant v3` reuses PR 3's `run --json` summary and the shared
`BENCH_MIN_RUN_FILES_PER_S` floor unchanged — V3 is a registry entry plus knob
flags, not a new command — and the `minio` / `netem` markers registered by PR 2;
the fast gate is `-m "not minio and not netem"`.

### 8.1 Unit test matrix

| # | Test | Asserts (incl. unhappy paths) |
|---|---|---|
| T1 | `test_v3_end_to_end` | seed → v3 → gate passes; hash equals V2's (and V1's) on same corpus — the invariant across all three MVP variants; rows arrived unordered (assert input order ≠ parquet physical order on ≥1 run) yet gate passes |
| T2 | `test_v3_counters` | manifest mode: `head_count == 0`, `get_count == 2n`, bytes == 65 536·n; `--no-manifest`: `head_count == n`; footerless files' discarded 32 KiB counted honestly |
| T3 | `test_backpressure` | `queue_bound=5`, `writer_delay_ms=20`, 200 files: sampled `queue_depth ≤ 5` at all times; RSS flat (no growth ∝ files); completion ≈ writer-limited time; fetchers actually suspended (in-flight drops observed via tap) |
| T4 | `test_sentinel_shutdown` | exactly one sentinel; final partial row group written; parquet valid & row-complete after close; a pre-close read attempt fails (invalid-until-close, parent §6.3) |
| T5 | `test_dead_letter` | one key persistently 500s: run completes, n−1 rows written, dead-letter JSONL has the key + attempts=3, `files_failed=1`, harness marks run invalid, exit ≠ 0 (PR 3 rule firing) |
| T6 | `test_retry_transient` | key fails twice then succeeds → row present, no dead-letter, retry counter == 2, backoff delays observed (fake clock) |
| T7 | `test_guard_paths` | monkeypatched fetch returning shifted tail → `verify_pixel_range`/`footer_magic` failure → dead-letter (not crash, not wrong silver); truncated-size manifest entry → CORRUPT → dead-letter |
| T8 | `test_failure_propagation` | writer raising mid-run → TaskGroup cancels fetchers, no hang, error surfaces, partial output not gate-passed; fetcher cancelled while blocked on full queue → unblocks cleanly |
| T9 | `test_parallel_header_footer` | tap timestamps: for each file the two GETs overlap in time (moto-server latency shim) — the §2 parallelism actually happens |
| T10 | `test_pool_fix` | live client config shows `max_pool_connections == n_inflight + 8`; simulated cap (pool forced to 10, N=64) → `inflight_peak ≤ 10` detected → run invalid with the §6.5 error |
| T11 | `test_inflight_computation` | synthetic tap records with known overlap → exact peak/p95; empty records → 0, no crash |
| T12 | `test_samplers_populated` | queue-depth/RSS/lag series present with plausible cadence; `on_flush` stats recorded; `--dump-samples` files written |
| T13 | `test_determinism_across_n` | same corpus, `n_inflight ∈ {1, 8, 64}` → identical content hash (concurrency must not change silver) |
| T14 | `test_writer_offload_equivalence` | `--writer-offload` on/off → identical hash; counters consistent (lock-guarded CounterSet exercised from the flush thread) |
| T15 | `test_full_schema_path` | `full` mode small run: arrays intact through the pipeline, gate passes — the code path the V4-trigger campaign will use |
### 8.2 Integration tests (live object store)

The unit suite runs against **moto server** (async, real HTTP in-process): it
proves the pipeline is *correct* — gate, counters, backpressure, shutdown — but
moto is loopback-in-a-process, so it can show neither real *throughput* nor the
latency-hiding that is V3's entire reason to exist. These tests drive the **real
CLI** (`sys.executable -m rgw_ingest_bench …` as a subprocess) against a **live**
store (MinIO for CI via `make minio-up`, RGW for headline numbers via
`make rgw-up`), pointed at it with the `BENCH_S3_*` env. Marked
`@pytest.mark.minio` (I2 is `@pytest.mark.netem`); run in CI's integration job,
skipped locally by default. They reuse PR 3's `run --json` summary and the shared
`BENCH_MIN_RUN_FILES_PER_S` floor unchanged — `run --variant v3` is a registry
entry plus knob flags, not a new command.

| # | Test | Asserts |
|---|---|---|
| I1 | `test_run_v3_throughput_cli` | **the CLI throughput check.** Seed a corpus, then `run --variant v3 --n-inflight 32 --schema scalar --repeat 2 --json` as a subprocess; parse PR 3's summary `{files, bytes, gate_passed, wall_s_median, files_per_s_median, mib_per_s_median, …}`. Assert `gate_passed` and `files == n_files`; `files_per_s_median == files/wall_s_median` and `mib_per_s_median == bytes/wall_s_median/2**20` within 1 % (**reported throughput is accurate**); `bytes ≈ 65 536·n` (V3 is ranged like V2, not V1's whole-file readahead); the matching `results/runs.jsonl` row has `params.inflight_peak > 1` (concurrency actually happened on a real socket — the §6 proof); reuse the **opt-in** `BENCH_MIN_RUN_FILES_PER_S` floor (unset ⇒ accounting-only, so CI never flakes; set *higher* than V2, since V3 is the fast one); the stdout medians reconcile with those rows. |
| I2 | `test_v3_vs_v2_speedup_netem` | **the H2/H3 headline, CLI-verified.** Under `scripts/netem.sh set <d>` (so there is latency to hide), `run --variant v3 --n-inflight 64` and `run --variant v2` over the same seeded bucket; assert **identical `content_hash`** (V3 == V2 == V1 silver) *and* `files_per_s_median(v3) ≥ 3× files_per_s_median(v2)` — the concurrency speedup, asserted as a conservative multiple so it can't flake but a collapsed pipeline trips. Marked `@pytest.mark.netem`, **skipped unless** `BENCH_NETEM=1` and the process can `sudo tc` (root + Linux); on loopback (RTT≈0) the speedup legitimately vanishes (§6 loop-bound regime), which is *why* it is netem-gated, not run bare. |
| I3 | `test_v3_backpressure_cli` | **H4 via the CLI.** `run --variant v3 --queue-bound 100 --writer-delay-ms 5 --dump-samples --json` on a few hundred files; from the dumped sampler JSONL assert `queue_depth ≤ queue_bound` at every sample and `rss_mib` flat (no growth ∝ files) — bounded memory under a deliberately throttled writer, read straight from a real run. The §1.1 backpressure demo, mechanized. |
| I4 | `test_v3_fidelity_minio` | the former T16 against **real HTTP under concurrency**: cross-variant hash == V1/V2 (T1); manifest-mode counters `head_count == 0`, `get_count == 2n`, `bytes == 65 536·n` (T2); backpressure holds (T3) — on genuinely concurrent ranged GETs a real server answered, not moto's approximation. Also confirms the §6.5 pool sizing on a live client (`max_pool == n_inflight + 8`). |

**Throughput contract — inherited, not re-declared.** V3 adds knob flags
(`--n-inflight`, `--queue-bound`, …) but no new *output* surface: `run --json`
(PR 3 §7) already emits the medians summary I1/I2 parse, and V3's speed shows up
in the same `files_per_s_median` an operator reads. The floor stays the shared
`BENCH_MIN_RUN_FILES_PER_S` (PR 1 §7.2 `BENCH_MIN_<command>_<rate>` convention) —
files/s is exactly V3's axis (unlike V1, where it was a weak proxy for a
bytes-moved story). V3's *distinctive* verification is the N-scaling speedup (I2)
and achieved concurrency (`inflight_peak`, I1); the real knee-vs-N curve is
PR 6's sweep, not a single floor here.

### 8.3 Running the integration tests (walkthrough)

The unit suite needs only an install (`moto[server]` comes with the dev extra);
the integration tests need a live store, and I2 additionally needs `tc`. From
the harness root:

```bash
pip install -e ".[dev]"              # moto[server], pytest-asyncio, pyarrow, pytest-cov, …
```

**1 — Fast gate (no Docker, no store): async unit + moto-server, with coverage:**

```bash
pytest -m "not minio and not netem" \
       --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100
```

**2 — Bring up a live store and point the client at it:**

```bash
make minio-up                        # or: make rgw-up   (headline numbers + rgw-stats)
export BENCH_S3_ENDPOINT=http://localhost:9000
export BENCH_S3_ACCESS_KEY=bench BENCH_S3_SECRET_KEY=bench-secret
export BENCH_S3_KIND=minio           # must match the store you started
```

**3 — Run the V3 integration tests** (I1 throughput, I3 backpressure, I4
fidelity; the netem speedup I2 is step 6):

```bash
pytest -m minio -v
```

**4 — Just the throughput test (I1), watching the numbers** — `-s` un-captures
stdout so the measured files/s + MiB/s print (`inflight_peak` lands in the
`results/runs.jsonl` row):

```bash
pytest -m minio -k throughput -s
```

The floor is opt-in and shared across variants (same `run` command); for V3 set
it *higher* than V2 — concurrency makes V3 the fast one (leave it unset ⇒
accounting-only, so CI never flakes):

```bash
BENCH_MIN_RUN_FILES_PER_S=500 pytest -m minio -k throughput
# Windows PowerShell:  $env:BENCH_MIN_RUN_FILES_PER_S=500; pytest -m minio -k throughput
```

**5 — Verify the V3-vs-V2 speed by hand (what I1 + I2 automate)** — seed once,
run both under netem to see the real gap, read files/s off stdout:

```bash
make seed TIER=medium BUCKET=bronze
make netem-set DELAY=1ms
python -m rgw_ingest_bench run --variant v3 --n-inflight 64 --repeat 2 --tier medium --bucket bronze --json \
       | jq '{files_per_s_median, mib_per_s_median}'
python -m rgw_ingest_bench run --variant v2 --repeat 2 --tier medium --bucket bronze --json \
       | jq '{files_per_s_median}'
make netem-clear
# v3.files_per_s ≫ v2.files_per_s at RTT≈2 ms, identical content_hash — H2, by hand.
# (achieved concurrency is params.inflight_peak in results/runs.jsonl)
```

**6 — The netem speedup test (I2), the H2/H3 headline** — needs root for `tc`,
and the live store from step 2 still up:

```bash
BENCH_NETEM=1 pytest -m netem -k speedup
```

**7 — See the H4 backpressure shape (I3) by hand:**

```bash
python -m rgw_ingest_bench run --variant v3 --queue-bound 100 --writer-delay-ms 5 \
       --tier small --dump-samples --json
# then inspect the *-samples.jsonl: queue_depth pinned ≤ 100, rss_mib flat
```

**8 — Tear down:**

```bash
make netem-clear ; make minio-down   # or make rgw-down  (compose down -v: full reset)
```

Notes:

- No new coverage surface from the CLI: `run --json` is PR 3 code (its branch is
  covered in-process by PR 3's T12). PR 5's new lines — `v3_pipelined.py`, the
  fetcher/writer tasks, the inflight computation — are covered by the async
  moto-server suite (§8.1); the live-store tests add none and run outside the
  `--cov-fail-under=100` gate.
- moto server (async, real HTTP in-process) is enough for *correctness* — gate,
  counters, backpressure, shutdown (T1–T15); the live store is only needed for
  real *throughput* (I1), the latency-hidden speedup (I2, netem), and real
  concurrent Range fidelity (I4).
- V3 rows carry `inflight_peak`/`inflight_p95`; if `inflight_peak ≤ 10 <
  n_inflight` the run is invalid (§6.5 default-pool cap) — that hard check runs
  in the integration job too, so a silently capped pool fails loudly, not
  quietly.

## 9. Acceptance checklist (PR review gate)

- [ ] WSL2 box, RGW, `medium`, scalar: `--n-inflight 128` run gate-passes in
      the parent-§5 sanity band; hash identical to V1/V2 runs (numbers +
      hashes pasted into the PR).
- [ ] Backpressure demo run (`--queue-bound 100 --writer-delay-ms 5`):
      sampled qsize pinned at bound, RSS flat — H4's shape visible in raw
      JSONL before PR 6 ever plots it.
- [ ] `--no-manifest` run recorded: HEAD tax quantified (Δ wall time + Δ
      request count in the PR description).
- [ ] `inflight_peak ≈ 128` at netem RTT≈2 ms; the forced-cap test (T10)
      demonstrated locally.
- [ ] `rgw-stats` byte delta ≈ client counters for one V3 run (third-opinion
      check, PR 4 tooling).
- [ ] Fast gate green without Docker (`pytest -m "not minio and not netem"`,
      100 % line/branch on new code, `moto[server]` backend); `pytest -m minio`
      green in CI — including I1 `test_run_v3_throughput_cli` (v3 `run --json`
      throughput self-consistent, `inflight_peak > 1`, reconciles with
      `results/runs.jsonl`) and I4 fidelity; the netem speedup I2 (H2) run once
      under `BENCH_NETEM=1`.
- [ ] PR 1–4 suites untouched and green; uvloop remains optional (CI runs
      without it).

## 10. Open questions

1. **s3fs async session lifecycle** — pinned-version behavior for
   `asynchronous=True` setup/teardown inside `asyncio.run` (explicit
   `set_session` vs lazy). Resolved during implementation against the pin;
   whatever it is, it lives inside `make_fs`/pipeline start, not in fetcher
   code. Canary-style assertion added if the contract proves fragile
   (PR 4 precedent).
2. **moto-server latency shim for T9** — inject per-request delay via a moto
   middleware/hook or a thin local proxy; needed only to make overlap
   observable in tests. If moto resists, T9 falls back to the MinIO mark
   with `tc`-free client-side delay injection (patched `_cat_file` sleep).
3. **Dead-letter + retry interplay under cancellation** — a file mid-retry
   when TaskGroup cancels: current design lets cancellation win (no
   dead-letter entry, run already failing anyway). Revisit only if T8 shows
   confusing double-reporting.
4. **`work` queue prefill vs streaming** — prefilled 10k tuples is ~1 MiB and
   simplest; if a future `full`-corpus campaign needs streaming intake, the
   seam is one function. Not built now.
