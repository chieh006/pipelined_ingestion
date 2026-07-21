# PR 6 Implementation Design — `sweep` Orchestration & `report` Figures

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§7.2 statistical hygiene, §8 experiment matrix & MVP cut, §9 hypotheses, §10 deliverables)
**Depends on:** PR 1–5 — this PR *runs* and *reads* them; it adds no runtime
pipeline code.
**Scope:** the campaign layer: `sweep` (anchor-cell + one-axis-at-a-time
orchestration, repetition/ordering hygiene, resume) and `report` (polars
ingestion → median/IQR stats → the four parent-§10 figures + a hypothesis
scorecard). Kept out of PR 5 deliberately: analysis code changes on writeup
cadence, runtime code on benchmark cadence — different reviewers, different
risk, different diffs.

---

## 1. Goal & non-goals

### 1.1 Goal

```bash
uv run python -m rgw_ingest_bench sweep --preset mvp            # §8.1 campaign, resumable
uv run python -m rgw_ingest_bench sweep --anchor "tier=medium,rtt=2ms,schema=scalar" \
                                        --axis n-inflight=1,8,32,64,128,256,512
uv run python -m rgw_ingest_bench report results/*.jsonl --figures out/figures/ --scorecard
```

Deliverables:

1. **`sweep.py` + `sweep` CLI** — expands an anchor cell + one axis into a
   run list; executes each repetition as a fresh subprocess of the PR 3 `run`
   command; enforces §7.2 hygiene (≥5 reps, warmup, alternating variant
   order); resumable; RTT-guarded (netem is verified, never trusted).
2. **`report.py` + `report` CLI** — reads `results/*.jsonl` with polars,
   normalizes across PR 2–5 row vintages, computes median + IQR per cell,
   renders the four §10 figures, and emits a **hypothesis scorecard**:
   H1–H5 each evaluated to a number and a verdict, ready to paste into the
   writeup.
3. **`--preset mvp`** — the §8.1 minimum viable campaign as one command
   (V1/V2/V3 bytes cells, V3 N-sweep, one backpressure demo cell with
   sampler dumps), plus a `make sweep-mvp` target.

### 1.2 Non-goals

| Out | Why / where |
|---|---|
| Automating netem changes | needs root; sweep *verifies* RTT per cell instead (§3.4) — the human moves the knob, the tool refuses to mismeasure |
| `large`/`full`/V0/V4 campaign cells | stretch beyond §8.1; the sweep grammar already expresses them, no code change needed later |
| Statistical tests beyond median + IQR | §7.2 fixes the estimator; fancier stats belong in the writeup if ever |
| Notebook deliverables | script-generated figures only, deterministic from JSONL (parent §10 allows either; scripts are reviewable and CI-checkable) |

---

## 2. Compatibility contract with PR 1–5

This PR is a **pure consumer** of earlier interfaces:

| Interface | From | Used for |
|---|---|---|
| `run` CLI (variants, `--repeat`, `--warmup`, knob flags, exit codes) | PR 3/5 | sweep's unit of execution — subprocess per cell (§3.2) |
| `RunResult` JSONL rows: `gate`, `counters`, `params`, `env.rtt`, warmup flag | PR 2–5 | everything report reads |
| `--dump-samples` series files | PR 2/5 | figure (c) time series |
| `probe_rtt` | PR 2 | the per-cell RTT guard |
| `TIER_SPECS` | PR 1 | pixel-size axis values for figure (d) |
| Makefile / netem helper | PR 2 | operator instructions sweep prints between RTT cells |

**Additive changes** (the only ones, both flagged):

1. **`run` gains `--tag key=value` (repeatable)** — stamped verbatim into
   `params`. Sweep uses it for `sweep_id` and `cell_id` so report can group
   rows by cell without reverse-engineering flag combinations. Trivial,
   additive, useful for manual runs too.
2. **The key-name contract becomes normative.** Report reads counters/params
   by name; §5.1's table freezes those names (`bytes_fetched`,
   `bytes_fetched_audit`, `n_inflight`, `inflight_peak`, `flush_ms_mean`, …)
   as the cross-PR schema. Any future rename is a breaking change to *this*
   doc's table — the table is the contract, and T-tests in this PR pin it.

New dependency: **`matplotlib`** (pinned) under an optional extra
`[report]` — benchmark-box installs stay lean; CI and the analysis
environment install it. polars stays the only dataframe layer (no pandas,
per project rules; matplotlib is fed numpy arrays extracted from polars).

## 3. `sweep` design

### 3.1 Plan model

```python
class SweepAxis(BaseModel):
    name: str                       # "n-inflight" | "queue-bound" | "variant" | "rtt" | ...
    values: list[str]

class SweepPlan(BaseModel):
    """One campaign: anchor + ONE varied axis (parent §8: not a cross-product)."""
    sweep_id: str                   # short uuid
    anchor: dict[str, str]          # {"tier": "medium", "rtt": "2ms", "schema": "scalar"}
    axis: SweepAxis
    repeat: int = 5                 # §7.2 floor; sweep refuses < 5 without --allow-few
    warmup: int = 1

    def cells(self) -> list[Cell]   # Cell = anchor ⊕ one axis value, stable cell_id
```

Presets are just stored `SweepPlan` constructors; `mvp` expands to three
plans run in sequence (variant-comparison cells at the anchor, the
`n-inflight` axis for V3, and one `--writer-delay-ms 5` demo cell run with
`--dump-samples` at two queue bounds — figures a/b/c's exact inputs, §8.1).

### 3.2 Execution: subprocess per repetition

Each repetition = `sys.executable -m rgw_ingest_bench run …` with the cell's
flags + `--repeat 1` + `--tag sweep_id=… --tag cell_id=…`, appending to the
shared results JSONL.

- **Why subprocess, not in-process:** a fresh interpreter per repetition
  guarantees no event-loop, connection-pool, s3fs-cache, or allocator state
  crosses repetitions — §7.2's independence assumption made structural. It
  also means sweep exercises the real CLI surface (flags, exit codes), so a
  campaign is documentation-by-execution of the commands a reader can run
  by hand. `sys.executable` + list-argv (no shell) keeps it cross-platform.
- Nonzero exit from `run` (gate failure, dead-letter) → the cell is marked
  failed in the sweep log; sweep continues to the next cell (one bad cell
  must not kill an overnight campaign, parent §8: "scriptable overnight")
  and exits nonzero at the end with a failure summary.

### 3.3 §7.2 hygiene, mechanized

- **Repetition interleaving:** repetitions iterate *outer* over rounds and
  *inner* over cells (round-robin) rather than completing each cell's 5 reps
  consecutively — for the `variant` axis this is exactly "alternate variant
  order"; for numeric axes it spreads any slow environmental drift (cache
  warmth, RGW background work) across all cells instead of biasing one.
- **Warmup:** first round is flagged warmup (PR 3's `--warmup` machinery);
  report excludes it.
- **Validity:** rows with `gate.passed == False` or `files_failed > 0` are
  excluded by report and *counted* in the sweep summary (§7.2: invalid runs
  invalidate, never silently vanish).

### 3.4 The RTT guard (netem is manual; mismeasurement is impossible)

The `rtt` anchor value is a *claim*. Before each cell, sweep calls
`probe_rtt` and compares the measured median against the claim with a
tolerance band (±30 % + 0.3 ms floor). Mismatch → the cell is **refused**
with instructions (`make netem-set DELAY=1ms`), and `--pause-between-rtt`
makes sweep stop and prompt at RTT-boundary cells for interactive campaigns.
The measured value (not the claim) is what lands in `EnvInfo` — parent §4's
"never trust the nominal netem value", enforced at orchestration time.

### 3.5 Resume

`--resume`: before executing a repetition, sweep scans the results JSONL for
a valid row with the same `cell_id` and round index (tags make this a lookup,
not a heuristic). Present → skipped. An interrupted overnight campaign
restarts with one flag and redoes only missing/invalid work.

## 4. `report` design

### 4.1 Ingestion & normalization

```python
def load_results(paths: list[Path]) -> pl.DataFrame
```

- `pl.read_ndjson` per file → concat → unnest `counters`/`params`/`env`
  into flat columns (vectorized; missing keys become nulls so PR 2-vintage
  seed rows and PR 5 rows coexist).
- Derived columns, defined once here:
  - `bytes_per_file = coalesce(bytes_fetched_audit, bytes_fetched) / files`
    (V1 reports via the PR 4 audit; V2/V3 via plain counters — coalesce is
    the normalization),
  - `files_per_s` (already present, sanity-recomputed),
  - `p99_get_ms` extracted from the `latencies` list (op == "get").
- Filters: drop warmup, drop invalid (§3.3), warn (not fail) on cells with
  < 5 valid reps — the number of reps backing every point is printed in the
  scorecard.

### 4.2 Statistics

Per cell (`group_by(cell_id)` — or by reconstructed keys for pre-sweep
manual rows): **median + IQR** for wall time, files/s, bytes/file, p99 —
exactly §7.2's estimator, computed via polars `quantile` expressions
(vectorized, no Python loops).

### 4.3 The four figures (parent §10)

Rendered with matplotlib (Agg backend — headless CI), one function per
figure, each taking a polars frame and an output dir, emitting PNG + SVG:

| Fig | Content | Data source | MVP? |
|---|---|---|---|
| (a) | bytes/file, bar per variant grouped by tier, **log y**, annotated ×-ratio vs V3 | run rows (H1/H2) | yes (medium-only bars) |
| (b) | **the knee**: files/s (median line + IQR band) and p99 GET ms on twin axis vs `n_inflight` (log₂ x) | V3 N-axis rows (H3) | yes |
| (c) | RSS MiB & queue depth vs time, bounded vs large-bound run under `--writer-delay-ms` | `--dump-samples` series of the two demo cells (H4) | yes |
| (d) | wall time vs pixel bytes/file (log x) per variant | tier-axis rows (H5) | stretch — renders automatically once `small`/`large` cells exist |

Figure hygiene: every figure stamps corpus tier, measured RTT (median of the
cells' `env.rtt`), schema mode, and git SHA into the footer — a figure
separated from its JSONL remains attributable (versions are the §11
drift defense, so they travel with the picture). Missing inputs (e.g. no
sampler dumps) → that figure is skipped with a logged warning, others still
render; `report` exits nonzero only if *zero* figures could be produced.

### 4.4 Hypothesis scorecard (`--scorecard`)

A generated markdown table — the writeup's evidence section, computed not
transcribed:

| Hyp | Check (computed from rows) | Output |
|---|---|---|
| H1 | bytes/file: V1 ÷ V3 ratio per tier; V2 ≈ V3 ≈ 64 KiB band | ratios + PASS/FAIL/UNTESTED |
| H2 | wall: V2 vs V1 delta vs transfer-time prediction | delta + verdict |
| H3 | knee N* (first N whose files/s gain < 10 %); p99 slope past N* | N*, slope, verdict |
| H4 | throughput spread across queue-bound cells (< few %); RSS flat vs growing in demo runs | numbers + verdict |
| H5 | V3 wall flatness across tiers vs V0/V1 scaling | slope ratio or UNTESTED |

`UNTESTED` (cells absent — e.g. H5 before the tier axis runs) is a first-class
verdict: the scorecard states what the data can and cannot yet say, matching
the parent's confirm-or-falsify framing (§9).

## 5. Contracts & CLI summary

### 5.1 Key-name contract (normative table, pinned by tests)

```
counters: bytes_fetched, bytes_fetched_audit, bytes_fetched_wire,
          get_count, head_count, dead_letter
params:   n_inflight, queue_bound, manifest_mode, writer_delay_ms,
          s3fs_block_size, inflight_peak, flush_ms_mean,
          sweep_id, cell_id, warmup, tier, schema
```

(Existing names from PR 2–5, frozen here; report and sweep tests import one
shared constants module so a rename breaks loudly in CI.)

### 5.2 CLI

```bash
sweep  (--preset mvp | --anchor K=V,... --axis name=v1,v2,...)
       [--repeat 5] [--warmup 1] [--resume] [--results results/runs.jsonl]
       [--pause-between-rtt] [--allow-few] [--dry-run]     # dry-run prints the run list + ETA
report results/*.jsonl [--figures out/figures/] [--scorecard] [--samples-dir out/]
```

`--dry-run` prints every subprocess argv and a runtime estimate (cells ×
reps × sanity-band wall time) — parent §8's "few hours, scriptable
overnight" made checkable before committing an evening to it.

## 6. Test plan

pytest + fixtures. The **unit** suite (§6.1) needs no store anywhere — sweep
tests fake the subprocess boundary, report tests run on synthetic JSONL,
matplotlib under Agg — and it carries the 100 % line/branch coverage gate. The
**integration** suite (§6.2) runs the real `sweep → run → report` chain against
a live store, where campaign throughput is verified end-to-end through the CLI;
§6.3 is the run walkthrough. `sweep` executes PR 3/5 `run` subprocesses, so it
inherits `run --json`, the §5.1 row schema, the `minio` / `netem` markers
(PR 2), and the `BENCH_MIN_RUN_FILES_PER_S` floor (PR 3) unchanged — no new
throughput surface, no new env var. The fast gate is
`-m "not minio and not netem"`.

### 6.1 Unit test matrix

| # | Test | Asserts (incl. unhappy paths) |
|---|---|---|
| T1 | `test_plan_expansion` | anchor ⊕ axis → correct cells, stable `cell_id`s; two axes at once → refused (one-axis rule); `repeat=3` without `--allow-few` → refused |
| T2 | `test_round_robin_order` | execution order interleaves cells across rounds; variant axis alternates order (§7.2) — asserted on the recorded argv sequence |
| T3 | `test_subprocess_invocation` | monkeypatched runner captures argv: correct flags per cell, `--tag` stamps present, list-argv (no shell), `sys.executable` used |
| T4 | `test_cell_failure_continues` | runner returns nonzero for one cell → sweep completes others, summary names the failed cell, overall exit nonzero |
| T5 | `test_rtt_guard` | probe within band → proceeds; outside → cell refused with netem instructions; `--pause-between-rtt` prompts at boundaries (monkeypatched stdin) |
| T6 | `test_resume` | pre-seeded results JSONL → only missing (cell, round) pairs executed; invalid rows do NOT count as done |
| T7 | `test_load_results_mixed_vintages` | PR 2 seed rows + PR 3/5 run rows in one file → unnest succeeds, nulls where absent, warmup/invalid filtered, `bytes_per_file` coalesce correct (audit for v1, counter for v2/v3) |
| T8 | `test_stats_median_iqr` | crafted cells vs numpy reference; < 5 reps → warning recorded, row kept |
| T9 | `test_figures_render` | synthetic frames → all four figures produce nonempty PNG+SVG; footer stamp contains tier/RTT/SHA; missing sampler dumps → (c) skipped with warning, exit 0; zero figures possible → exit ≠ 0 |
| T10 | `test_scorecard` | crafted data where H1/H3/H4 pass, H2 fails, H5 untested → exact verdicts; knee N* detection on a synthetic plateau curve |
| T11 | `test_key_contract` | shared constants module matches the names PR 2–5 actually emit (fixture rows generated via the real `RunResult` model) — the §5.1 table enforced |
| T12 | `test_dry_run` | prints full run list + ETA, executes nothing (runner never called) |

### 6.2 Integration tests (the real sweep → run → report chain)

The unit tests fake the subprocess boundary (T3) and feed `report` synthetic
JSONL (T7–T10) — fast, hermetic, no store. What they cannot prove is that the
campaign layer is faithful *end to end*: that `sweep` really drives the PR 3/5
`run` CLI against a live store, that the throughput `report` prints is the same
number those runs measured, and that the §4.3 figures render from real rows.
These tests drive the **real CLI** — `sweep` and `report` as subprocesses
(`sys.executable -m rgw_ingest_bench …`) — against a **live** store (MinIO for CI
via `make minio-up`, RGW for headline numbers via `make rgw-up`), pointed at it
with the `BENCH_S3_*` env. Marked `@pytest.mark.minio` (I2 is
`@pytest.mark.netem`); run in CI's integration job, skipped locally by default.

Compatibility: `sweep` adds no throughput surface of its own — each cell is a PR
3/5 `run` subprocess, so `run --json`, the `RunResult` JSONL schema (§5.1), and
the `BENCH_MIN_RUN_FILES_PER_S` floor all apply unchanged (subprocesses inherit
the env var, so a floor set once guards every cell). `report` only *reads* rows.

| # | Test | Asserts |
|---|---|---|
| I1 | `test_sweep_report_throughput_cli` | **the campaign throughput capstone.** Seed a small corpus; `sweep --anchor tier=small,schema=scalar --axis variant=v2,v3 --repeat 5` against the live store (real per-round `run --repeat 1` subprocesses); then `report <results> --figures <dir> --scorecard`. Assert the chain exits 0 and report's per-cell **median files/s equals `numpy.median` of that cell's JSONL rows' `files_per_s`** (report is a faithful aggregator — the throughput it prints is exactly the rows' own accounting, the same `files_per_s` a `run --json` surfaces); each row's `files_per_s == files/wall_s`; figure (a) bytes/file + scorecard H1 render from the real rows (V2 ≈ V3 ≈ 64 KiB band); warmup rows excluded, per-cell valid-rep count printed. Throughput verified through the full CLI, not a fixture. |
| I2 | `test_sweep_report_knee_netem` | **the knee (figure b / H3), CLI-verified.** Under `scripts/netem.sh set <d>` (so concurrency has latency to hide), `sweep --anchor tier=small,rtt=<d>,schema=scalar --axis n-inflight=1,4,16,64` for v3 → `report`. Assert files/s rises then plateaus with N (a detectable knee `N* > 1`), figure (b) renders non-empty PNG+SVG, and the scorecard reports H3 with `N*` and the p99-past-knee slope. Marked `@pytest.mark.netem`, **skipped unless** `BENCH_NETEM=1` and the process can `sudo tc`; on loopback the curve is legitimately flat (§6/PR 5 loop-bound regime) and the scorecard says so — so the test asserts *a verdict is produced* always, and the knee shape only when netem is active. |
| I3 | `test_sweep_resume_cli` | **overnight robustness, end to end.** Start a multi-cell `sweep` against the live store, interrupt it after ≥1 cell completes (detected via the results JSONL, not a timer), then rerun with `--resume`: assert only the missing (cell, round) pairs execute (completed rows are not duplicated), the RTT guard re-verifies each resumed cell, and a final `report` over the union matches an uninterrupted run's stats. The §3.5 resume claim proven against real subprocesses and real rows, not T6's pre-seeded fixture. |

**No new throughput contract.** PR 6 verifies throughput by *reconciliation*, not
a new floor: `report`'s numbers must equal what the underlying rows (and the
`run --json` summaries that printed them, PR 3 §7) already reported, so the
campaign cannot silently disagree with the per-run figure an operator checks by
hand. The `BENCH_MIN_RUN_FILES_PER_S` floor (PR 1 §7.2 `BENCH_MIN_<command>_<rate>`
convention) still applies to every `run` a sweep spawns — set it in the
environment and the whole campaign is floored. The real knee-vs-N curve is I2 +
figure (b); a single floor was never PR 6's job.

### 6.3 Running the integration tests (walkthrough)

The unit suite (§6.1) needs only an install — no store, no Docker. The
integration tests run the real campaign chain, so they need a live store (and I2
needs `tc`). From the harness root:

```bash
uv sync --extra dev --extra report   # adds matplotlib (Agg) for the report figures
```

**1 — Fast gate (no Docker, no store): unit tests + coverage:**

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

**3 — Run the campaign integration tests** (I1 capstone, I3 resume; the netem
knee I2 is step 6):

```bash
uv run pytest -m minio -v
```

**4 — Verify campaign throughput by hand (what I1 automates)** — run a tiny
sweep, then report it; the scorecard's files/s per cell is exactly the median of
that cell's run rows (report re-derives nothing an operator can't check):

```bash
make seed TIER=small BUCKET=bronze
uv run python -m rgw_ingest_bench sweep --anchor tier=small,schema=scalar \
       --axis variant=v2,v3 --repeat 5 --results results/demo.jsonl
uv run python -m rgw_ingest_bench report results/demo.jsonl --figures out/figures/ --scorecard
# each row's files_per_s == files / wall_s (the same value `run --json` prints);
# report's per-cell figure == the median of those rows.
```

**5 — Floor the whole campaign (optional)** — the per-run floor is inherited by
every `sweep` subprocess via the environment (leave it unset ⇒ accounting-only,
so CI never flakes):

```bash
BENCH_MIN_RUN_FILES_PER_S=200 uv run python -m rgw_ingest_bench sweep --preset mvp --dry-run
# --dry-run first to see the plan + ETA; every real cell's `run` then enforces the floor
```

**6 — The netem knee (I2), figure (b)'s headline** — needs root for `tc`, store
from step 2 still up:

```bash
BENCH_NETEM=1 uv run pytest -m netem -k knee
# …or by hand:
make netem-set DELAY=1ms
uv run python -m rgw_ingest_bench sweep --anchor tier=small,rtt=1ms,schema=scalar \
       --axis n-inflight=1,4,16,64 --repeat 5 --results results/knee.jsonl
uv run python -m rgw_ingest_bench report results/knee.jsonl --figures out/figures/ --scorecard
make netem-clear
# figure (b): files/s rises then plateaus; scorecard H3 reports N*
```

**7 — Tear down:**

```bash
make netem-clear ; make minio-down   # or make rgw-down  (compose down -v: full reset)
```

Notes:

- No new coverage surface: `sweep`/`report` line coverage is carried by the §6.1
  unit tests (fake runner, synthetic JSONL); the live-store tests exercise the
  real chain and run outside the `--cov-fail-under=100` gate. `run --json`
  itself is PR 3 code (covered by PR 3's T12).
- The integration sweep uses a *small* tier and few cells so a live pass is
  minutes, not the overnight `--preset mvp` campaign; the §7 acceptance
  checklist covers the full preset once on the WSL2 box.
- `report` needs the `[report]` extra (matplotlib); it is optional at runtime so
  benchmark-box installs stay lean (§2). The integration job installs it; a bare
  install skips figure rendering with a logged warning, not a crash.

## 7. Acceptance checklist (PR review gate)

- [ ] `sweep --preset mvp --dry-run` prints the §8.1 campaign (correct cells,
      sane ETA); the real preset then runs end-to-end on the WSL2 box across
      one evening, resumable after a mid-campaign Ctrl-C.
- [ ] `report … --figures --scorecard` on the campaign output: figures (a)
      (b) (c) render and are legible; scorecard verdicts consistent with
      eyeballing the figures; H5 correctly `UNTESTED`.
- [ ] Figure (b) shows a knee (or the scorecard says why not — e.g. RTT≈0
      loop-bound regime); either way the pipeline of evidence works.
- [ ] `results/` layout (JSONL + figures) committed as parent §12 specifies.
- [ ] Fast gate green without Docker (`pytest -m "not minio and not netem"`,
      100 % line/branch on new code); `pytest -m minio` green in CI — including
      I1 `test_sweep_report_throughput_cli` (report's per-cell files/s reconciles
      with the underlying `run` rows' `files_per_s`) and I3 resume; the netem
      knee I2 run once under `BENCH_NETEM=1`.
- [ ] `[report]` extra optional (runtime install works without matplotlib); no
      pandas anywhere.

## 8. Open questions

1. **Knee detection rule** — "first N with < 10 % marginal gain" is simple
   and monotone-robust; if real curves are noisy at low RTT, switch to a
   piecewise-linear fit. Decide from the first real sweep; the scorecard
   marks H3 `INCONCLUSIVE` rather than guessing.
2. **Figure (c) input pairing** — the two demo cells (bounded vs large-bound)
   are matched by `cell_id` convention from the preset; a `--pair` flag for
   manually-run demos may be worth adding if figure (c) gets regenerated
   outside sweeps.
3. **Committing figures vs regenerating** — parent §12 commits `results/`;
   PNGs are binary diffs. Default: commit final campaign figures only
   (`out/figures/` gitignored, `results/figures/` committed on purpose).
   Revisit if the repo gets noisy.
4. **RTT-axis ergonomics** — if pause-and-prompt proves annoying, a
   `--netem-sudo` opt-in that shells `make netem-set` between cells could
   automate it; deliberately not built until the manual flow has been lived
   with once.
