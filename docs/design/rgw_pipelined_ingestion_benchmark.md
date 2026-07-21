# Benchmark Harness Design — Pipelined Metadata Ingestion over Ceph RGW

**Status:** Draft
**Date created:** 2026-07-15
**Related:** [gap_project_plan.md](../gap_project_plan.md) (buffer-period artifact — see §1.2), companion doc `local_mmap_metadata_benchmark.md` (local-disk tier; not yet written)
**Quick start (when executing):** jump to [§8.1 MVP cut](#81-mvp-cut-execute-this-first-2-weekends)
— **V1/V2/V3 only, `medium` tier, `scalar` schema, ~2 weekends**, in the Oct–Nov 2026
buffer *after* the local-mmap companion; skip entirely if lead-wave interviews have
already converted. Everything else (V0, V4, `large`/`full`, the rest of the matrix)
is stretch, gated on the MVP surfacing something worth chasing.

---

## 1. Purpose & Scope

### 1.1 The question this benchmark answers

Bronze-layer `.raw` inspection images are large (pixels dominate), but bronze→silver
ingestion extracts only ~64 KiB of fixed-offset metadata (header + footer) per file.
When bronze lives behind **Ceph RGW (S3 API)**, `mmap` does not apply — there is no
kernel-visible file. The tier-native sparse-read primitive is the **HTTP ranged GET**,
and the tier-native acceleration is **concurrency** (overlapping ms-scale round trips),
not zero-copy.

This harness measures, with controlled variables and honest baselines:

1. **Bytes moved** — ranged GETs vs the buffered `read → seek → read` idiom
   (whose default block readahead silently fetches ~5 MiB per file) vs full-object GET.
2. **The concurrency knee** — throughput vs in-flight window `N`, and the latency
   inflation past the knee (Little's law with non-constant latency).
3. **Pipeline behavior** — bounded-queue backpressure: memory stays flat when the
   writer stalls, and completion time degrades gracefully to writer speed.

### 1.2 Where this sits in the gap plan

This is a **buffer-period (Oct–Nov 2026) artifact**, not a new phase. It must not
displace Phase 3/4 work. It hardens the resume claims around mmap/read-pattern
storage design by adding the *object-storage tier* of the story, and feeds the
writeup backlog with a measured piece: *"skip the pixels" is the invariant; mmap vs
`pread` vs range-GET are per-tier mechanisms, each with a readahead trap.*

### 1.3 Out of scope

- Local-disk `mmap` vs `pread` vs `fread` (companion benchmark, separate doc).
- Multi-writer table formats (Iceberg/Delta) — visibility/commit protocols are
  discussed in the writeup, not benchmarked here.
- Ceph/RGW server-side tuning. The cluster is a fixed test fixture.
- Pixel-plane ingestion (the full `parse()` path). This benchmark is metadata-only.

---

## 2. Background — file anatomy and access pattern

Layout mirrored from the production parser
([raw_file_parser.py](../../DatasetReader/src/asml/hmi/DatasetReader/parsers/raw_file_parser.py)):

```
offset 0            32768                  32768 + W·H·C            EOF
├─ header (32 KiB) ─┼─ pixel planes (W·H bytes × C channels) ─┼─ footer (32 KiB, OPTIONAL) ─┤
   fixed offsets        the bulk — MBs                            fixed offsets
```

- Header fields live at fixed byte offsets; the farthest used field
  (`CHANNEL_MASK`) ends at byte 17724. We fetch the full 32 KiB anyway
  (same round trip, negligible bytes, simpler offset math).
- Footer position = `32768 + W·H·C`, i.e. computable only after parsing the header —
  **unless** object size is known, in which case the footer is the last 32 KiB and
  header + footer fetches can go **in parallel**.
- Footer is optional. Presence test: `size == 32768 + W·H·C + 32768`.
  If `size == 32768 + W·H·C`, the trailing range would return pixels — discard.

> **Clean-room note (for the public repo).** The benchmark conclusions depend only on
> the *shape* (fixed-offset binary header / large opaque middle / fixed-size optional
> trailing footer), not on the proprietary field map. The public generator defines its
> own synthetic field schema with the same shape and sizes. Do not publish the
> production offset table.

---

## 3. Synthetic corpus — `fakeraw` generator

### 3.1 File content

Each fake file is generated from a Pydantic spec and is **self-describing for
verification**:

- **Header (32 KiB):** representative fields at fixed offsets — at minimum
  `img_width:int32@0`, `img_height:int32@4`, `file_id:int32@20`,
  `pixel_size_x:float32@48`, `scan_dir:int32@92`, `channel_mask:int32@17720`.
  `file_id` encodes the corpus index so every variant's parse can be asserted.
- **Pixels (`W·H·C` bytes):** a deterministic pattern `byte[i] = (file_id·31 + i)
  mod 256` (not random, not zeros), so each 1-byte sample spans the full 8-bit
  range 0–255, emulating real image pixel values. It lets integrity checks
  detect most off-by-one range math (e.g., a "footer" fetch that actually
  returned pixel bytes). **Limitation:** because the pattern repeats every 256
  bytes, a misread shifted by an *exact multiple of 256* is not detectable from
  the pixel bytes alone; that class of error is instead caught by the footer's
  `footer_magic` / `file_id_echo` tags and, authoritatively, by the §7.3
  correctness gate. (An earlier design used a prime period of 251 to catch even
  256-aligned shifts, traded away here for full-range pixel values.)
- **Footer (32 KiB, present with probability `footer_ratio`, default 0.9):**
  a few scalars + two `float32[4070]` arrays (`local_offset_x/y`), mirroring the
  production footer's dominant payload.

### 3.2 Config model

```python
class FakeRawSpec(BaseModel):
    """Generation spec for one corpus tier (NumPy-style docstrings in code)."""
    n_files: int
    img_width: int
    img_height: int
    n_channels: int
    footer_ratio: float = 0.9
    seed: int = 42
```

### 3.3 Corpus tiers & disk budget

| Tier | Geometry | Pixel bytes/file | Files | Corpus size | Purpose |
|---|---|---|---|---|---|
| `small` | 256×256×1 | 64 KiB | 10,000 | ~1.6 GiB | readahead ≈ whole file regime |
| `medium` (default) | 1024×1024×1 | 1 MiB | 10,000 | ~10.6 GiB | main sweeps |
| `large` | 4096×4096×2 | 32 MiB | 500 | ~16 GiB | exposes the 5 MiB readahead cap & full-GET pain |

Rationale: V1's readahead fetch is `min(file_size, 32 KiB + block_size)` — on small
files it degenerates to "fetch the whole object," so **only the `large` tier cleanly
separates V1 from V0**. The pixel-size sweep is therefore a first-class axis, not a
nice-to-have.

Seeding: `bench seed` generates locally (streamed, never all in RAM) and uploads via
s3fs with concurrency; deterministic from `seed`; writes a **manifest JSONL**
(`path, size, has_footer, file_id`) used both for verification and for V3's
manifest mode (§5).

---

## 4. Test environment

| Component | Choice | Notes |
|---|---|---|
| Object store | Ceph RGW via container (e.g. `quay.io/ceph/demo` single-node) | matches the planned production stack; pin an exact image digest; tune `osd_memory_target` for the 7 GiB WSL2 box (§11, §14) |
| CI fallback | MinIO | S3-compatible; fine for correctness tests, *not* for headline numbers |
| Client | Python 3.12+, `s3fs`/`aiobotocore`, `pyarrow`, `polars`, `pydantic`, `uvloop` (optional flag) | versions **pinned** and recorded into every result row |
| Network realism | `tc netem` injected delay | see below |

**Latency injection (required, not optional).** On loopback, RGW round trips are
sub-ms and the benchmark would measure the wrong regime (event-loop CPU instead of
RTT hiding). Inject delay on the relevant interface (`lo` or the container veth):
`delay d` per direction ⇒ RTT ≈ 2d. Sweep RTT ∈ {~0 (loopback), ~2 ms, ~10 ms}.
**Measure actual RTT** (TCP connect/echo probe) at run start and record it in the
result row — never trust the nominal netem value.

**Cache regime.** After seeding, RGW/OSD caches are warm; results measure protocol +
concurrency behavior, not disk. State this in the writeup. (Optionally add one
cold-cache run — restart the Ceph container — as an appendix data point.)

---

## 5. Variants under test

All variants must produce **identical silver output** (§7.3). Symbols used
throughout this doc: `r` = RTT, `B` = bandwidth, `S` = file size,
`RA = 32 KiB + block_size (≈5 MiB)` (s3fs readahead fetch); `F` = total fetch
(network) time for the batch, `E` = total encode (writer) time, single-threaded;
`K` = number of writer shards (V4).

| ID | Name | Per-file ops | Bytes/file | Model of |
|---|---|---|---|---|
| **V0** | Naive full GET | 1 GET (whole object), parse in memory | `S` | "download then parse" |
| **V1** | Buffered seek/read (**current `parse_metadata()` idiom pointed at RGW**) | HEAD + GET (readahead) + GET (footer), serialized | `min(S, RA) + 32 KiB` | the unmodified production code path |
| **V2** | Ranged GETs, serial | HEAD + GET header + GET footer, one file at a time | ~64 KiB | the "bytes fix" without concurrency |
| **V3** | **Pipelined ranged GETs** (design under test) | LIST-amortized manifest + 2 parallel GETs/file, `N` in flight → bounded queue → single Parquet writer | ~64 KiB | the proposed ingestion design |
| V4 *(optional)* | Sharded writers | as V3, but K batches → K part-files, no shared writer | ~64 KiB | "unit of parallelism = unit of output" |

Notes:

- **V1 must be instrumented, not assumed** — wrap/monkeypatch `S3File._fetch_range`
  to count the ranges s3fs *actually* fetches. This is the measurement that exposes
  the readahead trap; defaults are version-dependent, so measure, don't quote docs.
- **V2 keeps the per-file HEAD** deliberately (models a client with no manifest);
  **V3 uses manifest mode** — sizes come from the seeding manifest or a bucket LIST
  (~10 LIST calls per 10k objects), eliminating 10k HEADs and enabling parallel
  header+footer fetches. Run V3 once with `--no-manifest` to quantify the HEAD tax.
  End-to-end topology — the single shared writer is V3's defining feature (and its
  one serial stage; contrast the sharded V4 below):

  ```
  10,000 paths ──► 128 fetchers ──► one queue ──► ONE writer ──► one .parquet file
                                                  (shared by everyone)
  ```
- V4 exists to show the writer was never the bottleneck at this scale (or to fix it
  if the payload-heavy schema makes it one, §6.4). Topology — split the path list
  into K disjoint shards, each a complete, coordination-free mini-pipeline (no two
  writers ever touch the same file):

  ```
  paths[0:2500]     ──► 32 fetchers ──► queue₀ ──► writer₀ ──► part-00000.parquet
  paths[2500:5000]  ──► 32 fetchers ──► queue₁ ──► writer₁ ──► part-00001.parquet
  paths[5000:7500]  ──► 32 fetchers ──► queue₂ ──► writer₂ ──► part-00002.parquet
  paths[7500:10000] ──► 32 fetchers ──► queue₃ ──► writer₃ ──► part-00003.parquet
  ```

  Same total fetch concurrency as V3 (4 × 32 = 128) — only the *writer* stage gains
  capacity, so V4 ≈ `max(F, E/K)` vs V3 ≈ `max(F, E)` (symbols: §5 legend above).
  Speedup exists only in the
  writer-bound regime (`E > F`), and the K writers must actually parallelize
  (separate processes, or GIL-releasing thread offload) — K writer tasks on one
  event loop still encode serially. If built, prefer sharding only the writer stage
  (one shared fetch pool round-robining rows to K queues): static path splits lose
  V3's natural load balancing and finish at the slowest shard.
- **V4 caveat — single-file output contracts.** V4's natural output is K part-files
  (engines read `*.parquet` globs as one table; interrogate any "must be one file"
  requirement before honoring it). If one file is genuinely required, a final merge
  is needed and its economics decide V4's fate: a *synchronous re-encode* merge costs
  ≈ the full serial encode and refunds V4's entire win (total ≈ `F + E`, strictly
  worse than V3's `max(F, E)`); a *raw row-group concatenation* (byte copy + footer
  rewrite) keeps the win when encode ≫ fetch, but preserves part row-group sizes;
  an *asynchronous compaction* takes the merge off the ingestion critical path
  entirely (the lakehouse norm). Out of benchmark scope — design note only.

### Expected per-file cost model (to verify, not to assert)

| | ops on critical path | bytes | e.g. `large` tier, r=5 ms, B≈1 Gbps |
|---|---|---|---|
| V0 | 1 GET | 32 MiB | ~5 ms + 269 ms ≈ **274 ms** |
| V1 | 3 serial RTTs | ~5.3 MiB | ~15 ms + 45 ms ≈ **60 ms** |
| V2 | 3 serial RTTs | ~64 KiB | ≈ **15.5 ms** |
| V3 | ~1 RTT (2 GETs in parallel, HEAD amortized), N-wide | ~64 KiB | throughput ≈ `N / (r + ε)` → **10k files in ~1–2 s** at N=128 |

---

## 6. The pipelined design (V3)

### 6.1 Topology

```
64–256 async ranged GETs ──► bounded queue ──► single writer task
   (5–10 ms each, the           (backpressure)     buffer rows → flush row group
    expensive stage)                               every ~1k rows
```

### 6.2 The two knobs are in different places

```
paths ──► [ N fetcher coroutines ] ──► queue (maxsize=1000) ──► writer
               ▲                            ▲
        Knob A: N = 64–256           Knob B: bound = 1000
        requests in flight           completed rows parked in RAM
        ── a NETWORK limit ──        ── a MEMORY limit ──
```

Knob A is a ceiling on concurrent HTTP requests, sized to the measured knee of the
throughput curve. Knob B caps completed rows buffered in RAM
(≈ `bound × row_size`). They are independent; backpressure arbitrates at runtime
(writer slow → queue fills → `put()` suspends fetchers → effective in-flight drops).

### 6.3 Fetcher logic (per file)

1. Look up `size` in the manifest (no HEAD).
2. `asyncio.gather`: GET `[0, 32768)` and GET `[size − 32768, size)` in parallel.
3. Parse header (`np.frombuffer` at fixed offsets).
4. Footer-presence check: `size − 32768 − W·H·C == 32768` → trailing bytes are the
   footer; `== 0` → they are pixels, discard; anything else → integrity error.
5. Cross-check the pixel-pattern guard bytes (corpus is self-describing, §3.1) —
   best-effort: catches non-256-aligned shifts and wrong-object reads; footer
   tags and the §7.3 gate are the authoritative integrity checks.
6. `await queue.put(row)` ← the backpressure point.

**Retries:** ≤3 attempts on 5xx/timeout, exponential backoff + jitter. A file that
still fails goes to a dead-letter JSONL (path + error) and is *counted*, never
crashes the run. Expected failure count in a controlled run: 0.

**Shutdown:** work list drained → all fetchers exit → one `None` sentinel → writer
flushes the final partial row group and closes the `ParquetWriter` (footer written
once; the file is invalid until close — worth one line in the writeup).

### 6.4 Writer & silver schema

- `pyarrow.parquet.ParquetWriter`, schema fixed up front, zstd, row group ≈ 1,000 rows.
- **Two schema modes** (a real silver-design question, and a writer-load axis):
  - `full`: scalars + `list<float32>[4070]` × 2 → ~33 KiB/row → 10k rows ≈ 330 MiB.
  - `scalar`: scalars only (~200 B/row) — models "big arrays don't belong in silver."
- Record per-flush durations. If the `full` schema makes the writer the bottleneck
  (queue pinned at bound), that is a *finding*, and V4 is the demonstrated fix.
  Optional flag: offload flush via `asyncio.to_thread` (pyarrow releases the GIL).

### 6.5 Implementation gotcha that invalidates the whole sweep if missed

`botocore`'s default connection pool is **10**. With `max_pool_connections = 10`,
every `N > 10` is silently capped and the knee plot is fiction. The harness must set
`config_kwargs={"max_pool_connections": max(N) + headroom}` and assert (via a probe
or connection metrics) that concurrency actually reached `N`.

---

## 7. Metrics & instrumentation

### 7.1 Captured per run (Pydantic model → JSONL; analysis in polars)

| Metric | How |
|---|---|
| Wall time, files/s | monotonic clock around the run |
| **Bytes fetched (actual)** | counters at fetch call sites; for V1, a wrap of `S3File._fetch_range`; cross-check against RGW bucket stats |
| Request counts | LIST / HEAD / GET tallies |
| Per-GET latency histogram (p50/p95/p99) | recorded at each await; this is the latency-inflation-past-the-knee evidence |
| Queue depth over time | 100 ms sampler task (`qsize()`) |
| Peak RSS | `resource.getrusage` + periodic sampler |
| Writer stats | rows/s, per-flush ms, output bytes |
| Event-loop lag | periodic `loop.time()` drift probe (detects CPU-bound loop at high N / low RTT) |
| Environment | RTT (measured), netem setting, versions (s3fs/aiobotocore/pyarrow/python/kernel), corpus tier, git SHA |

### 7.2 Statistical hygiene

≥5 repetitions per cell, alternate variant order, report **median + IQR**; one warm-up
run discarded; a run is invalid if dead-letter count > 0 or any integrity check fails.

### 7.3 Correctness gate (benchmark validity)

After each run, sort the output Parquet by `file_id` and compare a content hash
against the manifest-derived expectation. **Every variant must produce byte-identical
silver rows.** A fast wrong pipeline is not a result.

---

## 8. Experiment matrix

| Axis | Values | Primary question |
|---|---|---|
| Variant | V0, V1, V2, V3 (V4 optional) | headline comparison |
| In-flight `N` (V3) | 1, 8, 32, 64, 128, 256, 512 | the knee; latency inflation |
| Queue bound (V3) | 1, 100, **1000**, 10000, unbounded | prove it's not the throughput knob |
| Writer throttle (V3) | none / `--writer-delay-ms 5` | force backpressure regime for the RSS/queue-depth plot |
| RTT (netem) | ~0, ~2 ms, ~10 ms | regime realism; where does concurrency stop mattering |
| Corpus tier | small / medium / large | readahead-cap behavior; bytes-vs-pixel-size flatness |
| Schema | scalar / full | writer as (non-)bottleneck |

Not a full cross-product — anchor cell is (medium, RTT≈2 ms, scalar), then vary one
axis at a time from the anchor. Estimated total runtime: a few hours, scriptable
overnight.

### 8.1 MVP cut (execute this first; ~2 weekends)

Interview signal concentrates in three figures, so the minimum viable campaign is:
**V1/V2/V3 only, `medium` tier, `scalar` schema, loopback + one netem point (~2 ms),
N sweep, one queue-bound demo run with `--writer-delay-ms`** → figures (a) bytes,
(b) knee, (c) backpressure. V0, `large` tier, `full` schema, V4, and the remaining
matrix are stretch — add only if the MVP surfaces something worth chasing.
Sequencing per §1.2 and [gap_project_plan.md](../gap_project_plan.md): buffer period
(Oct–Nov 2026), *after* the local-mmap companion (cheaper; hardens the existing
resume bullet; database-internals fit), and skipped entirely if lead-wave interviews
have already converted — this artifact is an amplifier, not a prerequisite.

## 9. Hypotheses (numbers to confirm or falsify)

- **H1 (bytes):** V2/V3 ≈ 64 KiB/file regardless of tier; V1 ≈ `min(S, 5.3 MiB)`;
  V0 ≈ `S`. On `large`, V1 moves ~85× more than V3; on `small`, V1 ≈ V0 (readahead
  swallows the file) — the trap's magnitude is size-dependent.
- **H2 (serial ranged):** V2 beats V1 by roughly the transfer-time delta
  (~3× on `large` at 1 Gbps), not by round trips (both pay 3 serial RTTs).
- **H3 (knee):** V3 throughput rises ~linearly with N, then plateaus; past the knee
  p99 GET latency inflates ∝ N with flat throughput. Knee position shifts with RTT
  (higher RTT → knee at higher N).
- **H4 (knobs are independent):** with a keeping-up writer, queue bound ∈
  [100, 10000] changes throughput < a few %; with `--writer-delay-ms`, unbounded
  queue RSS grows toward corpus-row size while bounded stays flat at
  `bound × row_size`, completion time identical (writer-limited either way).
- **H5 (flatness):** V3 wall time is ~independent of pixel size; V0/V1 scale with it.

## 10. Deliverables & writeup mapping

1. Harness repo (layout below) with one-command `make bench` / CLI.
2. `results/*.jsonl` + a polars notebook/script emitting four figures:
   **(a)** bytes-per-file by variant × tier (log scale) — H1/H2;
   **(b)** throughput & p99 vs N — H3 (the knee figure);
   **(c)** RSS & queue depth vs time, bounded vs unbounded under writer throttle — H4;
   **(d)** wall time vs pixel size by variant — H5.
3. Writeup (backlog Tier-1 candidate): *problem → alternatives (V0/V1/V2) →
  design (V3) → measured result*, closing with the cross-tier invariant
  (mmap ↔ range-GET, `MADV_RANDOM` ↔ readahead/block-size) once the companion
  local benchmark exists.

## 11. Risks & pitfalls

| Risk | Mitigation |
|---|---|
| Loopback regime lies about concurrency value | netem sweep is mandatory; report per-RTT |
| Connection pool silently caps N | §6.5; assert achieved concurrency |
| s3fs/aiobotocore defaults drift across versions | pin + record versions; measure actual fetched ranges |
| Event loop CPU-bound at high N / RTT≈0 | loop-lag metric; uvloop flag; report as a finding, not noise |
| Corpus too big for laptop | tiered sizes (§3.3), `large` capped at 500 files |
| Warm-cache flattery | state the regime; optional cold-start appendix run |
| Fixture RAM vs the 7 GiB WSL2 cap (probe 2026-07-15: 7 GiB total, ~3 free; a stock OSD defaults `osd_memory_target` to 4 GiB) | tune `osd_memory_target` ≈ 1 GiB on the toy OSD; raise the WSL2 cap in `.wslconfig` (e.g. `memory=12GB`); seeding stays streamed |
| Writer flush blocks the loop | measure flush ms; `to_thread` flag |
| Fake-vs-real drift | shape/sizes mirrored from the production parser; clean-room field map (§2) |

## 12. Repo layout & conventions

```
rgw-ingest-bench/
  pyproject.toml            # pinned deps
  Makefile                  # seed / bench / sweep / report / rgw-up / rgw-down
  docker-compose.yml        # Ceph demo (RGW) + optional MinIO profile
  src/rgw_ingest_bench/
    layout.py               # offset schema shared by generator & parsers
    fakeraw.py              # corpus generator (streamed)
    manifest.py             # seed manifest read/write
    parse.py                # header/footer np.frombuffer parsing
    metrics.py              # counters, histograms, samplers, result models
    variants/v0_full_get.py … v3_pipelined.py
    cli.py                  # seed | run | sweep | report
  tests/                    # pytest: generator↔parser roundtrip, footer arithmetic,
                            # range math vs pattern guard, sentinel shutdown,
                            # backpressure (tiny bound + writer delay ⇒ qsize ≤ bound),
                            # fetch-byte counters; MinIO/moto integration marks
  results/                  # JSONL + figures (committed)
```

Per project guidelines: `pathlib` everywhere, Pydantic for all configs/results,
NumPy-style docstrings, `logging` with f-strings (no `print` outside CLI output),
`pytest` + fixtures with full coverage on new code, `polars`/`pyarrow` for analysis
(no pandas).

## 13. CLI sketch

```bash
make rgw-up
python -m rgw_ingest_bench seed  --tier medium --bucket bronze --seed 42
python -m rgw_ingest_bench run   --variant v3 --n-inflight 128 --queue-bound 1000 \
                                 --schema scalar --repeat 5
python -m rgw_ingest_bench sweep --anchor "tier=medium,rtt=2ms,schema=scalar" \
                                 --axis n-inflight=1,8,32,64,128,256,512
python -m rgw_ingest_bench report results/*.jsonl --figures out/
```

## 14. Open questions

- Ceph demo container vs `microceph` — **resolved 2026-07-15 (smoke test pending):
  containerized RGW primary, microceph fallback.** Box probe: this WSL2 distro runs
  systemd (snap viable) and Docker Desktop is installed but WSL integration is
  disabled for the distro (one settings toggle to enable). Deciding criteria — *not*
  seeding speed, which is a one-time cost: (a) disposability + exact pinning
  (image digest, `compose down -v` reset, CI-portable) favor the container;
  (b) netem scoping — delaying the container veth slows only benchmark traffic,
  whereas microceph on the host needs netem on `lo`, delaying every local service;
  (c) snap bootstrap friction on WSL2. microceph's advantage (current Ceph releases)
  buys little here: the benchmark needs correct S3 range/LIST semantics and honest
  RGW per-op overhead, which an older RGW still provides. Fallback order:
  demo container → microceph → MinIO (correctness only, never headline numbers).
- Does RGW per-op overhead on loopback (~ms auth/index cost) already provide enough
  "latency" to see the knee without netem? Measure first; keep netem regardless for
  the 10 ms cloud point.
- Whether to fold V4 (sharded writers) into the first run — **resolved 2026-07-16:
  defer; build V4 only on evidence.** Plain-language framing: **V4 exists for
  *further* acceleration on top of V3, and is needed only IF the writer proves too
  slow to keep up with the fetchers** (the writer-bound regime `E > F` — total
  encode time exceeds total fetch time; symbols in the §5 legend, topology and
  formulas in the §5 V4 note). Run V0–V3 first. Trigger for building V4:
  under the `full` schema at the anchor cell, sustained queue depth ≈ bound
  (backpressure permanently engaged) and/or `full` wall time materially above
  `scalar` wall time with identical fetch metrics — i.e. the writer is the
  *measured* binding constraint. V4's success criterion is then restoring wall
  time to fetch-bound levels (shard K writers until fetch binds again). If the
  writer never binds, drop V4 and record the quantified negative in the writeup
  ("single writer suffices at this scale; headroom = X"). The H4 backpressure
  demonstration does not depend on V4 — `--writer-delay-ms` covers it.
  Rationale: measure-first discipline; the writeup arc is stronger either way
  (before/after fix with numbers, or a quantified "not needed"); V4 is cheap to
  add later since fetcher code is unchanged.
