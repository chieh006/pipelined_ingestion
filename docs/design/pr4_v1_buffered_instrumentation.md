# PR 4 Implementation Design — V1 (Buffered seek/read) & Fetch Instrumentation

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§5 V1 definition & "instrumented, not assumed" note, §7.1 bytes-fetched row, §11 version-drift risk)
**Depends on:** [pr1_corpus_foundation.md](pr1_corpus_foundation.md) (parsers),
[pr2_environment_seeding_metrics.md](pr2_environment_seeding_metrics.md) (config/metrics),
[pr3_v2_serial_ranged_and_gate.md](pr3_v2_serial_ranged_and_gate.md) (harness, gate, silver writer).
**Scope:** the V1 variant — the *unmodified production `parse_metadata()`
idiom pointed at RGW* — plus the instrumentation that makes its hidden
readahead measurable. The variant itself is ~40 lines; the real content of
this PR is the measurement machinery and its version-drift defenses, which is
why it gets an isolated diff.

---

## 1. Goal & non-goals

### 1.1 Goal

```bash
python -m rgw_ingest_bench run --variant v1 --schema scalar --repeat 5
# → gate-passed rows whose counters show what s3fs ACTUALLY fetched per file
```

Deliverables:

1. **`variants/v1_buffered.py`** — `fs.open()` → `read(32 KiB)` → `seek` →
   `read(32 KiB)`, the buffered idiom, serialized per file (parent §5 row V1).
2. **`taps.py`** — two *independent* fetch-measurement layers:
   - **`RangeAudit`** — patches `s3fs.S3File._fetch_range` to record every
     range s3fs actually fetches (the parent §5 mandate).
   - **`BotocoreTap`** — a botocore event hook capturing `GetObject` /
     `HeadObject` requests and their `Range` headers at the wire layer.
   The two must agree; disagreement fails the run. This turns "s3fs internals
   drifted" from a silent falsehood into a loud error.
3. **Version canary** — an import-time signature check on `_fetch_range` so a
   pinned-version bump that changes s3fs internals fails in CI, not in a
   benchmark campaign.
4. **`make rgw-stats`** — `radosgw-admin bucket stats` via `docker exec`, the
   manual server-side cross-check of client byte counters (parent §7.1),
   deferred from PR 3.

First deliverable-by-numbers: H1/H2 become measurable — V1 vs V2 bytes/file
on the same seeded corpus, from the same harness, gate-verified identical
output.

### 1.2 Non-goals (deferred)

| Deferred item | Lands in |
|---|---|
| V0 (naive full GET) — stretch beyond the MVP cut (parent §8.1); the PR 3 registry seam makes it a ~20-line follow-up whenever wanted | stretch |
| V3, concurrency, achieved-N assertion (BotocoreTap is built here to be reused for that) | PR 5 |
| `large`-tier campaign (the S > readahead regime at scale) — MVP is `medium`; the regime itself is unit-tested here (§6, T6) | stretch |
| Automated RGW-stats scraping — manual make target only | PR 6 if figures need it |

---

## 2. Compatibility contract with PR 1–3

Consumed **unchanged**:

| Interface | From | Note |
|---|---|---|
| `parse_header`, `parse_footer`, `classify_tail`, `channels()` | PR 1/3 | V1 reads exactly 32 KiB buffers through the file API, so the same parsers run byte-for-byte — file-like reads return requested lengths regardless of what s3fs fetched underneath |
| `S3Config`, `make_fs` | PR 2 | see constraint below |
| `CounterSet`, `LatencyRecorder`, `EnvInfo`, `append_result` | PR 2 | taps feed the same collectors |
| `VariantHarness`, `VariantInputs`, `Variant` protocol, registry | PR 3 | `"v1"` is one new registry entry; `run --variant v1` works with zero CLI changes |
| `SilverWriter`, `gate.verify_output` | PR 3 | identical-silver invariant now testable *across* variants (T1) |

**Zero model changes.** Tap outputs land in the existing free-form
`RunResult.counters` (`bytes_fetched_audit`, `bytes_fetched_wire`,
`get_count`, `head_count`) and `params` (`s3fs_block_size`, `s3fs_cache_type`,
fetch-size distribution summary). PR 2/3 rows stay valid; no new deps.

**One constraint made explicit and tested:** `make_fs` (PR 2) must not pass
`default_block_size`, `default_cache_type`, or any read-buffering option —
V1's entire point is *library defaults* ("the unmodified production code
path"). T9 asserts `make_fs` leaves these untouched, so a future PR can't
quietly tune them and skew V1.

---

## 3. The V1 variant (`variants/v1_buffered.py`)

Per-file sequence — deliberately the production idiom, not an optimized one:

```
for key in inputs.keys:                       # serial, like V2
    f = fs.open(key, "rb")                    # → HEAD (size); do NOT pass block_size
    hdr = f.read(HEADER_SIZE)                 # → s3fs fetches [0, 32768 + block_size) ∩ [0, S)
    fields = parse_header(hdr)
    kind = classify_tail(f.size, fields.img_width, fields.img_height, channels(fields))
    if kind is CORRUPT: raise IntegrityError(key)
    if kind is FOOTER:
        f.seek(f.size - FOOTER_SIZE)
        buf = f.read(FOOTER_SIZE)             # cache hit OR second fetch — regime-dependent, §4
        footer = parse_footer(buf)            # magic + file_id_echo, same checks as V2
    f.close()
    writer.write_row(fields, footer_or_none)
```

- The variant never calls `cat_file` and never passes buffering options —
  the contrast with V2 (PR 3 used `cat_file` precisely so *its* counters
  measure requested ranges) is the whole experiment.
- Retries/abort policy, latency recorders, batched `SilverWriter`, gate:
  all inherited from the PR 3 harness unchanged.
- Runtime-recorded (never assumed from docs): `f.blocksize` and
  `f.cache.name` from the first opened file → `params`. Parent §5: defaults
  are version-dependent; we record what this run actually used.

## 4. Expected fetch behavior — hypotheses the audit exists to verify

With s3fs defaults (block_size ≈ 5 MiB, readahead cache), the *first* read
fetches `[0, 32768 + block_size)` clipped to file size `S`. What happens next
is regime-dependent, and this nuance is exactly why parent §5 demands
instrumentation instead of a formula:

| Tier | S | First fetch | Footer read | GETs | Bytes/file (expected) |
|---|---|---|---|---|---|
| `small` | ~128 KiB | whole file | **cache hit** | 1 | ≈ S (~128 KiB) |
| `medium` | ~1.09 MiB | whole file | **cache hit** | 1 | ≈ S (~1.09 MiB) |
| `large` | ~32 MiB | ~5.03 MiB | second fetch (32 KiB + readahead clip) | 2 | ≈ 5.03 MiB + 32 KiB |

Consequences worth stating up front (they will surprise a reader of the
parent's simplified "3 serial RTTs" row):

- On the MVP `medium` tier, **V1 degenerates to "HEAD + download the whole
  object"** — the readahead swallows a 1 MiB file entirely, the footer read
  is a cache hit, and there are only 2 round trips, not 3. The headline
  number is bytes: ~1.09 MiB vs V2's ~61 KiB ⇒ **~17× more data moved for
  identical silver output** (H1's medium-tier instantiation).
- Only `large` (S > readahead) shows the 3-RTT, capped-fetch shape — the
  parent §3.3 rationale for that tier, now concretely predicted.
- These are **predictions, not assertions**: the table gets confirmed or
  corrected by `RangeAudit` on real runs, and the corrected version goes in
  the writeup. If s3fs's cache behaves differently under the pinned version,
  that is a *finding*, recorded by the same machinery.

## 5. Instrumentation (`taps.py`)

### 5.1 `RangeAudit` — the s3fs-layer tap

```python
class FetchRecord(BaseModel):
    key: str; start: int; end: int; nbytes: int; ms: float

class RangeAudit:
    """Records every range s3fs actually fetches, via S3File._fetch_range.

    Context manager. __enter__ saves the original method and installs a
    wrapper (plain attribute save/restore with try/finally semantics — no
    unittest.mock in library code); __exit__ ALWAYS restores, exception or
    not. Wrapper: t0 → original(start, end) → append FetchRecord, feed
    CounterSet("bytes_fetched_audit", "get_count") + LatencyRecorder("fetch").
    """
    records: list[FetchRecord]
    def summary(self) -> dict[str, float]   # fetches/file, mean/max nbytes, total
```

- Patching is process-global (class attribute), so the harness runs variants
  strictly sequentially per repetition — already true — and T8 proves
  restoration even when the variant raises mid-run.
- HEADs are counted by the wire tap (below), not by patching `fs.info` —
  one intrusive patch is enough.

### 5.2 `BotocoreTap` — the wire-layer tap

```python
class BotocoreTap:
    """Independent measurement: botocore event hooks, no s3fs internals.

    Registers 'before-send.s3.GetObject' / '.HeadObject' handlers on the
    session under the harness's S3FileSystem, recording each request's key,
    Range header (parsed to start/end), and timestamp. Feeds
    CounterSet("bytes_fetched_wire", "get_count_wire", "head_count").
    """
```

- Uses only botocore's *public, stable* event system — this is the layer that
  survives s3fs refactors.
- **Cross-check rule (enforced by the harness for V1 runs):** at repetition
  end, `bytes_fetched_audit == bytes_fetched_wire` and
  `get_count == get_count_wire`, else the repetition is marked invalid with a
  loud error naming both numbers. Two independent observers agreeing is the
  strongest defense against the parent §11 "defaults drift across versions"
  risk — stronger than any pin.
- Reuse seam: PR 5's §6.5 achieved-concurrency assertion will extend this tap
  with in-flight timestamps; designed now with `t_start/t_end` per record so
  PR 5 adds a computation, not a hook.

### 5.3 Version canary

```python
def assert_s3fs_contract() -> None:
    """Fail fast if pinned-s3fs internals moved.

    Asserts s3fs.S3File._fetch_range exists with signature (self, start, end),
    and that the default cache class is one this doc's §4 model understands.
    Raised at RangeAudit construction and exercised as a plain unit test, so
    a dependency bump that breaks the tap fails in CI with a message pointing
    at this doc — never silently mismeasures a campaign.
    """
```

### 5.4 Server-side cross-check (`make rgw-stats`)

`docker exec <rgw> radosgw-admin bucket stats --bucket=bronze` before/after a
run; the delta of `bytes_sent` is the third, fully independent opinion on
bytes moved. Manual (acceptance checklist §8), not CI — RGW-only, and the demo
container's admin tooling is exactly what parent §7.1's "cross-check against
RGW bucket stats" meant.

## 6. Test plan

Same regime: pytest + fixtures, moto default, MinIO marks, 100 % line/branch
on new code. Key trick: **the S > readahead (`large`) regime is unit-tested
without 32 MiB files** by opening with a tiny explicit `block_size` in the
*test only* (T6) — the audit logic doesn't care whether the cap came from a
5 MiB default or a 64 KiB override; V1 production code still never passes one.

| # | Test | Asserts (incl. unhappy paths) |
|---|---|---|
| T1 | `test_v1_end_to_end_and_cross_variant_hash` | seed → run v1 → gate passes; `content_hash` **equals V2's hash** on the same corpus/schema — first cross-variant proof of the §5 identical-silver invariant |
| T2 | `test_v1_fetch_counts_small_regime` | tiny corpus (S < block_size): exactly 1 fetch/file, fetch covers whole object, footer read produced **no** second fetch; bytes_audit == Σ object sizes |
| T3 | `test_taps_agree` | audit vs wire: byte totals and GET counts equal; deliberate desync (drop one wire record via monkeypatch) → repetition marked invalid, loud error |
| T4 | `test_wire_tap_head_count` | HEADs == n_files (one per `fs.open`); Range headers parsed correctly incl. suffix/clipped forms |
| T5 | `test_canary` | `assert_s3fs_contract` passes on pinned version; monkeypatched wrong-signature `_fetch_range` → clear failure naming this doc |
| T6 | `test_large_regime_via_small_blocksize` | file with S > block_size (test-only override): 2 fetches — first ≈ 32 KiB + block_size, second covers footer; bytes ≈ min(S, RA) + footer fetch; §4 table's arithmetic validated cheaply |
| T7 | `test_v1_footerless_and_integrity` | footerless: no footer read, still 1 fetch, null footer columns; truncated object → CORRUPT abort; pixels-as-footer → `footer_magic` IntegrityError (same PR 1 guards, now through the buffered path) |
| T8 | `test_audit_restores_on_exception` | variant raising mid-run → `_fetch_range` is the original afterward; nested/duplicate audit → explicit error (no silent double-patch) |
| T9 | `test_make_fs_leaves_defaults` | PR 2's `make_fs` passes no block-size/cache kwargs; recorded `params["s3fs_block_size"]` equals the library default at test time |
| T10 | `test_params_recorded` | block size, cache type, fetch-size summary present in the RunResult row; distribution numbers consistent with records |
| T11 | `@pytest.mark.minio` | T1–T3 against live MinIO — real HTTP Range semantics vs moto's |

## 7. CLI & recorded output

No new commands — `run --variant v1` exists the moment the registry entry
does (PR 3's seam working as designed). New per-row content, all in existing
free-form fields:

```
counters:  bytes_fetched_audit, bytes_fetched_wire, get_count, get_count_wire, head_count
params:    s3fs_block_size, s3fs_cache_type, fetches_per_file_mean,
           fetch_bytes_mean, fetch_bytes_max
```

`--dump-samples` (PR 2 flag) additionally writes the full `FetchRecord` list
as JSONL beside the results file — raw material for the PR 6 bytes figure.

## 8. Acceptance checklist (PR review gate)

- [ ] On the WSL2 box against RGW, `medium` tier: V1 runs gate-pass and the
      counters confirm (or correct!) the §4 table — expected ≈ 1 GET/file,
      ≈ 1.09 MiB/file, ~17× V2's bytes; actual numbers + verdict pasted into
      the PR description.
- [ ] `content_hash` identical between V1 and V2 runs on the same corpus.
- [ ] Audit vs wire tap totals equal on every repetition; `make rgw-stats`
      delta ≈ client-side bytes (± protocol overhead) recorded once manually.
- [ ] Canary green on the pinned s3fs; deliberately bumping s3fs a major
      version locally shows the canary failing informatively (screenshot in PR).
- [ ] 100 % line/branch on new code; no new deps; no model changes
      (PR 2/3 JSONL rows still parse).

## 9. Open questions

1. **Does a cache hit really cost zero wire ops in the pinned s3fs?** §4
   assumes the footer read on `medium` is served from the readahead cache.
   If the wire tap shows otherwise, the §4 table gets corrected — that is
   the deliverable working, not a design failure. Resolved by T2/T11 + first
   real run.
2. **`radosgw-admin` availability inside the demo container** — assumed
   present (it ships in ceph/demo); if the pinned digest lacks it, fall back
   to RGW usage log via the admin REST API, still behind `make rgw-stats`.
3. **Whether to fold V0 in here after all** — it shares the taps and would
   make the H1 figure three-bar instead of two. Default: no (MVP cut, parent
   §8.1); revisit only if the writeup wants V0's bar before the stretch
   window. The registry seam keeps it a trivial follow-up either way.
