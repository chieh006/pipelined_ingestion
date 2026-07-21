# PR 1 Implementation Design — Corpus Foundation (`fakeraw` + parsing, no Ceph)

**Status:** Draft
**Date created:** 2026-07-19
**Parent doc:** [rgw_pipelined_ingestion_benchmark.md](rgw_pipelined_ingestion_benchmark.md)
(§2 file anatomy, §3 synthetic corpus, §12 repo layout)
**Scope:** first PR of the benchmark harness — everything needed to *create* and
*read back* synthetic `.raw` files on local disk. Zero infrastructure: no Docker,
no Ceph, no network. `pytest` runs green on a bare laptop.

---

## 1. Goal & non-goals

### 1.1 Goal

Deliver the data layer every later PR stands on:

1. A **repo scaffold** (`pyproject.toml`, `src/` package layout, pinned deps).
2. **`layout.py`** — the single source of truth for byte offsets and section
   sizes, imported by both generator and parsers so they can never drift.
3. **`fakeraw.py`** — a streamed, deterministic corpus generator driven by a
   Pydantic spec.
4. **`manifest.py`** — JSONL manifest write/read (`path, size, has_footer,
   file_id`), later consumed by V3's manifest mode and the §7.2 correctness gate (I1).
5. **`parse.py`** — header/footer parsing (`np.frombuffer` at fixed offsets),
   footer-presence arithmetic, and the pixel-pattern guard.
6. **A minimal CLI**: `python -m rgw_ingest_bench generate` (local disk only).
7. **Tests** proving generator ↔ parser roundtrip, footer arithmetic, pattern
   guard, determinism, and manifest integrity — 100 % line/branch coverage on
   the new code.

### 1.2 Non-goals (explicitly deferred)

| Deferred item | Lands in |
|---|---|
| `docker-compose.yml`, Makefile, RGW/MinIO profiles | PR 2 |
| `seed` command (upload to bucket, s3fs) | PR 2 |
| `metrics.py`, netem/RTT probe | PR 2 |
| Any variant (`v0`–`v3`), `run`/`sweep`/`report` CLI | PR 3+ |

**Clean-room reminder (parent §2):** the synthetic field schema below is *not*
the production offset table. Only the shape is mirrored: 32 KiB fixed-offset
header / large opaque pixel middle / optional 32 KiB trailing footer. Do not
copy offsets from `raw_file_parser.py` beyond what parent §3.1 already defines.

---

## 2. Repo scaffold

Created at the harness root (per parent §12; whether that root is this repo
after `git init` or a fresh `rgw-ingest-bench/` checkout is an open question
tracked outside this doc — the scaffold is identical either way):

```
rgw-ingest-bench/
  pyproject.toml
  README.md                       # one paragraph + generate quick-start
  src/rgw_ingest_bench/
    __init__.py                   # __version__
    __main__.py                   # delegates to cli.main()
    cli.py                        # argparse; `generate` subcommand only (PR 1)
    layout.py
    fakeraw.py
    manifest.py
    parse.py
  tests/
    conftest.py                   # shared fixtures (tiny specs, tmp corpora)
    test_layout.py
    test_fakeraw.py
    test_parse.py
    test_manifest.py
    test_cli.py
    test_integration.py           # @pytest.mark.integration: CLI end-to-end + throughput
```

`pyproject.toml`:

- Build backend: `hatchling`; `src/` layout; `requires-python = ">=3.12"`.
- Runtime deps (pinned, parent §4): `numpy`, `pydantic>=2`, `polars`.
  (`pyarrow`, `s3fs`/`aiobotocore`, `uvloop` are **not** needed until PR 2/5 —
  do not add them here; every dep added now is a dep CI installs forever.)
- Dev deps: `pytest`, `pytest-cov`, `psutil` (dev-only; backs the §7.2 RSS
  streaming check — I3 `skipif`s when it is absent).
- Coverage config: `fail_under = 100` scoped to `src/rgw_ingest_bench`
  (branch coverage on), measured on the fast `-m "not integration"` run.
- Register the marker under `[tool.pytest.ini_options]`
  (`markers = ["integration: end-to-end CLI tests (subprocess, slow)"]`) so
  `-m "not integration"` selects the fast/coverage gate and strict-marker runs
  don't warn.

All paths handled via `pathlib.Path`; no `os.path` string joins. All functions
carry NumPy-style docstrings. Logging via `logging` with f-strings; `print()`
only in `cli.py` console output.

---

## 3. `layout.py` — shared offset schema

Pure constants + tiny pure functions. **No I/O.** This module is imported by
`fakeraw.py`, `parse.py`, and (later) every variant.

### 3.1 Section sizes

```python
HEADER_SIZE: Final[int] = 32 * 1024          # 32_768
FOOTER_SIZE: Final[int] = 32 * 1024          # 32_768
PATTERN_MODULUS: Final[int] = 251            # see §4.3
```

### 3.2 Field tables

A field is a frozen Pydantic model — name, dtype, offset — so the table is
validated at import time and usable by both sides:

```python
class Field(BaseModel):
    """One fixed-offset scalar or array field."""
    model_config = ConfigDict(frozen=True)
    name: str
    dtype: str        # numpy dtype string, e.g. "<i4", "<f4"
    offset: int       # byte offset within its section
    count: int = 1    # >1 ⇒ array field
```

**Header fields** (parent §3.1, verbatim):

| name | dtype | offset |
|---|---|---|
| `img_width` | `<i4` | 0 |
| `img_height` | `<i4` | 4 |
| `file_id` | `<i4` | 20 |
| `pixel_size_x` | `<f4` | 48 |
| `scan_dir` | `<i4` | 92 |
| `channel_mask` | `<i4` | 17720 |

**Footer fields** (parent §3.1: "a few scalars + two `float32[4070]` arrays"):

| name | dtype | offset | count | bytes |
|---|---|---|---|---|
| `footer_magic` | `<i4` | 0 | 1 | 4 |
| `file_id_echo` | `<i4` | 4 | 1 | 4 |
| `n_points` | `<i4` | 8 | 1 | 4 |
| `local_offset_x` | `<f4` | 128 | 4070 | 16 280 |
| `local_offset_y` | `<f4` | 16408 | 4070 | 16 280 |

Total used: 32 688 ≤ 32 768 ✓. `footer_magic` is a fixed sentinel
(`0x0F007E4A`); `file_id_echo` lets integrity checks confirm a fetched footer
belongs to the right object. An import-time validation loop asserts fields are
non-overlapping and inside their section — a wrong entry fails at import, not
at analysis time.

### 3.3 Size arithmetic (the footer-presence functions)

The three pure functions the whole benchmark's range math hangs on:

```python
def pixel_bytes(width: int, height: int, channels: int) -> int:
    """W·H·C — size of the pixel middle section."""

def expected_size(width: int, height: int, channels: int, has_footer: bool) -> int:
    """HEADER_SIZE + W·H·C (+ FOOTER_SIZE if has_footer)."""

def classify_tail(size: int, width: int, height: int, channels: int) -> TailKind:
    """Decide what the trailing 32 KiB of an object is.

    Returns
    -------
    TailKind
        ``FOOTER``    if size == HEADER + W·H·C + FOOTER
        ``NO_FOOTER`` if size == HEADER + W·H·C   (trailing bytes are pixels)
        ``CORRUPT``   otherwise (integrity error — parent §6.3 step 4)
    """
```

`TailKind` is a `StrEnum`. `classify_tail` is *the* footer-arithmetic unit
under test in §7; V1–V3 all call it in later PRs.

---

## 4. `fakeraw.py` — corpus generator

### 4.1 Config model (parent §3.2, extended)

```python
class FakeRawSpec(BaseModel):
    """Generation spec for one corpus tier."""
    n_files: int = Field(gt=0)
    img_width: int = Field(gt=0)
    img_height: int = Field(gt=0)
    n_channels: int = Field(gt=0, le=32)
    footer_ratio: float = Field(default=0.9, ge=0.0, le=1.0)
    seed: int = 42
```

Tier presets as a module-level dict (parent §3.3):

```python
TIER_SPECS: Final[dict[str, FakeRawSpec]] = {
    "small":  FakeRawSpec(n_files=10_000, img_width=256,  img_height=256,  n_channels=1),
    "medium": FakeRawSpec(n_files=10_000, img_width=1024, img_height=1024, n_channels=1),
    "large":  FakeRawSpec(n_files=500,    img_width=4096, img_height=4096, n_channels=2),
}
```

### 4.2 Determinism model

Everything is a pure function of `(spec.seed, file_id)` — no hidden RNG state,
so generation order (or a future parallel/seeding rerun in PR 2) cannot change
output bytes:

- **Footer presence:** one upfront draw,
  `numpy.random.default_rng(seed).random(n_files) < footer_ratio` → a boolean
  vector (vectorized, one call). Stored in the manifest as `has_footer`.
- **Header values:** `file_id` = corpus index (0-based);
  `scan_dir = file_id % 2`; `pixel_size_x = 0.1 * (1 + file_id % 4)`;
  `channel_mask = (1 << n_channels) - 1`; width/height from the spec.
- **Pixel bytes:** the pattern function in §4.3.
- **Footer arrays:** `local_offset_x[i] = float32(file_id + i * 1e-3)`,
  `local_offset_y[i] = float32(file_id - i * 1e-3)`; `n_points = 4070`;
  `file_id_echo = file_id`. Computed with vectorized numpy expressions.

Same seed ⇒ byte-identical corpus + manifest. This is asserted in tests (§7).

### 4.3 Pixel pattern (the off-by-one tripwire)

Purpose (parent §3.1): any range fetch that is shifted, truncated, or lands in
the wrong section must produce bytes that *provably* aren't what was expected.

```python
def expected_pixel_bytes(file_id: int, start: int, length: int) -> np.ndarray:
    """Pattern bytes for pixel-region indices [start, start+length).

    byte[i] = (file_id * 31 + i) % PATTERN_MODULUS   (uint8)
    """
```

Design notes:

- **Modulus 251 (prime), not 256.** A 256-period pattern is invisible to any
  shift that is a multiple of 256 — and every interesting offset in this file
  format (32 KiB header, 4 KiB blocks, s3fs block sizes) is a multiple of 256.
  With period 251, any misalignment up to 250 bytes — and in particular all
  power-of-two shifts — changes the bytes.
- **`file_id * 31` phase term** makes the pattern file-unique, so a range read
  from the *wrong object* also fails the check.
- Values stay in `[0, 250]`, so `0xFF` never appears in pixels — cheap extra
  signal when eyeballing hexdumps.
- Implementation is vectorized: `(np.arange(start, start + length, dtype=np.int64)
  + file_id * 31) % PATTERN_MODULUS`, cast to `uint8`. No Python-level loops.

### 4.4 Streamed writing

```python
def generate_file(spec, file_id, has_footer, out_path, *, chunk_size=4 * 2**20) -> ManifestEntry
def generate_corpus(spec, out_dir) -> Iterator[ManifestEntry]
```

- `generate_file` writes header (one 32 KiB `bytes` built by stamping §3.2
  fields into a zeroed buffer), then pixel pattern in `chunk_size` slabs, then
  the footer if present. Peak memory ≈ one chunk (parent §3.3: "streamed,
  never all in RAM") — a `large`-tier 32 MiB file never materializes whole.
- `generate_corpus` iterates `file_id = 0 … n_files-1`, draws the footer
  vector once (§4.2), yields a `ManifestEntry` per file. It is a generator so
  the PR 2 `seed` command can interleave generation with upload later.
- File naming: `{file_id:08d}.raw`, flat under `out_dir`. Paths built with
  `pathlib` only.
- Logs one `logging.info` per 1 000 files (f-string), not per file.

---

## 5. `manifest.py` — seed manifest

```python
class ManifestEntry(BaseModel):
    """One corpus object: identity + the facts V3 needs instead of a HEAD."""
    path: str          # relative POSIX-style path ("00000042.raw")
    size: int
    has_footer: bool
    file_id: int

def write_manifest(entries: Iterable[ManifestEntry], path: Path) -> int
def read_manifest(path: Path) -> list[ManifestEntry]
def read_manifest_df(path: Path) -> pl.DataFrame
```

- Format: JSONL, one `model_dump_json()` line per entry, written streamed as
  `generate_corpus` yields (never buffers the corpus list).
- `path` is stored **relative and POSIX-style** (`Path.as_posix()`): entries
  must be valid both as local relative paths and as S3 keys, and must compare
  equal across Windows/Linux.
- `read_manifest` re-validates every line through Pydantic (a truncated or
  hand-edited manifest fails loudly). `read_manifest_df` uses
  `polars.read_ndjson` — this is what the §7.2 correctness gate (I1) and the
  analysis scripts will join against later.

---

## 6. `parse.py` — reading it back

The exact functions every variant reuses in PR 3+; written and tested now
against local files.

```python
class HeaderFields(BaseModel):
    img_width: int; img_height: int; file_id: int
    pixel_size_x: float; scan_dir: int; channel_mask: int

class FooterFields(BaseModel):
    file_id_echo: int; n_points: int
    local_offset_x: list[float]; local_offset_y: list[float]

def parse_header(buf: bytes) -> HeaderFields
def parse_footer(buf: bytes) -> FooterFields
def verify_pixel_range(buf: bytes, file_id: int, start: int) -> bool
```

- Both parsers take a `bytes` buffer (length exactly `HEADER_SIZE` /
  `FOOTER_SIZE`; anything else raises `ValueError` before any decoding) — in
  PR 1 the buffer comes from a local `Path.read_bytes()` slice, in PR 3+ from
  a ranged GET. The parsers never do I/O, which is what makes them reusable
  across every variant and trivially testable.
- Decoding is `np.frombuffer(buf, dtype, count, offset)` driven by the
  `layout.py` field tables — no hand-repeated offsets.
- `parse_footer` checks `footer_magic` first and raises `IntegrityError`
  (module-local exception) on mismatch: a "footer" that is actually pixels
  (parent §2's discard case) or garbage fails here, not downstream.
- `verify_pixel_range` compares `buf` against `expected_pixel_bytes(file_id,
  start, len(buf))` via one vectorized `np.array_equal` — this is the §6.3
  step-5 guard-byte cross-check.

---

## 7. Test plan

`pytest` + fixtures throughout (no `unittest`); `tmp_path` for every file the
tests create. A `tiny_spec` fixture (`8×8×1, n_files=6, footer_ratio=0.5,
seed=7`) keeps the suite fast (<2 s); one `medium`-geometry single-file test
covers the multi-chunk streaming path. The plan splits into fast, in-process
**unit tests** (§7.1) and slower, subprocess-driven **integration tests**
(§7.2) that drive the CLI end to end — including the throughput check an
operator runs by hand.

### 7.1 Unit test matrix

| # | Test | Asserts (including unhappy paths) |
|---|---|---|
| T1 | `test_layout_tables_valid` | fields non-overlapping, inside section bounds; footer arrays fit in 32 KiB; import-time validator rejects a deliberately overlapping table |
| T2 | `test_size_arithmetic` | `expected_size` for all four (tier, footer) combos; `classify_tail` returns `FOOTER` / `NO_FOOTER` exactly, `CORRUPT` for sizes off by ±1 and ±32 768±1 |
| T3 | `test_spec_validation` | `FakeRawSpec` rejects `n_files=0`, negative dims, `footer_ratio=1.5`, `n_channels=0` (Pydantic `ValidationError`) |
| T4 | `test_roundtrip_header` | generate → `parse_header` → every field equals the §4.2 formulas |
| T5 | `test_roundtrip_footer` | footer file: `parse_footer` fields match formulas incl. array contents; footerless file: `classify_tail` says `NO_FOOTER` and trailing 32 KiB verifies as *pixels* |
| T6 | `test_pattern_guard_detects_shift` | correct range passes `verify_pixel_range`; ranges shifted by +1, −1, +256, +4096 bytes all fail; range read with the wrong `file_id` fails |
| T7 | `test_footer_magic_guard` | `parse_footer` on pixel bytes raises `IntegrityError`; short buffer raises `ValueError` |
| T8 | `test_determinism` | two runs, same seed → byte-identical files and manifests; different seed → different footer-presence vector |
| T9 | `test_streaming_equivalence` | chunked write (small `chunk_size`) produces bytes identical to a one-shot reference build; file size matches `expected_size` |
| T10 | `test_footer_ratio_edges` | `footer_ratio=0.0` → no footers, `=1.0` → all footers; manifest `has_footer` matches on-disk sizes for every file |
| T11 | `test_manifest_roundtrip` | write → `read_manifest` equality; `read_manifest_df` shape/dtypes; corrupted line → loud failure; sizes in manifest match `Path.stat()` |
| T12 | `test_cli_generate` | CLI in `tmp_path` creates `n_files` files + manifest; exit code 0; `--json` mode (driven in-process via `cli.main([...])`) emits a valid stats object with the five keys and correct `files`/`bytes` — covers the throughput-summary branch for the 100 % gate; unknown tier / missing args → non-zero exit + usage message |

Coverage: 100 % line + branch on all six modules (project guideline §6);
enforced via `--cov --cov-branch --cov-fail-under=100` in CI config (the CI
workflow file itself may land in PR 2 with the rest of the infra — the local
`pytest` invocation in the README carries the flags until then). The gate is
met by the in-process unit tests above (T1–T12 all call library functions or
`cli.main([...])` directly); the §7.2 integration tests shell out to a
subprocess, add no line coverage, and so run *outside* the gate —
`pytest -m "not integration" --cov …` enforces 100 %, `pytest -m integration`
runs the end-to-end pass.

### 7.2 Integration tests (`test_integration.py`)

End-to-end tests that drive the **real CLI** (`sys.executable -m
rgw_ingest_bench …` as a subprocess — closest to how an operator invokes it)
across the whole stack (`cli → fakeraw → manifest → parse → layout`) against a
real filesystem. Marked `@pytest.mark.integration` so the fast unit suite stays
<2 s (`pytest -m "not integration"`); the full pass is `pytest` or
`pytest -m integration`. Geometry stays small (tens of files) so wall-clock is
a few seconds — except I3, which needs one `large`-geometry file to exercise
streaming.

| # | Test | Asserts |
|---|---|---|
| I1 | `test_cli_roundtrip_corpus` | `generate` into `tmp_path`, then reload `manifest.jsonl` and, for **every** entry: on-disk `stat().st_size == entry.size == expected_size(...)`; `parse_header` fields equal the §4.2 formulas; `has_footer` files `parse_footer` cleanly with `file_id_echo == file_id`; a mid-pixel ranged slice passes `verify_pixel_range`. Realizes the full-corpus **correctness gate** referenced by §5/§6, exercised through the CLI. |
| I2 | `test_cli_throughput` | run `generate --json`, parse the stats object (`files, bytes, elapsed_s, mib_per_s, files_per_s`). Assert `files == n_files` and `bytes == Σ` on-disk sizes (CLI accounting is correct); assert `mib_per_s == bytes / elapsed_s / 2**20` within 1 % (**reported throughput is accurate**, not merely present); assert `elapsed_s ≤` the test's own wall-clock around the subprocess; assert an **opt-in floor** — when `BENCH_MIN_GENERATE_MIB_PER_S` is set, the measured `mib_per_s` must clear it; unset ⇒ accounting-only, so CI never flakes on machine variance (suggested trusted-machine value ≈ 20 MiB/s) — a non-streaming / O(n²) regression trips wherever the floor is set. |
| I3 | `test_cli_memory_streaming` | generate one `large`-geometry file (~32 MiB) via the CLI subprocess while sampling the child's peak RSS (`psutil`; `skipif` when unavailable); assert peak RSS stays under a small multiple of `chunk_size` and well below the file size — the executable proof of the §4.4 "never all in RAM" claim. |
| I4 | `test_cli_determinism_e2e` | two `generate` runs, same `--seed`, different out dirs → byte-identical `manifest.jsonl` and identical per-file SHA-256 across the corpus; a third run with a different `--seed` → a differing manifest. Promotes T8's determinism claim to a through-the-CLI check. |

**Throughput contract (drives the small §8 CLI addition).** So a test — or an
operator — can *verify* throughput without scraping free text, `generate` grows
a machine-readable summary: `--json` prints exactly one JSON object to stdout,
`{"files", "bytes", "elapsed_s", "mib_per_s", "files_per_s"}`; without `--json`
the human summary (§8) simply gains the two rate figures. I2 parses that object.
Its floor is opt-in and a regression tripwire, **not** a benchmark — real,
netem-aware throughput numbers are PR 2's `metrics.py` job (non-goal here). The
floor env var follows a convention shared by every later PR:
`BENCH_MIN_<command>_<rate>`, naming the CLI subcommand and the throughput rate
it bounds — `BENCH_MIN_GENERATE_MIB_PER_S` / `BENCH_MIN_SEED_MIB_PER_S` bound
`mib_per_s`, `BENCH_MIN_RUN_FILES_PER_S` bounds `run`'s median `files_per_s`.

### 7.3 Running the tests (walkthrough)

From the harness root (the directory holding `pyproject.toml`), in a fresh
virtualenv:

```bash
python -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\activate
pip install -e ".[dev]"              # pulls in pytest-cov + psutil
```

**1 — Everyday loop: fast unit suite + coverage gate** (no subprocess, <2 s):

```bash
pytest -m "not integration" --cov=rgw_ingest_bench --cov-branch --cov-fail-under=100
```

**2 — The integration tests** (I1–I4; each shells out to a real
`rgw_ingest_bench` process and writes only under pytest's `tmp_path`):

```bash
pytest -m integration -v
```

**3 — Just the throughput test (I2), watching the number** — `-s` un-captures
stdout so the measured rate prints:

```bash
pytest -m integration -k throughput -s
```

I2 always asserts the CLI's throughput *accounting* is correct; the absolute
floor is opt-in. Set `BENCH_MIN_GENERATE_MIB_PER_S` to enforce a floor on a
machine you trust (leave it unset ⇒ accounting-only, so CI never flakes on
hardware variance); a good starting value on local disk is ≈ 20 MiB/s:

```bash
BENCH_MIN_GENERATE_MIB_PER_S=50 pytest -m integration -k throughput
# Windows PowerShell:  $env:BENCH_MIN_GENERATE_MIB_PER_S=50; pytest -m integration -k throughput
```

**4 — Verify throughput by hand (exactly what I2 automates)** — run the CLI with
`--json` and read the rate straight off stdout:

```bash
python -m rgw_ingest_bench generate --tier small --out ./corpus --json
# {"files": 10000, "bytes": 1719664640, "elapsed_s": 6.7, "mib_per_s": 244.8, "files_per_s": 1492.5}

python -m rgw_ingest_bench generate --tier small --out ./corpus --json | jq .mib_per_s
```

**5 — Everything at once** (unit + integration), e.g. a pre-push check:

```bash
pytest
```

Notes:

- The 100 % coverage gate is run *only* on the `-m "not integration"` selection:
  subprocess tests register no lines (§7.2), so including them would make the
  gate unmeetable. CI runs the two selections as separate jobs.
- I3 (`test_cli_memory_streaming`) needs `psutil` and `skip`s automatically if
  it is absent, so the suite still passes on a minimal environment.
- Editing source needs no reinstall — `pip install -e` is editable and the
  integration tests launch the package via `sys.executable -m rgw_ingest_bench`,
  so they pick up live code. Reinstall only when entry points or deps change.

---

## 8. CLI (`generate` only)

```bash
python -m rgw_ingest_bench generate --tier medium --out ./corpus [--seed 42]
python -m rgw_ingest_bench generate --n-files 100 --width 256 --height 256 \
                                    --channels 1 --out ./corpus   # explicit spec
python -m rgw_ingest_bench generate --tier small --out ./corpus --json  # machine-readable stats
```

- `argparse` subcommands; `--tier` and the explicit-spec flags are mutually
  exclusive groups. The command builds a `FakeRawSpec`, calls
  `generate_corpus`, streams the manifest to `<out>/manifest.jsonl`, and
  prints a one-line summary — files, bytes, elapsed, **and throughput
  (MiB/s, files/s)**, so a `generate` run doubles as a quick local throughput
  check. `print` is acceptable here (CLI console output); progress uses
  `logging`.
- `--json` emits that same summary as one JSON object on stdout
  (`{"files", "bytes", "elapsed_s", "mib_per_s", "files_per_s"}`) and
  suppresses the human line so stdout stays valid JSON — this is what the §7.2
  `test_cli_throughput` integration test parses.
- PR 2 adds `seed` (upload) beside it; the subcommand registry is a dict so
  that addition is one entry.

---

## 9. Acceptance checklist (PR review gate)

- [ ] `pip install -e .[dev] && pytest` passes on a clean machine, no Docker
      (fast gate `pytest -m "not integration" --cov …` at 100 %; full pass
      `pytest -m integration` green).
- [ ] `generate --tier small` on a laptop: ~1.6 GiB corpus, flat memory
      profile (spot-check RSS, now covered by I3), manifest line count =
      10 000, and the printed throughput figure is non-trivial (I2 verifies its
      accounting).
- [ ] Same-seed rerun reproduces byte-identical output (T8 / I4 also run
      manually once on `small`).
- [ ] 100 % line/branch coverage on new code; no `pandas`, no `print` outside
      `cli.py`, all paths via `pathlib`, NumPy-style docstrings throughout.
- [ ] No production offsets beyond parent §3.1's synthetic schema (clean-room
      check against `raw_file_parser.py`).

## 10. Open questions

1. **Harness root:** `git init` this repo vs. fresh `rgw-ingest-bench/`
   checkout (parent §12). Blocks nothing in this doc; decide before opening
   the PR.
2. **Flat 10 k-file directory:** fine on ext4/NTFS at this scale; revisit
   (two-level fan-out) only if `small`-tier generation shows filesystem pain.
3. **`pixel_size_x` float roundtrip:** values chosen (`0.1 · k`) are not
   exactly representable in float32; tests must compare via
   `np.float32(expected)` rather than Python floats. Noted here so T4 is
   written correctly the first time.
