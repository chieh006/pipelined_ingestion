# Progress Log — Pipelined Ingestion Benchmark

Running log of work done on the RGW pipelined-ingestion benchmark project.

**Rule:** append-only, newest first; the **Next up** pointer stays pinned at the top, above the dated entries — exactly one one-line bullet per date; if a day has multiple changes, condense them into that single line.

---

**Next up:** open the PR for review, then implement PR 2 per
[pr2_environment_seeding_metrics.md](design/pr2_environment_seeding_metrics.md).

- **2026-07-21** — Implemented **PR 1 (corpus foundation)** on branch `pr1-corpus-foundation` (full `uv`/`src` scaffold + generator/parsers/`generate` CLI, 53 tests — 49 unit at 100 % coverage + 4 integration — all green): switched synthetic pixels to full 0–255 (mod 256, replacing prime-251); added a readable `gib` field beside the exact `bytes` in `--json` output (propagated across the seed/run summaries in the pr2–6 docs); hardened the integration tests (single-source geometry constants so CLI args and assertions can't diverge; throughput now prints under `-s`); git-ignored generated `.raw` corpora; and validated the full §7.3 walkthrough end-to-end (`generate --tier small` → 10 000 files, 1.191 GiB, gate + determinism + streaming all pass). Same-day follow-ups: made the package version single-source (`pyproject.toml` → hatch `dynamic = ["version"]` reading `__init__.py` per root CLAUDE.md §7; `uv.lock` re-locked; build + 100 % gate re-verified); and reconciled [pr2_environment_seeding_metrics.md](design/pr2_environment_seeding_metrics.md) with the as-built PR 1 (synced the `gib` summary wording, reframed §2 as "two behaviour-preserving changes" covering the §5.2 shared parent-parser refactor, and recorded the `0.1.0 → 0.2.0` bump deliverable).

- **2026-07-19** — Decided the PR split (5 core + 1 optional, ordered
  PR 1 corpus → PR 2 environment → V2 → V1 → V3 → sweep/report) and wrote
  all six PR implementation design docs in [docs/design/](design/).
