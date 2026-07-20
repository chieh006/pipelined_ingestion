# Progress Log — Pipelined Ingestion Benchmark

Running log of work done on the RGW pipelined-ingestion benchmark project.
Newest entries first. Append one dated section per working session.

---

- **2026-07-19** — Decided the PR split (5 core + 1 optional, ordered
  PR 1 corpus → PR 2 environment → V2 → V1 → V3 → sweep/report) and wrote
  all six PR implementation design docs in [docs/design/](design/).

**Next up:** implement PR 1 per
[pr1_corpus_foundation.md](design/pr1_corpus_foundation.md) (settle its §10.1
harness-root question first — the workspace is not yet a git repo).
