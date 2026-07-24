"""PR 2 refactor guard: iter_file_chunks matches generate_file (T14)."""

from __future__ import annotations

from rgw_ingest_bench.fakeraw import FakeRawSpec, generate_file, iter_file_chunks


def test_iter_file_chunks_equivalence(tiny_spec: FakeRawSpec, tmp_path) -> None:
    """Chunks concatenated equal the on-disk file byte-for-byte (complements T9).

    A small ``chunk_size`` forces the multi-slab path so the pixel loop is
    exercised for both footer and footerless files.
    """
    for file_id in range(tiny_spec.n_files):
        for has_footer in (True, False):
            out_path = tmp_path / f"{file_id}_{int(has_footer)}.raw"
            generate_file(tiny_spec, file_id, has_footer, out_path, chunk_size=17)
            streamed = b"".join(
                iter_file_chunks(tiny_spec, file_id, has_footer, chunk_size=17)
            )
            assert streamed == out_path.read_bytes()
