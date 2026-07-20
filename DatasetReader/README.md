# DatasetReader

Parses HMI proprietary file formats (`.raw`, `.ddf`, `.dzf`, `.defect`,
`.patch`) into Python objects and provides pipeline-specific orchestration for
combining parsed outputs.

## Key Modules

| Module            | Purpose                                           |
| ----------------- | ------------------------------------------------- |
| `file_dispatcher` | Routes files to appropriate parsers               |
| `parsers`         | Format-specific parser implementations            |
| `orchestrators`   | Pipeline-specific post-processing and composition |

**Dependencies**: `numpy`, `opencv`, `DatasetSchemas`

## Parsers

### Image Files

- **`RawFileParser`** — Extracts multi-channel image data and metadata from
  `.raw` HMI files; `parse()` returns a 3-D `(n_channels, H, W)` pixel stack
  alongside structured `RawImageMetadata`
- **`FileDispatcher`** — Processes directories, routing each file to its parser
  (currently `.raw` only; use `DefectResultManager` for defect files)

### Defect Files

- **`DzfFileParser`** — XML `.dzf` metadata (defect zones, test parameters,
  result file references)
- **`DdfFileParser`** — Binary `.ddf` defect records with PycDefectBase structs
  and patch image pointers
- **`DefectFileParser`** — Protobuf `.defect` records with embedded patch
  images
- **`PatchFileParser`** — Binary `.patch` containing only patch images (no
  defect structs)
- **`DefectResultManager`** — Orchestrates parsing of defect file combinations:
  protobuf (`.defect` + optional `.patch`) or classic (`.dzf` +
  `.ddf`/`.patch`)

**Parsing priority**: `.defect` (protobuf) takes precedence over `.dzf`
(classic) when both exist.

**Image data strategy**: Parsers store pointer metadata (byte offsets, file
paths, dimensions) — not actual pixels — enabling lazy loading during dataset
creation.

### Consolidation

`defect_schema_consolidator.py` merges multi-source defect parse results into a
unified DataFrame via vectorized Polars operations with priority-based
`pl.coalesce()`.

## Orchestrators

Orchestrators coordinate multiple format parsers and apply pipeline-specific
post-processing to prepare data for training workflows.

### DL FE Pipeline

- **`dl_fe_orchestrator.py`** — Coordinates `.dzf` + `.ddf`/`.patch` parsing
  for DL FE training workflows
  - `load_dzf_with_labels()` — Parse `.dzf` and construct label dictionaries
    (review types, patch indices)
  - `separate_doi_nuisance()` — Partition defect IDs into DOI/nuisance lists
    based on ReviewType values
  - `resolve_patch_file()` — Resolve the `.ddf`/`.patch` path from a
    `DzfParseResult`; raises `FileNotFoundError` if the file is missing
  - `load_patch_images_batch()` — Batch-load 3-channel `[C, H, W]` patch images
    for a list of defect IDs; dispatches `.ddf`/`.patch` transparently;
    optional `normalize` flag converts uint8 → float `[0, 1]`
  - `load_patch_images_single()` — Single-defect variant of the above
  - `load_single_channel_batch()` — Batch-load defect-only (single-channel)
    `[1, H, W]` patches; falls back to channel 0 when the requested channel is
    not present
  - `cut_patch_to_target_size()` — Center-crop (if larger) or `cv2.resize` (if
    smaller) a `[C, H, W]` patch to a square target size
  - `extract_defect_box_from_feature()` — Decode the bounding box encoded in
    `DefectRecord.f_feature[103]` via 4-byte float reinterpretation; returns
    `[x0, y0, x1, y1]` or `[0, 0, 0, 0]` when invalid

### DL Detection Pipeline

- **`dl_detection_orchestrator.py`** — Coordinates `.dzf` + `.raw` parsing for
  DL Detection training workflows
  - `load_dzf_for_detection()` — Parse `.dzf` for defect metadata and
    coordinates
  - `create_image_filename_mapping()` — Build defect ID → image filename lookup
    for resolving which `.raw` file contains each defect
    - `is_mbi_mode()` — Detect MBI (multi-beam) vs SBI (single-beam)
      acquisition from the image filename
  - `adjust_position_for_mbi()` — Compute MBI-corrected `ImgPosX`/`ImgPosY`
    using `JobId`, image dimensions, and pixel sizes
  - `get_die_position()` — Convert pixel coordinates to die-relative
    coordinates (µm), with optional global/local footer offset application
  - `get_gds_position()` — Convert pixel coordinates to GDS coordinates
  - `get_wafer_position()` — Convert pixel coordinates to wafer coordinates
    (µm)
  - `create_image_mapping_dict()` — Build wildcard → actual filename mapping
    from a directory of `.raw` files
  - `load_raw_image()` — Convenience wrapper: parse a `.raw` file and return
    `(image_3d_uint8, RawImageMetadata)` where `image_3d_uint8` has shape
    `(n_channels, H, W)`
  - `load_ref_images()` — Load reference `.raw` images associated with a defect
    image using a caller-supplied stem → `[ref1, ref2]` mapping; returns 3-D
    arrays

**Design**: Orchestrators depend on parsers (one-directional), enabling future
extraction into a separate package if scope grows (e.g., data API, live API
integrations).

## File System Compatibility

All parsers use `fsspec` for unified local and cloud I/O (`s3://`, `az://`,
`gs://`).
