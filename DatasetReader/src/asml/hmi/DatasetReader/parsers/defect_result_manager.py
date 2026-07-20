from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from asml.hmi.DatasetSchemas.defect_enums import DefectFileExtension

from .ddf_parser import DdfFileParser
from .defect_parser import DefectFileParser
from .dzf_parser import DzfFileParser
from .patch_parser import PatchFileParser

if TYPE_CHECKING:
    from asml.hmi.DatasetSchemas.bronze.parse_results import (
        DdfParseResult,
        DefectParseResult,
        DzfParseResult,
        PatchParseResult,
    )

# Create logger for this module
logger = logging.getLogger(__name__)


# --- PARSE RESULTS CONTAINER ---


@dataclass(slots=True)
class DefectParseResults:
    """Container for all defect file parse results.

    This dataclass replaces the dictionary return type from DefectResultManager.run_pipeline()
    to provide type-safe access to parse results.

    Attributes
    ----------
    dzf_result : DzfParseResult | None
        Parsed .dzf file result (metadata only).
    ddf_result : DdfParseResult | None
        Parsed .ddf file result (struct + images).
    defect_result : DefectParseResult | None
        Parsed .defect file result (protobuf format).
    patch_result : PatchParseResult | None
        Parsed .patch file result (images only).

    Notes
    -----
    All fields are optional since different file format combinations
    will populate different subsets of results:
    - Classic format (.dzf + .ddf): dzf_result and ddf_result populated
    - Classic format (.dzf + .patch): dzf_result and patch_result populated
    - Protobuf format (.defect): defect_result populated
    - Protobuf + override (.defect + .patch): defect_result and patch_result populated

    Examples
    --------
    >>> results = manager.run_pipeline()
    >>> if results.defect_result:
    ...     print(f"Parsed {results.defect_result.defect_file.defect_count} defects")
    """

    dzf_result: DzfParseResult | None = None
    ddf_result: DdfParseResult | None = None
    defect_result: DefectParseResult | None = None
    patch_result: PatchParseResult | None = None


class DefectResultManager:
    """Orchestrates discovery and parsing of defect files.

    This manager handles the complete workflow from file discovery to
    parsed output generation, supporting multiple defect file formats.

    Parameters
    ----------
    input_dir : Path
        Directory to scan for defect files.
    output_dir : Path | None
        Directory for output files. Defaults to input_dir if not specified.

    Attributes
    ----------
    input_dir : Path
        Resolved input directory path.
    output_dir : Path
        Resolved output directory path.
    """

    def __init__(self, input_dir: Path, output_dir: Path | None = None):
        """Initialize the defect result manager.

        Parameters
        ----------
        input_dir : Path
            Directory to scan for defect files.
        output_dir : Path | None
            Directory for output files. Created if doesn't exist.

        Raises
        ------
        FileNotFoundError
            If input directory does not exist.
        """
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve() if output_dir else self.input_dir

        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.input_dir}")

        # Create output directory if it doesn't exist
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output directory: {self.output_dir}")

    def discover(self) -> dict[str, Path]:
        """Scan input directory for defect files.

        Returns
        -------
        dict[str, Path]
            Mapping of file extensions to file paths.
            Keys are lowercase extensions ('.dzf', '.ddf', '.defect', '.patch').

        Raises
        ------
        ValueError
            If invalid file combinations are found.

        Notes
        -----
        Valid file combinations:
        - 1 .dzf + 1 .ddf (classic format)
        - 1 .dzf + 1 .patch (classic format)
        - 1 .dzf + 1 .ddf + 1 .patch (classic format with both)
        - 1 .defect (protobuf format, standalone)
        - 1 .defect + 1 .patch (protobuf format with patch override)
        - 1 .dzf + 1 .defect (both formats, priority handled by caller)
        - 1 .dzf + 1 .ddf + 1 .defect (all formats, priority handled by caller)
        """
        # Collect all files by extension
        dzf_files = []
        ddf_files = []
        defect_files = []
        patch_files = []

        for file_path in self.input_dir.iterdir():
            if file_path.is_file():
                ext = file_path.suffix.lower()
                if ext == DefectFileExtension.DZF.value:
                    dzf_files.append(file_path)
                elif ext == DefectFileExtension.DDF.value:
                    ddf_files.append(file_path)
                elif ext == DefectFileExtension.DEFECT.value:
                    defect_files.append(file_path)
                elif ext == DefectFileExtension.PATCH.value:
                    patch_files.append(file_path)

        # Determine format: classic (.dzf + .ddf/.patch) and/or protobuf (.defect)
        has_dzf = len(dzf_files) > 0
        has_defect = len(defect_files) > 0

        # Initialize result dictionary
        files = {}

        # CASE 1: Classic format (.dzf + .ddf/.patch)
        if has_dzf:
            # Validate exactly 1 .dzf file
            if len(dzf_files) > 1:
                file_names = [f.name for f in dzf_files]
                raise ValueError(
                    f"Multiple .dzf files found in {self.input_dir}: {file_names}. "
                    "Exactly 1 .dzf file is required."
                )

            # Validate at most 1 .ddf file
            if len(ddf_files) > 1:
                file_names = [f.name for f in ddf_files]
                raise ValueError(
                    f"Multiple .ddf files found in {self.input_dir}: {file_names}. "
                    "At most 1 .ddf file is allowed."
                )

            # Validate at most 1 .patch file
            if len(patch_files) > 1:
                file_names = [f.name for f in patch_files]
                raise ValueError(
                    f"Multiple .patch files found in {self.input_dir}: {file_names}. "
                    "At most 1 .patch file is allowed."
                )

            # Validate at least one of .ddf or .patch exists
            if len(ddf_files) == 0 and len(patch_files) == 0:
                raise ValueError(
                    f"No .ddf or .patch file found in {self.input_dir}. "
                    "Classic format requires at least one of (.ddf, .patch)."
                )

            # Add classic format files to result dictionary
            files[DefectFileExtension.DZF.value] = dzf_files[0]

            if len(ddf_files) > 0:
                files[DefectFileExtension.DDF.value] = ddf_files[0]

            if len(patch_files) > 0:
                files[DefectFileExtension.PATCH.value] = patch_files[0]

        # CASE 2: Protobuf format (.defect + optional .patch)
        if has_defect:
            # Validate exactly 1 .defect file
            if len(defect_files) > 1:
                file_names = [f.name for f in defect_files]
                raise ValueError(
                    f"Multiple .defect files found in {self.input_dir}: {file_names}. "
                    "Exactly 1 .defect file is required."
                )

            # Validate at most 1 .patch file
            if len(patch_files) > 1:
                file_names = [f.name for f in patch_files]
                raise ValueError(
                    f"Multiple .patch files found in {self.input_dir}: {file_names}. "
                    "At most 1 .patch file is allowed."
                )

            # Validate no .ddf files (not compatible with protobuf format)
            if len(ddf_files) > 0 and not has_dzf:
                # Only raise error if no .dzf file exists (pure protobuf format)
                file_names = [f.name for f in ddf_files]
                raise ValueError(
                    f".ddf file found with .defect file in {self.input_dir}: {file_names}. "
                    "Protobuf format (.defect) is not compatible with .ddf files without .dzf. "
                    "Use either (.dzf + .ddf) OR (.defect) format."
                )

            # Add protobuf format files to result dictionary
            files[DefectFileExtension.DEFECT.value] = defect_files[0]

            # Only add .patch to protobuf format if not already added by classic format
            if len(patch_files) > 0 and DefectFileExtension.PATCH.value not in files:
                files[DefectFileExtension.PATCH.value] = patch_files[0]

        # CASE 3: No valid format found
        if not has_dzf and not has_defect:
            raise ValueError(
                f"No valid defect files found in {self.input_dir}. "
                "Supported formats: "
                "Classic (.dzf + .ddf/.patch) OR Protobuf (.defect + optional .patch)."
            )

        # Log discovered files
        file_list = ", ".join([f"{ext}={path.name}" for ext, path in files.items()])
        logger.info(f"Discovered valid defect files in {self.input_dir}: {file_list}")

        return files

    def run_pipeline(self) -> DefectParseResults:
        """Execute the parsing pipeline and return parse results.

        # TODO: Future refactoring - Implement streaming parsing strategy
        # Current implementation uses "parse all at once" approach which loads
        # entire file contents into memory. For large defect files (e.g., >10k defects
        # or >100MB file size), consider refactoring to streaming/incremental parsing:
        # - Process defects in batches/chunks to reduce memory footprint
        # - Yield results incrementally instead of loading all at once
        # - Add memory usage monitoring to trigger streaming mode automatically
        # - Expected performance gain: 50-70% memory reduction for large files
        # Evaluation criteria: Monitor file sizes and defect counts in production
        # to determine when streaming becomes necessary.

        Pipeline supports two formats:

        **Classic Format (.dzf + .ddf/.patch):**
        1. Discover and validate files
        2. Parse .dzf file (metadata)
        3. Parse .ddf file (defect struct + pointer metadata) or .patch file (pointer metadata only)

        **Protobuf Format (.defect + optional .patch):**
        1. Discover and validate files
        2. Parse .defect file (defect struct + pointer metadata)
        3. Optionally parse .patch file to override embedded image pointers

        All parsers now use lazy loading architecture, storing only pointer metadata
        for patch images. Actual image data will be loaded on-demand during
        Hugging Face dataset creation or other downstream processing.

        Returns
        -------
        DefectParseResults
            Container with parse results for each format:
            - dzf_result: DzfParseResult or None
            - ddf_result: DdfParseResult or None
            - defect_result: DefectParseResult or None
            - patch_result: PatchParseResult or None

        Raises
        ------
        ValueError
            If invalid file combinations are found.
        """
        # Stage 1: Discover and validate files
        files = self.discover()

        # Initialize parse result holders for consolidation
        dzf_result: DzfParseResult | None = None
        ddf_result: DdfParseResult | None = None
        defect_result: DefectParseResult | None = None
        patch_result: PatchParseResult | None = None

        # Stage 2: Determine format type
        has_dzf = DefectFileExtension.DZF.value in files
        has_defect = DefectFileExtension.DEFECT.value in files

        # PARSING PRIORITY LOGIC:
        # 1. If .defect file exists -> Use protobuf format parsing (CASE 1)
        # 2. If .defect file does NOT exist -> Fall back to classic format parsing with .dzf file (CASE 2)

        # CASE 1: Protobuf format (.defect + optional .patch)
        # Handles: {.defect, .patch} or {.defect}
        if has_defect:
            # Stage 3: Parse .defect file (pointer metadata only, lazy loading)
            # TODO: Streaming parsing - For large .defect files, implement streaming
            # parser that yields defects incrementally instead of loading all at once.
            # Consider threshold: file_size > 100MB or defect_count > 10000.
            defect_result = DefectFileParser(files[DefectFileExtension.DEFECT.value]).parse()
            logger.info(f"Parsed DEFECT file: {defect_result.defect_file.defect_count} defects")

            # Stage 3: Optionally parse .patch file to override embedded images
            patch_result = None
            if DefectFileExtension.PATCH.value in files:
                # TODO: Streaming parsing - For large .patch files, implement streaming
                # parser to process patch images in batches. Coordinate with .defect streaming.
                patch_result = PatchFileParser(files[DefectFileExtension.PATCH.value]).parse()
                logger.info(f"Parsed PATCH file: {patch_result.patch_file.defect_count} defects")

                # Override embedded images with .patch images
                if patch_result.patch_file.defect_count == defect_result.defect_file.defect_count:
                    for defect_idx in range(defect_result.defect_file.defect_count):
                        defect_with_images = defect_result.defect_file.defects[defect_idx]
                        patch_images = patch_result.patch_file.defects[defect_idx]

                        # Replace images by assigning the entire DefectPatchImages wrapper
                        defect_with_images.images = patch_images
                        defect_with_images.imgsize = patch_images.imgsize

                    logger.info(
                        f"Replaced {defect_result.defect_file.defect_count} embedded images "
                        "with images from .patch file (REQUIREMENT 4, behavior 2)"
                    )
                else:
                    logger.warning(
                        f"Defect count mismatch: .defect has {defect_result.defect_file.defect_count} defects, "
                        f".patch has {patch_result.patch_file.defect_count} defects. "
                        "Skipping image replacement."
                    )

            # Summary for protobuf format
            if patch_result is not None:
                logger.info(
                    f"Protobuf format parsing completed: {defect_result.defect_file.defect_count} defects "
                    f"from .defect with .patch image override"
                )
            else:
                logger.info(
                    f"Protobuf format parsing completed: {defect_result.defect_file.defect_count} defects "
                    f"from .defect with embedded images"
                )

        # CASE 2: Classic format (.dzf + .ddf/.patch)
        # Handles: {.dzf, .ddf} or {.dzf, .patch}
        # Note: .patch takes priority over .ddf (handled by .dzf result_file_name resolution)
        elif has_dzf:
            # Stage 3: Parse .dzf file
            # TODO: Streaming parsing - For large .dzf files (large XML with many defects),
            # implement streaming XML parser (e.g., iterparse) to process defects incrementally.
            dzf_result = DzfFileParser(files[DefectFileExtension.DZF.value]).parse()

            # Stage 4: Parse result file based on extension (.ddf or .patch)
            ddf_result = None
            patch_result = None
            result_file_ext = Path(dzf_result.result_file_name).suffix.lower()

            if result_file_ext == DefectFileExtension.DDF.value:
                # Parse .ddf file
                if DefectFileExtension.DDF.value in files:
                    # TODO: Streaming parsing - For large .ddf files, implement streaming
                    # parser to read defect structures in chunks (e.g., 1000 defects at a time).
                    ddf_result = DdfFileParser(files[DefectFileExtension.DDF.value]).parse()
                    logger.info(f"Parsed DDF file: {ddf_result.ddf_file.defect_count} defects")
                else:
                    logger.warning(f"DZF references .ddf file but no .ddf found in {self.input_dir}")
            elif result_file_ext == DefectFileExtension.PATCH.value:
                # Parse .patch file
                if DefectFileExtension.PATCH.value in files:
                    # TODO: Streaming parsing - For large .patch files, implement streaming
                    # parser to process patch images in batches. Coordinate with .dzf streaming.
                    patch_result = PatchFileParser(files[DefectFileExtension.PATCH.value]).parse()
                    logger.info(f"Parsed PATCH file: {patch_result.patch_file.defect_count} defects")
                else:
                    logger.warning(
                        f"DZF references .patch file but no .patch found in {self.input_dir}"
                    )
            else:
                logger.info(
                    f"DZF references {result_file_ext} file (not .ddf or .patch), skipping result file parsing"
                )

            # Summary for classic format
            if ddf_result is not None:
                logger.info(
                    f"Classic format parsing completed: {ddf_result.ddf_file.defect_count} defects "
                    f"from .ddf and {len(dzf_result.parsed_defects)} defects from .dzf"
                )
            elif patch_result is not None:
                logger.info(
                    f"Classic format parsing completed: {patch_result.patch_file.defect_count} defects "
                    f"from .patch and {len(dzf_result.parsed_defects)} defects from .dzf"
                )
            else:
                logger.info(
                    f"Classic format parsing completed: {len(dzf_result.parsed_defects)} defects from .dzf"
                )

        else:
            raise ValueError("Invalid file combination: no .dzf or .defect file found")

        # Return parse results
        logger.info("Pipeline completed: returning parse results")
        return DefectParseResults(
            dzf_result=dzf_result,
            ddf_result=ddf_result,
            defect_result=defect_result,
            patch_result=patch_result,
        )
