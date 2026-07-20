import logging
import struct
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar

import fsspec
import numpy as np

from asml.hmi.DatasetSchemas.bronze.defect_models import DefectPatchImages
from asml.hmi.DatasetSchemas.bronze.parse_results import (
    PatchFile,
    PatchParseResult,
)
from asml.hmi.DatasetSchemas.defect_enums import DefectFileExtension

from .base_parser import BaseParser
from .defect_parser_util import create_parse_metadata

# Create logger for this module
logger = logging.getLogger(__name__)

class PatchFileParser(BaseParser[PatchParseResult]):
    """Parser for .patch (PatchImageFile) binary format.

    Parses `.patch` files with a 15-byte header followed by patch-image data
    for each defect.
    """

    has_binary_images: ClassVar[bool] = True
    is_multi_row: ClassVar[bool] = True
    file_extensions: ClassVar[tuple[str, ...]] = (".patch",)

    def _read_header(self) -> tuple[int, int, int, int, int]:
        with self.fs.open(self.file_path, "rb") as f:
            header_bytes = f.read(15)
            if len(header_bytes) < 15:
                raise ValueError(
                    f"Invalid .patch file header: expected at least 15 bytes, got {len(header_bytes)}"
                )
        
        header_size = struct.unpack("<H", header_bytes[2:4])[0]
        data_offset = struct.unpack("<I", header_bytes[4:8])[0]
        defect_count = struct.unpack("<I", header_bytes[8:12])[0]
        channels = header_bytes[12]
        imgsize = struct.unpack("<H", header_bytes[13:15])[0]
        return header_size, data_offset, defect_count, channels, imgsize

    def _build_patch_metadata(
        self,
        defect_count: int,
        data_offset: int,
        channels: int,
        imgsize: int,
    ) -> list[DefectPatchImages]:
        return [
            DefectPatchImages(
                imgsize=imgsize,
                defect_index=defect_idx,
                source_file=Path(self.file_path),
                file_format=DefectFileExtension.PATCH.value,
                patch_defect_byte_offset=data_offset,
                num_channels_in_patch=channels,
            )
            for defect_idx in range(defect_count)
        ]

    def parse_metadata(self) -> list[DefectPatchImages]:
        """Extract defect patch metadata without loading patch pixels.

        Returns
        -------
        list[DefectPatchImages]
            One DefectPatchImages per defect in the file.
        """
        _, data_offset, defect_count, channels, imgsize = self._read_header()
        return self._build_patch_metadata(defect_count, data_offset, channels, imgsize)

    def parse(self, **kwargs) -> PatchParseResult:
        """Parse .patch binary file using pbddf.readPatches() logic.

        Returns
        -------
        PatchParseResult
            Validated parse result containing PatchFile and metadata.

        Raises
        ------
        ValueError
            If file format is invalid.

        Notes
        -----
        The binary layout follows `pbddf.py`. The header stores the header
        size, data offset, defect count, channel count, and image size. Each
        defect contributes three sequential channels representing the raw,
        reference, and mask patches.
        """
        logger.info(f"Parsing .patch file: {self.file_path}")

        # Read patch file header only (without loading actual image data)
        # We only need metadata for pointer-based lazy loading
        header_size, data_offset, defect_count, channels, imgsize = self._read_header()

        logger.info(
            f"Read .patch header: {defect_count} defects, "
            f"{channels} channels, {imgsize} x {imgsize} images"
        )

        # Create DefectPatchImages objects with pointer metadata only (no actual image data)
        # Images will be loaded lazily during Hugging Face dataset creation
        defect_patch_images_list = self._build_patch_metadata(
            defect_count, data_offset, channels, imgsize
        )

        # Create validated PatchFile model
        patch_file = PatchFile(
            header_size=header_size,
            data_offset=data_offset,
            defect_count=defect_count,
            channels=channels,
            imgsize=imgsize,
            defects=defect_patch_images_list,
            source_file=Path(self.file_path),
        )

        metadata = create_parse_metadata(self.file_path, DefectFileExtension.PATCH.value, fs=self.fs)

        logger.info(f"Successfully parsed .patch: {patch_file.defect_count} defects")

        return PatchParseResult(patch_file=patch_file, metadata=metadata)

    def stream_pixels(
        self, indices: np.ndarray | list[int] | None = None, **kwargs
    ) -> Iterator[np.ndarray]:
        """Yield patch image triplets as `(channels, H, W)` arrays.

        Parameters
        ----------
        indices : array-like or None
            If provided, yield only the patches at the given defect indices.

        Yields
        ------
        np.ndarray
            Array of shape `(channels, imgsize, imgsize)`.
        """
        metadata = kwargs.get("metadata")
        if (
            isinstance(metadata, list)
            and metadata
            and all(isinstance(item, DefectPatchImages) for item in metadata)
        ):
            defect_metadata = metadata
            first = defect_metadata[0]
            data_offset = first.patch_defect_byte_offset or 0
            channels = first.num_channels_in_patch or 3
            imgsize = first.imgsize
            defect_count = len(defect_metadata)
        else:
            _, data_offset, defect_count, channels, imgsize = self._read_header()

        iter_indices: range | list[int]
        iter_indices = list(indices) if indices is not None else range(defect_count)

        patch_size = imgsize * imgsize
        defect_size = patch_size * channels

        with self.fs.open(self.file_path, "rb") as f:
            for idx in iter_indices:
                if idx < 0 or idx >= defect_count:
                    raise IndexError(f"Defect index {idx} out of range [0, {defect_count})")

                offset = data_offset + defect_size * idx
                f.seek(offset)
                data = f.read(defect_size)
                if len(data) != defect_size:
                    raise ValueError(
                        f"Unexpected EOF while reading defect {idx}: "
                        f"expected {defect_size} bytes, got {len(data)}"
                    )

                patches = np.frombuffer(data, dtype=np.uint8).reshape(channels, imgsize, imgsize)
                yield patches

    @classmethod
    def write(
        cls,
        file_path: Path | str,
        parse_result: PatchParseResult,
        *,
        patch_images: list[np.ndarray] | None = None,
        **kwargs,
    ) -> None:
        """Write a PatchParseResult back to .patch binary format.

        Parameters
        ----------
        file_path : Path or str
           Target file path.
        parse_result : PatchParseResult
            Parse result to write.
        patch_images : list[np.ndarray] | None
            Per-defect patch arrays of shape ``(channels, imgsize, imgsize)``.
            When ``None``, zero-filled patches are written.
        """
        pf = parse_result.patch_file
        header = bytearray(15)
        header[0:2] = b"\x50\x41"
        struct.pack_into("<H", header, 2, pf.header_size)
        struct.pack_into("<I", header, 4, pf.data_offset)
        struct.pack_into("<I", header, 8, pf.defect_count)
        header[12] = pf.channels
        struct.pack_into("<H", header, 13, pf.imgsize)

        bytes_per_defect = pf.channels * pf.imgsize * pf.imgsize

        write_fs = kwargs.get("fs") or fsspec.filesystem("file")
        with write_fs.open(str(file_path), "wb") as f:
            f.write(header)
            # Pad to data_offset if needed
            current = len(header)
            if pf.data_offset > current:
                f.write(b"\x00" * (pf.data_offset - current))

            for i in range(pf.defect_count):
                if patch_images is not None and i < len(patch_images):
                    f.write(patch_images[i].tobytes())
                else:
                    f.write(b"\x00" * bytes_per_defect)