import logging
import struct
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import fsspec
import numpy as np

from asml.hmi.DatasetSchemas.bronze.defect_binary_formats import (
    CURRENT_SCHEMA_VERSION,
    DEFECT_STRUCT_SIZE,
    NUM_PATCH_IMAGES_PER_DEFECT,
    PycDefectBase,
    get_binary_format,
    get_imgsize_from_flag,
)
from asml.hmi.DatasetSchemas.bronze.defect_models import (
    DefectPatchImages,
    DefectRecord,
    DefectWithImages,
)
from asml.hmi.DatasetSchemas.bronze.parse_results import (
    DdfFile,
    DdfParseResult,
)
from asml.hmi.DatasetSchemas.defect_enums import DefectFileExtension

from .base_parser import BaseParser
from .defect_parser_util import create_parse_metadata

# Create logger for this module
logger = logging.getLogger(__name__)

class DdfFileParser(BaseParser[DdfParseResult]):
    """Parser for .ddf (DefectDataFile) binary format.

    Parses `.ddf` files with a 1024-byte header, one `PycDefectBase`
    struct per defect, and three patch images per defect.
    """

    has_binary_images: ClassVar[bool] = True
    is_multi_row: ClassVar[bool] = True
    file_extensions: ClassVar[tuple[str, ...]] = (".ddf",)

    def _read_header(self, file_obj) -> tuple[int, int, int, int, int]:
        header_bytes = file_obj.read(1024)
        if len(header_bytes) < 88:
            raise ValueError(
                f"Invalid .ddf file header: expected at least 88 bytes, got {len(header_bytes)}"
            )

        flag = struct.unpack("<I", header_bytes[0:4])[0]
        version = struct.unpack("<I", header_bytes[4:8])[0]

        try:
            imgsize = get_imgsize_from_flag(flag)
        except ValueError as e:
            raise ValueError(f"Invalid .ddf file flag: {e}") from e

        defect_count = struct.unpack("<q", header_bytes[80:88])[0]
        defect_position = struct.unpack("<q", header_bytes[72:80])[0]

        logger.info(
            f"Header parsed: flag={flag:#x}, version={version}, "
            f"imgsize={imgsize}, defect_count={defect_count}, "
            f"defect_position={defect_position}"
        )

        return flag, version, imgsize, defect_count, defect_position

    def _get_defect_schema(self, version: int):
        try:
            defect_schema = get_binary_format(version, "defect")
            logger.debug(f"Using schema version {version} for parsing")
            return defect_schema
        except ValueError:
            logger.warning(
                f"Unknown schema version {version}, falling back to version {CURRENT_SCHEMA_VERSION}"
            )
            return get_binary_format(CURRENT_SCHEMA_VERSION, "defect")

    @staticmethod
    def _to_defect_record(parsed_defect: Any) -> DefectRecord:
        return DefectRecord(
            i_version=parsed_defect.iVersion,
            i_defect_id=parsed_defect.iDefectID,
            i_i_id=parsed_defect.iIID,
            i_column=parsed_defect.iColumn,
            i_die_index=parsed_defect.iDieIndex,
            i_die_x=parsed_defect.iDieX,
            i_die_y=parsed_defect.iDieY,
            i_flag=parsed_defect.iFlag,
            i_selected=parsed_defect.iSelected,
            i_i_id1=parsed_defect.iIID1,
            i_i_id2=parsed_defect.iIID2,
            i_temp_flag=parsed_defect.iTempFlag,
            img_x=parsed_defect.imgX,
            img_y=parsed_defect.imgY,
            img_x_size=parsed_defect.imgXSize,
            img_y_size=parsed_defect.imgYSize,
            f_x=parsed_defect.fX,
            f_y=parsed_defect.fY,
            f_x_size=parsed_defect.fXSize,
            f_y_size=parsed_defect.fYSize,
            threshold=parsed_defect.threshold,
            strength1=parsed_defect.strength1,
            strength2=parsed_defect.strength2,
            sigma1=parsed_defect.sigma1,
            sigma2=parsed_defect.sigma2,
            i_type=list(parsed_defect.iType),
            f_strength=list(parsed_defect.fStrength),
            i_feature=parsed_defect.iFeature,
            f_feature=list(parsed_defect.fFeature),
            i_feature_selected=list(parsed_defect.iFeatureSelected),
            f_d_feature=list(parsed_defect.fDFeature),
            d_create_time=parsed_defect.dCreateTime,
            d_modify_time=parsed_defect.dModifyTime,
            f_reserve=list(parsed_defect.fReserve),
        )
    
    def parse_metadata(self) -> list[DefectRecord]:
        """Extract defect metadata without loading patch images.

        Returns
        -------
        list[DefectRecord]
            One DefectRecord per defect in the file.
        """
        logger.info(f"Parsing .ddf metadata: {self.file_path}")

        with self.fs.open(self.file_path, "rb") as f:
            _, version, imgsize, defect_count, defect_position = self._read_header(f)
            defect_schema = self._get_defect_schema(version)
            defect_size = DEFECT_STRUCT_SIZE + imgsize * imgsize * NUM_PATCH_IMAGES_PER_DEFECT

            defect_records: list[DefectRecord] = []
            for x in range(defect_count):
                locseek = defect_position + defect_size * x
                f.seek(locseek)
                defect_bytes = f.read(DEFECT_STRUCT_SIZE)
                parsed_defect = defect_schema.parse(defect_bytes)
                defect_records.append(self._to_defect_record(parsed_defect))

        return defect_records

    def parse(self, **kwargs) -> DdfParseResult:
        """Parse .ddf binary file using iddf.py logic.

        Returns
        -------
        DdfParseResult
            Validated parse result containing DdfFile and metadata.

        Raises
        ------
        ValueError
            If file format is invalid or magic flag is unrecognized.

        Notes
        -----
        The binary layout follows `iddf.py`. The flag is stored at offset 0,
        the version at offset 4, the defect position at offset 72, and the
        defect count at offset 80. Each defect record then contains a
        1024-byte `PycDefectBase` struct followed by raw, reference, and mask
        patches of size `imgsize^2`.
        """
        logger.info(f"Parsing .ddf file: {self.file_path}")

        with self.fs.open(self.file_path, "rb") as f:
            flag, version, imgsize, defect_count, defect_position = self._read_header(f)
            defect_schema = self._get_defect_schema(version)

            # Parse each defect
            # Source: iddf.py lines 263-378
            defects = []
            defect_size = DEFECT_STRUCT_SIZE + imgsize * imgsize * NUM_PATCH_IMAGES_PER_DEFECT

            for x in range(defect_count):
                # Seek to defect location
                # Source: iddf.py lines 267-268
                locseek = defect_position + defect_size * x
                f.seek(locseek)

                # Read 1024-byte PycDefectBase struct
                # Source: iddf.py line 270
                defect_bytes = f.read(DEFECT_STRUCT_SIZE)

                parsed_defect = defect_schema.parse(defect_bytes)
                defect_record = self._to_defect_record(parsed_defect)

                # Create DefectPatchImages with pointer metadata only (no actual image data)
                # Images will be loaded lazily during Hugging Face dataset creation
                images = DefectPatchImages(
                    imgsize=imgsize,
                    defect_index=x,
                    source_file=Path(self.file_path),
                    file_format=DefectFileExtension.DDF.value,
                    ddf_defect_byte_offset=defect_position,
                    ddf_flag=flag,
                    ddf_schema_version=version,
                )

                # Create DefectWithImages
                defect_with_images = DefectWithImages(
                    defect=defect_record,
                    images=images,
                    imgsize=imgsize,
                )

                defects.append(defect_with_images)

        # Create validated DdfFile model
        ddf_file = DdfFile(
            flag=flag,
            version=version,
            defect_position=defect_position,
            defect_count=defect_count,
            imgsize=imgsize,
            defects=defects,
            source_file=Path(self.file_path),
        )

        metadata = create_parse_metadata(self.file_path, DefectFileExtension.DDF.value, fs=self.fs)

        logger.info(f"Successfully parsed .ddf: {ddf_file.defect_count} defects")

        return DdfParseResult(ddf_file=ddf_file, metadata=metadata)

    def stream_pixels(
        self, indices: np.ndarray | list[int] | None = None, **kwargs
    ) -> Iterator[np.ndarray]:
        """Yield patch image triplets as `(3, H, W)` arrays.

        Parameters
        ----------
        indices : array-like or None
            If provided, yield only the patches at the given defect indices.

        Yields
        ------
        np.ndarray
            Array of shape ``(3, imgsize, imgsize)`` (raw, ref, mask).
        """
        imgsize = kwargs.get("imgsize")
        defect_position = kwargs.get("defect_position")
        defect_count = kwargs.get("defect_count")

        with self.fs.open(self.file_path, "rb") as f:
            if not all(isinstance(value, int) for value in (imgsize, defect_position, defect_count)):
                _, _, imgsize_read, defect_count_read, defect_position_read = self._read_header(f)
                imgsize = imgsize_read
                defect_count = defect_count_read
                defect_position = defect_position_read

            if imgsize is None or defect_count is None or defect_position is None:
                raise ValueError("Failed to read .ddf header metadata required for pixel streaming.")

            iter_indices: range | list[int]
            iter_indices = list(indices) if indices is not None else range(defect_count)

            patch_bytes_per_defect = imgsize * imgsize * NUM_PATCH_IMAGES_PER_DEFECT
            defect_size = DEFECT_STRUCT_SIZE + patch_bytes_per_defect

            for idx in iter_indices:
                if idx < 0 or idx >= defect_count:
                    raise IndexError(f"Defect index {idx} out of range [0, {defect_count})")

                locseek = defect_position + defect_size * idx + DEFECT_STRUCT_SIZE
                f.seek(locseek)
                patch_bytes = f.read(patch_bytes_per_defect)
                if len(patch_bytes) != patch_bytes_per_defect:
                    raise ValueError(
                        f"Unexpected EOF while reading defect {idx}: "
                        f"expected {patch_bytes_per_defect} bytes, got {len(patch_bytes)}"
                    )

                patches = np.frombuffer(patch_bytes, dtype=np.uint8).reshape(
                    NUM_PATCH_IMAGES_PER_DEFECT, imgsize, imgsize
                )
                yield patches

    @classmethod
    def write(
        cls,
        file_path: Path | str,
        parse_result: DdfParseResult,
        *,
        patch_images: list[np.ndarray] | None = None,
        **kwargs,
    ) -> None:
        """Write a DdfParseResult back to .ddf binary format.

        Parameters
        ----------
        file_path : Path or str
            Target file path.
        parse_result : DdfParseResult
            Parse result to write.
        patch_images : list[np.ndarray] | None
            Per-defect patch arrays of shape ``(3, imgsize, imgsize)``.
            When ``None``, zero-filled patches are written.
        """
        ddf = parse_result.ddf_file
        header = bytearray(1024)
        struct.pack_into("<I", header, 0, ddf.flag)
        struct.pack_into("<I", header, 4, ddf.version)
        struct.pack_into("<q", header, 72, ddf.defect_position)
        struct.pack_into("<q", header, 80, ddf.defect_count)

        write_fs = kwargs.get("fs") or fsspec.filesystem("file")
        with write_fs.open(str(file_path), "wb") as f:
            f.write(header)
            f.seek(ddf.defect_position)

            for i, dw in enumerate(ddf.defects):
                dr = dw.defect
                defect_data = PycDefectBase.build(
                    {
                        "iVersion": dr.i_version,
                        "iDefectID": dr.i_defect_id,
                        "iIID": dr.i_i_id,
                        "iColumn": dr.i_column,
                        "iDieIndex": dr.i_die_index,
                        "iDieX": dr.i_die_x,
                        "iDieY": dr.i_die_y,
                        "iFlag": dr.i_flag,
                        "iSelected": dr.i_selected,
                        "iIID1": dr.i_i_id1,
                        "iIID2": dr.i_i_id2,
                        "iTempFlag": dr.i_temp_flag,
                        "imgX": dr.img_x,
                        "imgY": dr.img_y,
                        "imgXSize": dr.img_x_size,
                        "imgYSize": dr.img_y_size,
                        "fX": dr.f_x,
                        "fY": dr.f_y,
                        "fXSize": dr.f_x_size,
                        "fYSize": dr.f_y_size,
                        "threshold": dr.threshold,
                        "strength1": dr.strength1,
                        "strength2": dr.strength2,
                        "sigma1": dr.sigma1,
                        "sigma2": dr.sigma2,
                        "iType": dr.i_type,
                        "fStrength": dr.f_strength,
                        "iFeature": dr.i_feature,
                        "fFeature": dr.f_feature,
                        "iFeatureSelected": dr.i_feature_selected,
                        "fDFeature": dr.f_d_feature,
                        "dCreateTime": dr.d_create_time,
                        "dModifyTime": dr.d_modify_time,
                        "fReserve": dr.f_reserve,
                    }
                )
                f.write(defect_data)

                img_bytes_per_patch = ddf.imgsize * ddf.imgsize
                if patch_images is not None and i < len(patch_images):
                    for ch in range(3):
                        f.write(patch_images[i][ch].tobytes())
                else:
                    f.write(b"\x00" * img_bytes_per_patch * 3)