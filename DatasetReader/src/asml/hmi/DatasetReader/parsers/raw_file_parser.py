import dataclasses
import json
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, ClassVar

import cv2
import fsspec
import numpy as np

from asml.hmi.DatasetSchemas.bronze.raw_file import RawImageMetadata


from .base_parser import BaseParser

# Create a logger instance at the top of module.
logger = logging.getLogger(__name__)

@dataclass(frozen=True, slots=True)
class RawParseResult:
    """Parse result for .raw (HMI raw image) files.

    Attributes
    ----------
    image_data : np.ndarray
        Per-channel pixel data as a 3-D numpy array of shape
        ``(n_channels, H, W)``, with scan-direction normalisation applied.
    metadata : RawImageMetadata
        Structured metadata extracted from the file.
    file_path : Path
        Source file path.
	img_width : int
        Image width in pixels (post scan-dir normalisation).
    img_height : int
        Image height in pixels (post scan-dir normalisation).
    channel_byte_offsets : list[int]
        Absolute byte offsets in the .raw file where each channel's pixel
        block begins.  ``len(channel_byte_offsets) == n_channels``.
    """
	
	image_data: np.ndarray
    metadata: RawImageMetadata
    file_path: Path
    img_width: int
    img_height: int
    channel_byte_offsets: list[int]
	
	def __post_init__(self) -> None:
        """Validate that channel counts are consistent across all fields."""
        n_ch = self.image_data.shape[0]
        n_meta = len(self.metadata.Channels)
        n_off = len(self.channel_byte_offsets)
        if n_ch != n_meta or n_meta != n_off:
			raise ValueError(
                f"Channel count mismatch: image_data has {n_ch} channels, "
                f"metadata.Channels has {n_meta}, "
                f"channel_byte_offsets has {n_off}."
            )
			
			
# Define a custom encoder class
class NumpyEncoder(json.JSONEncoder):
    """A custom JSON encoder for NumPy's data types."""

    def default(self, o):
        """Convert NumPy values to JSON-serializable native Python objects."""
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        return super().default(o)
		

class HeaderField(Enum):
    """Define the schema for the .raw file header."""

    IMG_WIDTH = (0, 4, np.int32)
    IMG_HEIGHT = (4, 8, np.int32)
    IMAGE_ID = (20, 24, np.int32)
    PIXEL_SIZE_X = (48, 52, np.float32)
    PIXEL_SIZE_Y = (52, 56, np.float32)
    IMG_POS_X = (56, 60, np.float32)
    IMG_POS_Y = (60, 64, np.float32)
    TEST_ID = (62 * 4, 63 * 4, np.int32)
    DIE_INDEX = (80, 84, np.int32)
    DIE_X = (84, 88, np.int32)
	DIE_Y = (88, 92, np.int32)
    SCAN_DIR = (92, 96, np.int32)
    DIE_POS_X = (96, 100, np.float32)
    DIE_POS_Y = (100, 104, np.float32)
    GDS_POS_X = (192, 196, np.int32)
    GDS_POS_Y = (196, 200, np.int32)
    CHANNEL_MASK = (17720, 17724, np.int32)
    IMG_SEGMENT = (8, 12, np.int32)
    JOB_ID = (28, 32, np.int32)


class FooterField(Enum):
    """Define the schema for the .raw file footer."""

    DIE_X = (20 * 4, 21 * 4, np.float32)
    DIE_Y = (21 * 4, 22 * 4, np.float32)
    GDS_POS_X = (28 * 4, 29 * 4, np.int32)
    GDS_POS_Y = (29 * 4, 30 * 4, np.int32)
    GLOBAL_OFFSET_X = (38 * 4, 39 * 4, np.float32)
    GLOBAL_OFFSET_Y = (39 * 4, 40 * 4, np.float32)
    BLOCK_SIZE_X = (44 * 4, 45 * 4, np.int32)
    BLOCK_SIZE_Y = (45 * 4, 46 * 4, np.int32)
	BLOCK_NUM_X = (46 * 4, 47 * 4, np.int32)
    BLOCK_NUM_Y = (47 * 4, 48 * 4, np.int32)
    LOCAL_OFFSET_X = (52 * 4, 4122 * 4, np.float32)
    LOCAL_OFFSET_Y = (4122 * 4, 8192 * 4, np.float32)
	
	
class ImageFormat(Enum):
    """Enumeration for supported output image file formats."""

    BMP = ".bmp"
    PNG = ".png"
	
class ScanDirection(IntEnum):
    """Enumeration for scan direction code in the raw file header."""

    NORTH = 0  # transpose + row-flip  (output is WxH)
    EAST = 1  # transpose + row-flip + col-flip  (output is WxH)
    SOUTH = 2  # identity — standard orientation, no transform
    WEST = 3  # row-flip only
	
# Define module-level constants here
IMAGE_DATA_START_OFFSET: int = 32768
RAW_FOOTER_SIZE_BYTES: int = FooterField.LOCAL_OFFSET_Y.value[1]

class RawFileParser(BaseParser[RawParseResult]):
    """Parser for .raw HMI image files.

    Parses .raw files containing multi-channel image data and metadata,
    returning a structured RawParseResult with processed image and metadata.
    """

    has_binary_images: ClassVar[bool] = True
    is_multi_row: ClassVar[bool] = False
    file_extensions: ClassVar[tuple[str, ...]] = (".raw",)
	
	def parse(
        self,
        verbose: bool = False,
        **kwargs,
    ) -> RawParseResult:
		"""Parse a .raw file and return the full result.

        Parameters
        ----------
        verbose : bool
            Enable verbose logging during parsing.

        Returns
        -------
        RawParseResult
            Structured result containing per-channel 3-D image data and metadata.
        """
		
		if verbose:
            logger.info("Reading and parsing HMI image: %s", self.file_path)

        with self.fs.open(self.file_path, "rb") as rp:
            header_bytes = rp.read(IMAGE_DATA_START_OFFSET)
            img_width, img_height, scan_dir, channels = _parse_header_data(header_bytes)

            pixel_data_size = img_width * img_height * len(channels)
            pixel_bytes = rp.read(pixel_data_size)
			
			imgs = _get_imgs_from_bytes(pixel_bytes, channels, img_width, img_height)
            stacked = np.stack(imgs)
            channel_byte_offsets = [
                IMAGE_DATA_START_OFFSET + i * img_width * img_height for i in range(len(channels))
            ]
            image_data, img_width, img_height, scan_dir = _normalize_scan_dir(stacked, scan_dir)
			
			metadata = _build_metadata(
                header_bytes=header_bytes,
                reader=rp,
                pixel_data_size=pixel_data_size,
                skip_pixel_bytes=False,
                file_path=Path(self.file_path),
                img_width=img_width,
                img_height=img_height,
                scan_dir=scan_dir,
                channels=channels,
                verbose=verbose,
            )
			
		return RawParseResult(
            image_data=image_data,
            metadata=metadata,
            file_path=Path(self.file_path),
            img_width=img_width,
            img_height=img_height,
            channel_byte_offsets=channel_byte_offsets,
        )
		
	def parse_metadata(self) -> list[RawImageMetadata]:
        """Extract metadata without reading image pixels.

        Returns
        -------
        list[RawImageMetadata]
            Single-element list with the extracted metadata.
        """
		
		with self.fs.open(self.file_path, "rb") as rp:
            header_bytes = rp.read(IMAGE_DATA_START_OFFSET)
            img_width, img_height, scan_dir, channels = _parse_header_data(header_bytes)
            pixel_data_size = img_width * img_height * len(channels)

            try:
                ScanDirection(scan_dir)
                normalized_scan_dir = ScanDirection.WEST.value
            except ValueError:
                normalized_scan_dir = scan_dir
				
			metadata = _build_metadata(
                header_bytes=header_bytes,
                reader=rp,
                pixel_data_size=pixel_data_size,
                skip_pixel_bytes=True,
                file_path=Path(self.file_path),
                img_width=img_width,
                img_height=img_height,
                scan_dir=normalized_scan_dir,
                channels=channels,
                verbose=False,
            )
			
		return [metadata]
		
	def stream_pixels(self, **kwargs) -> Iterator[np.ndarray]:
        """Yield the per-channel image stack as a single ndarray.

        Yields
        ------
        np.ndarray
            The per-channel pixel stack of shape ``(n_channels, H, W)``,
            with scan-direction normalisation applied.
        """
		with self.fs.open(self.file_path, "rb") as rp:
            header_bytes = rp.read(IMAGE_DATA_START_OFFSET)
            img_width, img_height, scan_dir, channels = _parse_header_data(header_bytes)

            pixel_data_size = img_width * img_height * len(channels)
            pixel_bytes = rp.read(pixel_data_size)
			
		imgs = _get_imgs_from_bytes(pixel_bytes, channels, img_width, img_height)
        stacked = np.stack(imgs)
        image_data, _, _, _ = _normalize_scan_dir(stacked, scan_dir)
        yield image_data
		
	def save_output(self, output_dir: Path | str, image_format: str = "png", **kwargs) -> None:
        """Save the processed image and metadata to the output directory.

        Parameters
        ----------
        output_dir : Path or str
            Directory where output files will be saved.
        image_format : str
            Image format extension without dot (e.g. ``"png"``, ``"bmp"``).
        """
		
		result = self.parse(**kwargs)
        output_dir_str = str(output_dir)

        if not self.fs.exists(output_dir_str):
            self.fs.makedirs(output_dir_str, exist_ok=True)

        def _join_output_path(base_dir: str, filename: str) -> str:
            if "://" in base_dir:
                return f"{base_dir.rstrip('/')}/{filename}"
            return str(Path(base_dir) / filename)
			
		base_name = Path(self.file_path).stem

        # Save each channel as a separate image file
        fmt_ext = f".{image_format}" if not image_format.startswith(".") else image_format
        for i, channel_num in enumerate(result.metadata.Channels):
            channel_img = result.image_data[i]
            image_filename = f"{base_name}_ch{channel_num}{fmt_ext}"
            image_output_path = _join_output_path(output_dir_str, image_filename)
			
			success, encoded_image = cv2.imencode(fmt_ext, channel_img)
            if not success:
                raise ValueError(
                    f"Failed to encode channel {channel_num} as '{fmt_ext}' for {self.file_path}"
                )

            with self.fs.open(image_output_path, "wb") as f:
                f.write(encoded_image.data)
            logger.info("Image channel %s successfully saved to %s", channel_num, image_output_path)
			
		# Save metadata JSON
        metadata_dict = dataclasses.asdict(result.metadata)
        metadata_filename = f"{base_name}.json"
        metadata_output_path = _join_output_path(output_dir_str, metadata_filename)
		
		try:
            with self.fs.open(metadata_output_path, "w") as f:
                json.dump(metadata_dict, f, cls=NumpyEncoder)
                f.write("\n")
            logger.info("Metadata saved to %s", metadata_output_path)
        except TypeError:
            logger.exception("Could not serialize metadata to JSON for %s.", metadata_filename)
			
	@classmethod
    def write(
        cls,
        file_path: Path | str,
        parse_result: RawParseResult,
        *,
        channel_images: list[np.ndarray] | None = None,
        **kwargs,
    ) -> None:
		"""Write a RawParseResult back to the proprietary .raw binary format.

        Parameters
        ----------
        file_path : Path or str
            Target file path.
        parse_result : RawParseResult
            Parse result to write.
		channel_images : list[np.ndarray] | None
            Per-channel image arrays. When provided, each array is written
            as a separate channel. When ``None``, the channel planes are
            taken from ``parse_result.image_data`` (iterating its first axis).
        """
		file_path = Path(file_path)
        metadata = parse_result.metadata

        header = bytearray(IMAGE_DATA_START_OFFSET)
        header[0:4] = np.int32(metadata.ImgWidth).tobytes()
        header[4:8] = np.int32(metadata.ImgHeight).tobytes()
        header[20:24] = np.int32(metadata.LegacyImageId or 0).tobytes()
        header[84:88] = np.int32(metadata.DieIdX or 0).tobytes()
		header[88:92] = np.int32(metadata.DieIdY or 0).tobytes()
        header[92:96] = np.int32(metadata.ScanDir).tobytes()

        channel_mask = sum(1 << (ch - 1) for ch in parse_result.metadata.Channels)
        header[17720:17724] = np.int32(channel_mask).tobytes()

        imgs = channel_images or list(parse_result.image_data)
        footer_bytes = _build_footer_bytes(metadata)
		
		write_fs = kwargs.get("fs") or fsspec.filesystem("file")
        with write_fs.open(str(file_path), "wb") as f:
            f.write(header)
            for img in imgs:
                f.write(img.tobytes())
            if footer_bytes is not None:
                f.write(footer_bytes)
				

# ---------------------------------------------------------------------------
# Module-level helper functions (stateless)
# ---------------------------------------------------------------------------


def _read_header_field(binary_data: bytes, field: HeaderField):
    """Read a single field from binary header data."""
    start, end, dtype = field.value
    return np.frombuffer(binary_data[start:end], dtype=dtype)[0]


def _read_footer_field(footer_bytes: bytes, field: FooterField, is_array: bool = False):
    """Read a field from footer bytes."""
    start_offset, end_offset, dtype = field.value
    buffer = np.frombuffer(footer_bytes[start_offset:end_offset], dtype=dtype)
    return buffer if is_array else buffer[0]
	
	
def _has_footer_metadata(metadata: RawImageMetadata) -> bool:
    """Return True when any footer field has a value to serialize."""
    return any(
        value is not None
        for value in (
            metadata.DieXFooter,
            metadata.DieYFooter,
            metadata.GdsPosXFooter,
            metadata.GdsPosYFooter,
            metadata.GlobalOffsetX,
            metadata.GlobalOffsetY,
			metadata.BlockSizeX,
            metadata.BlockSizeY,
            metadata.BlockNumX,
            metadata.BlockNumY,
            metadata.LocalOffsetX,
            metadata.LocalOffsetY,
        )
    )
	
	
def _write_footer_field(
    footer_bytes: bytearray,
    field: FooterField,
    value: int | float | list[float] | None,
) -> None:
	"""Write a scalar or array field into footer bytes when value is
    available.
    """
    if value is None:
        return
		
	start_offset, end_offset, dtype = field.value
    np_dtype = np.dtype(dtype)

    if isinstance(value, list):
        max_elements = (end_offset - start_offset) // np_dtype.itemsize
        encoded = np.asarray(value, dtype=np_dtype).ravel()[:max_elements].tobytes()
        footer_bytes[start_offset : start_offset + len(encoded)] = encoded
        return
		
	footer_bytes[start_offset:end_offset] = np.asarray([value], dtype=np_dtype).tobytes()


def _build_footer_bytes(metadata: RawImageMetadata) -> bytes | None:
    """Build serialized footer bytes from RawImageMetadata footer fields."""
    if not _has_footer_metadata(metadata):
        return None
		
	footer_bytes = bytearray(RAW_FOOTER_SIZE_BYTES)
    _write_footer_field(footer_bytes, FooterField.DIE_X, metadata.DieXFooter)
    _write_footer_field(footer_bytes, FooterField.DIE_Y, metadata.DieYFooter)
    _write_footer_field(footer_bytes, FooterField.GDS_POS_X, metadata.GdsPosXFooter)
    _write_footer_field(footer_bytes, FooterField.GDS_POS_Y, metadata.GdsPosYFooter)
	_write_footer_field(footer_bytes, FooterField.GLOBAL_OFFSET_X, metadata.GlobalOffsetX)
    _write_footer_field(footer_bytes, FooterField.GLOBAL_OFFSET_Y, metadata.GlobalOffsetY)
    _write_footer_field(footer_bytes, FooterField.BLOCK_SIZE_X, metadata.BlockSizeX)
    _write_footer_field(footer_bytes, FooterField.BLOCK_SIZE_Y, metadata.BlockSizeY)
    _write_footer_field(footer_bytes, FooterField.BLOCK_NUM_X, metadata.BlockNumX)
	_write_footer_field(footer_bytes, FooterField.BLOCK_NUM_Y, metadata.BlockNumY)
    _write_footer_field(footer_bytes, FooterField.LOCAL_OFFSET_X, metadata.LocalOffsetX)
    _write_footer_field(footer_bytes, FooterField.LOCAL_OFFSET_Y, metadata.LocalOffsetY)
    return bytes(footer_bytes)
	
def _get_channel_info(channel_mask: int) -> list[int]:
    """Decode a channel mask integer into a list of active channel numbers."""
    if channel_mask == 0:
        return [1]
    channels: list[int] = []
    channel_num = 1
    while channel_mask > 0:
		if channel_mask & 1:
            channels.append(channel_num)
        channel_mask >>= 1
        channel_num += 1
    return channels
	
def _parse_header_data(header_bytes: bytes) -> tuple[int, int, int, list[int]]:
    """Parse minimal header data for image extraction.

    Returns (img_width, img_height, scan_dir, channels).
    """
	img_width = _read_header_field(header_bytes, HeaderField.IMG_WIDTH)
    img_height = _read_header_field(header_bytes, HeaderField.IMG_HEIGHT)
    scan_dir = _read_header_field(header_bytes, HeaderField.SCAN_DIR)
    channel_mask = _read_header_field(header_bytes, HeaderField.CHANNEL_MASK)
    channels = _get_channel_info(channel_mask)
    return int(img_width), int(img_height), int(scan_dir), channels
	
	
# Dispatch table for scan-direction transforms (matches image_io.py reference).
# All transforms operate on (C, H, W) stacks using vectorised numpy slicing.
_SCAN_DIR_TRANSFORM: dict[ScanDirection, Any] = {
    ScanDirection.NORTH: lambda a: np.transpose(a, (0, 2, 1))[:, ::-1, :],
    ScanDirection.EAST: lambda a: np.transpose(a, (0, 2, 1))[:, ::-1, ::-1],
    ScanDirection.SOUTH: lambda a: a,
    ScanDirection.WEST: lambda a: a[:, ::-1, :],
}


def _normalize_scan_dir(arr: np.ndarray, scan_dir: int) -> tuple[np.ndarray, int, int, int]:
    """Adjust per-channel image stack orientation based on scan direction.

    Parameters
    ----------
    arr : np.ndarray
        Per-channel pixel stack of shape ``(C, H, W)``.
    scan_dir : int
        Scan direction code from the .raw header.

    Returns
	-------
    tuple[np.ndarray, int, int, int]
        ``(transformed_stack, new_width, new_height, normalized_scan_dir)``.

    For all valid scan directions the returned ``normalized_scan_dir`` is
    always ``ScanDirection.WEST.value`` (3), matching the reference terminal
    state in ``image_io.py``.  For unknown values the input is returned
    unchanged together with the original ``scan_dir``.
    """
	try:
        direction = ScanDirection(scan_dir)
    except ValueError:
        logger.warning("Unknown scanDir value '%s'. No rotation will be applied.", scan_dir)
        return arr, arr.shape[2], arr.shape[1], scan_dir

    new_arr = _SCAN_DIR_TRANSFORM[direction](arr)
    return new_arr, new_arr.shape[2], new_arr.shape[1], ScanDirection.WEST.value
	
	
def _get_imgs_from_bytes(
    pixel_bytes: bytes, channels: list[int], img_width: int, img_height: int
) -> list[np.ndarray]:
    """Extract individual channel images from pixel data bytes."""
    n_channels = len(channels)
    image_size_bytes = img_width * img_height
    all_pixels = np.frombuffer(pixel_bytes[: n_channels * image_size_bytes], dtype=np.uint8).reshape(
        n_channels, img_height, img_width
    )
    return [all_pixels[i] for i in range(n_channels)]
	
	
def _build_metadata(
    *,
    header_bytes: bytes,
    reader,
    pixel_data_size: int,
    skip_pixel_bytes: bool,
    file_path: Path,
    img_width: int,
	img_height: int,
    scan_dir: int,
    channels: list[int],
    verbose: bool,
) -> RawImageMetadata:
	"""Build a RawImageMetadata from header bytes and optional footer."""
    original_filename = file_path.name

    # Start with only the fields always available from the header
    kwargs: dict[str, Any] = {
        "OriginalFilename": original_filename,
        "ImgWidth": int(img_width),
        "ImgHeight": int(img_height),
        "ScanDir": scan_dir,
        "Channels": channels,
    }
	
	if verbose:
        logger.info("Extracting full metadata from header and footer")

    kwargs.update(
        PixelSizeX=float(_read_header_field(header_bytes, HeaderField.PIXEL_SIZE_X) * 1e6),
        PixelSizeY=float(_read_header_field(header_bytes, HeaderField.PIXEL_SIZE_Y) * 1e6),
        LegacyImageId=int(_read_header_field(header_bytes, HeaderField.IMAGE_ID)),
        ImgPosX=float(_read_header_field(header_bytes, HeaderField.IMG_POS_X)),
		ImgPosY=float(_read_header_field(header_bytes, HeaderField.IMG_POS_Y)),
        TestId=int(_read_header_field(header_bytes, HeaderField.TEST_ID)),
        DieIndex=int(_read_header_field(header_bytes, HeaderField.DIE_INDEX)),
        DieIdX=int(_read_header_field(header_bytes, HeaderField.DIE_X)),
        DieIdY=int(_read_header_field(header_bytes, HeaderField.DIE_Y)),
		DiePosX=float(_read_header_field(header_bytes, HeaderField.DIE_POS_X)),
        DiePosY=float(_read_header_field(header_bytes, HeaderField.DIE_POS_Y)),
        GdsPosX=int(_read_header_field(header_bytes, HeaderField.GDS_POS_X)),
        GdsPosY=int(_read_header_field(header_bytes, HeaderField.GDS_POS_Y)),
        ImgSegment=int(_read_header_field(header_bytes, HeaderField.IMG_SEGMENT)),
        JobId=int(_read_header_field(header_bytes, HeaderField.JOB_ID)),
	)
    # WaferPos mirrors ImgPos
    kwargs["WaferPosX"] = kwargs["ImgPosX"]
    kwargs["WaferPosY"] = kwargs["ImgPosY"]

    if skip_pixel_bytes:
        reader.seek(pixel_data_size, os.SEEK_CUR)
		
	footer_bytes = reader.read()
    if footer_bytes:
        kwargs.update(
            DieXFooter=float(_read_footer_field(footer_bytes, FooterField.DIE_X)),
            DieYFooter=float(_read_footer_field(footer_bytes, FooterField.DIE_Y)),
            GdsPosXFooter=int(_read_footer_field(footer_bytes, FooterField.GDS_POS_X)),
			GdsPosYFooter=int(_read_footer_field(footer_bytes, FooterField.GDS_POS_Y)),
            GlobalOffsetX=float(_read_footer_field(footer_bytes, FooterField.GLOBAL_OFFSET_X)),
            GlobalOffsetY=float(_read_footer_field(footer_bytes, FooterField.GLOBAL_OFFSET_Y)),
            BlockSizeX=int(_read_footer_field(footer_bytes, FooterField.BLOCK_SIZE_X)),
            BlockSizeY=int(_read_footer_field(footer_bytes, FooterField.BLOCK_SIZE_Y)),
			BlockNumX=int(_read_footer_field(footer_bytes, FooterField.BLOCK_NUM_X)),
            BlockNumY=int(_read_footer_field(footer_bytes, FooterField.BLOCK_NUM_Y)),
            LocalOffsetX=_read_footer_field(
                footer_bytes, FooterField.LOCAL_OFFSET_X, is_array=True
            ).tolist(),
			LocalOffsetY=_read_footer_field(
                footer_bytes, FooterField.LOCAL_OFFSET_Y, is_array=True
            ).tolist(),
        )

    return RawImageMetadata(**kwargs)