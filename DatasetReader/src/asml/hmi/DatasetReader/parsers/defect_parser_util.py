"""Utility functions and classes for defect file parsing.

This module provides minimal, modernized implementations of parsing utilities
from the legacy arceus repository to minimize cross-repo dependencies.

Legacy Source Reference
-----------------------
Implementation derived from `arceus` commit
`26a5d5d960bccd61f258df191633fb6d12960a06`, primarily from
`./hmclient/hmadc/src/hmadc/c4api.py` and
`./hmclient/hmadc/src/hmadc/defects.py`.
"""

import hashlib
import logging
import struct
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import fsspec
import numpy as np

from asml.hmi.DatasetSchemas.bronze.defect_models import DefectRecord
from asml.hmi.DatasetSchemas.bronze.parse_results import ParseMetadata
from asml.hmi.DatasetSchemas.defect_enums import (
    TAG_ARRAY,
    TAG_ARRAY_INT,
    TAG_FLOAT,
    TAG_INT,
    DefectFileExtension,
    DefectTag,
)

# --- FEATURE ARRAY CONSTANTS ---
# Confidence index location within ValueExtended array
# Source: arceus/c4api.py
# The full 256-element feature array is split into:
#  - Value[0:127] (first 128 elements, indices 0-127)
# - ValueExtended[0:127] (next 128 elements, indices 128-255 in full array)
# Confidence index is at position 185 in the full array, which maps to
# position 185-128=57 in the ValueExtended array
CONFIDENCE_INDEX_IN_VALUE_EXTENDED = 57

# Create logger instance
logger = logging.getLogger(__name__)

# --- DEFECT ATTRIBUTE PARSING ---

def parse_defect_attribute(tag: str, value: str, no_array: bool = False) -> Any:
    """Parse defect attribute from XML string value.

    Modernized version of translate() from arceus/c4api.py.
    Converts XML string values to appropriate Python types based on tag.

    Parameters
    ----------
    tag : str
        Attribute tag name (e.g., "ReviewType", "Value").
    value : str
        String value from XML attribute.
    no_array : bool
        If True, skip array parsing except for confidence index extraction.

    Returns
    -------
    Any
        Parsed value with appropriate type:
        - int for TAG_INT tags
        - float for TAG_FLOAT tags
        - list[float] for TAG_ARRAY tags (if no_array=False)
        - list[int] for TAG_ARRAY_INT tags (if no_array=False)
        - str otherwise

    Examples
    --------
    >>> parse_defect_attribute("ReviewType", "1", no_array=False)
1
>>> parse_defect_attribute("Threshold", "0.5", no_array=False)
0.5
>>> parse_defect_attribute("Value", "1.0 2.0 3.0", no_array=False)
[1.0, 2.0, 3.0]
"""

    # Special handling for no_array mode
    # checked BEFORE type-specific parsing to short-circuit non-ValueExtended tags
    if no_array:
        # Extract confidence index from ValueExtended
        # Source: c4api.py
        if tag == DefectTag.VALUE_EXTENDED.value:
            parts = [x for x in value.split() if x.strip()]
            if len(parts) > CONFIDENCE_INDEX_IN_VALUE_EXTENDED:
                # Extract confidence index from ValueExtended array
                # Confidence is stored at index 185 in the full 256-element feature array.
                # Since ValueExtended contains elements 128-255, the confidence index
                # is at position 185-128=57 within ValueExtended.
                # Source: arceus/c4api.py (lst[185 - 128])
                return float(parts[CONFIDENCE_INDEX_IN_VALUE_EXTENDED])
        return None

    # Integer tags
    if tag in TAG_INT:
        return int(value)

    # Float tags
    if tag in TAG_FLOAT:
        return float(value)

    # Array of floats
    if tag in TAG_ARRAY:
        parts = [x for x in value.split() if x.strip()]
        return [float(x) for x in parts]

    # Array of integers
    if tag in TAG_ARRAY_INT:
        parts = [x for x in value.split() if x.strip()]
        return [int(x) for x in parts]
    
    # Default: return as-is
    return value

# --- UTILITY FUNCTIONS ---

def compute_file_checksum(
    file_path: Path | str,
    fs: fsspec.AbstractFileSystem | None = None,
) -> str:
    """Compute SHA256 checksum of a file for integrity verification.

    Parameters
    ----------
    file_path : Path
        Path to the file to checksum.

    Returns
    -------
    str
        Hexadecimal SHA256 checksum.

    Examples
    --------
    >>> from pathlib import Path
    >>> checksum = compute_file_checksum(Path("data.ddf"))
    >>> len(checksum)
    64
    """
    sha256_hash = hashlib.sha256()
    fs_to_use = fs or fsspec.filesystem("file")
    with fs_to_use.open(str(file_path), "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def create_parse_metadata(
    file_path: Path | str,
    file_format: str,
    fs: fsspec.AbstractFileSystem | None = None,
) -> ParseMetadata:
    """Create metadata about a parsing operation.

    Parameters
    ----------
    file_path : Path
        Path to the file being parsed.
    file_format : str
        File format extension (e.g., '.ddf', '.dzf', '.defect', '.patch').

    Returns
    -------
    ParseMetadata
        Metadata model with file stats, checksum and timestamp.
    """
    path_str = str(file_path)
    fs_to_use = fs or fsspec.filesystem("file")
    file_info = fs_to_use.info(path_str)
    file_size = int(file_info.get("size", 0))

    return ParseMetadata(
        source_file=Path(path_str),
        file_format=file_format,
        parsed_at=datetime.now(),
        file_size_bytes=file_size,
        file_checksum=compute_file_checksum(path_str, fs=fs_to_use),
    )


def parse_patch_images_from_bytes(image_bytes: bytes, imgsize: int) -> np.ndarray:
    """Convert raw patch image bytes to numpy array.

    Based on iddf.py parse_ddf() function (lines 346-361).

    Parameters
    ----------
    image_bytes : bytes
        Raw image data (imgsize x imgsize bytes).
    imgsize : int
        Image dimension in pixels (32, 64, 128, 192, 256, or 512).

    Returns
    -------
    np.ndarray
        2D grayscale image array (uint8, shape: imgsize x imgsize).

    Raises
    ------
    ValueError
        If image_bytes length doesn't match expected size.

    Notes
    -----
    - Source: iddf.py
    - Images are stored as raw uint8 bytes in row-major order
    - Direct memory mapping via np.frombuffer for efficiency

    Examples
    --------
    >>> raw_bytes = f.read(64 * 64)
    >>> img_array = parse_patch_images_from_bytes(raw_bytes, 64)
    >>> img_array.shape
    (64, 64)
    >>> img_array.dtype
    dtype('uint8')
    """
    expected_size = imgsize * imgsize
    if len(image_bytes) != expected_size:
        raise ValueError(
            f"Image bytes size mismatch: expected {expected_size} bytes "
            f"for {imgsize}x{imgsize} image, got {len(image_bytes)} bytes"
        )

    # Convert bytes to numpy array and reshape
    # Source: iddf.py line 346-361
    img_array = np.frombuffer(image_bytes, dtype=np.uint8).reshape(imgsize, imgsize)

    return img_array

def parse_single_defect_patches(
    file_handle, imgsize: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Read and parse 3 patch images for a single defect.

    Based on iddf.py read_nth_defect_from_ddf() function (lines 146-151)
    and parse_ddf() function (lines 346-361).

    This function assumes the file pointer is positioned immediately after
    the PycDefectBase struct (1024 bytes) and reads the 3 consecutive
    patch images.

    Parameters
    ----------
    file_handle : BinaryIO
        Open file handle positioned after defect struct.
    imgsize : int
        Image dimension in pixels (32, 64, 128, 192, 256, or 512).

    Returns
    -------
    Tuple[np.ndarray, np.ndarray, np.ndarray]
        Three 2D grayscale image arrays:
        - raw_patch: Original defect patch image
        - reference_patch: Reference/background patch for comparison
        - mask_patch: Mask indicating defect region
        Each array has shape (imgsize, imgsize) and dtype uint8.

    Notes
    -----
    - Source: iddf.py (read bytes, reshape)
    - Images are read sequentially: raw_patch, reference_patch, mask_patch
      (corresponds to legacy image1, image2, image3)
    - Each image is imgsize x imgsize bytes (uint8, row-major)

    Examples
    --------
    >>> with open("defects.ddf", "rb") as f:
    ...     f.seek(defect_position)  # Skip to defect location
    ...     _ = f.read(1024)  # Skip PycDefectBase struct
    ...     raw, ref, mask = parse_single_defect_patches(f, 64)
    >>> raw.shape
    (64, 64)
    """

    # Read 3 consecutive patch images
    # Source: iddf.py
    # Naming convention:
    #   - raw_patch: Original defect patch image
    #   - reference_patch: Reference/background patch for comparison
    #   - mask_patch: Mask indicating defect region
    raw_patch_bytes = file_handle.read(imgsize * imgsize)
    reference_patch_bytes = file_handle.read(imgsize * imgsize)
    mask_patch_bytes = file_handle.read(imgsize * imgsize)

    # Convert bytes to numpy arrays
    # Source: iddf.py
    raw_patch = parse_patch_images_from_bytes(raw_patch_bytes, imgsize)
    reference_patch = parse_patch_images_from_bytes(reference_patch_bytes, imgsize)
    mask_patch = parse_patch_images_from_bytes(mask_patch_bytes, imgsize)

    return raw_patch, reference_patch, mask_patch


def read_patch_file_binary(
    file_path: Path, batch_start: int | None = None, batch_end: int | None = None
) -> tuple[int, int, int, int, int, int, list[list[np.ndarray]]]:
    """Read .patch binary file and extract patch images.

    Refactoring of Arceus pbddf.Pb2ddf.readPatches() functionality.
    Parses .patch file header and extracts multi-channel patch images for each defect.
    Supports both full file reading (backward compatible) and batch reading for
    memory-efficient streaming.

    Parameters
    ----------
    file_path : Path
        Path to .patch file to read.
    batch_start : int | None, optional
        Starting defect index (0-based) for batch reading. If None, reads all defects.
    batch_end : int | None, optional
        Ending defect index (exclusive) for batch reading. If None, reads all defects.

    Returns
    -------
    tuple[int, int, int, int, int, int, list[list[np.ndarray]]]
        - header_size: Size of header in bytes (typically 15)
        - data_offset: Byte offset where patch data starts
        - defect_count: Total number of defects in file (not batch size)
        - channels: Number of image channels per defect (typically 3)
        - imgsize: Width/height of each patch image
        - patch_data: list[list[np.ndarray]] where:
            - Outer list: defects in batch (length = batch_end - batch_start, or defect_count if no batch)
            - Inner list: channels (length = channels, typically 3)
            - np.ndarray: 2D grayscale image (imgsize x imgsize, dtype=uint8)

    Raises
    ------
    ValueError
        If file header is invalid or corrupted, or if batch indices are invalid.

    Notes
    -----
    Binary structure:
    - Offset 0-1: Magic/signature (2 bytes)
    - Offset 2-3: header_size (2 bytes, 'H', typically 15)
    - Offset 4-7: data_offset (4 bytes, 'I')
    - Offset 8-11: defect_count (4 bytes, 'I')
    - Offset 12: channels (1 byte, typically 3)
    - Offset 13-14: imgsize (2 bytes, 'H', e.g., 64)
    - Then at data_offset, for each defect:
        - For each channel (0 to channels-1):
            - imgsize^2 bytes: grayscale image data

    Channel ordering:
    - Channel 0: raw_patch (original defect patch)
    - Channel 1: reference_patch (reference/background patch)
    - Channel 2: mask_patch (mask indicating defect region)

    Batch Reading:
    - When batch_start and batch_end are provided, only reads the specified range
    - Reduces memory usage by loading only the required defects
    - File is still opened and header parsed, but only batch bytes are read
    """
    with fsspec.open(str(file_path), "rb") as f:
        # Read 15-byte header
        header_bytes = f.read(15)
        if len(header_bytes) < 15:
            raise ValueError(
                f"Invalid .patch file header: expected at least 15 bytes, got {len(header_bytes)}"
            )

        # Parse header fields
        # Offset 2-3: header_size (2 bytes, little-endian unsigned short)
        header_size = struct.unpack("<H", header_bytes[2:4])[0]

        # Offset 4-7: data_offset (4 bytes, little-endian unsigned int)
        data_offset = struct.unpack("<I", header_bytes[4:8])[0]

        # Offset 8-11: defect_count (4 bytes, little-endian unsigned int)
        defect_count = struct.unpack("<I", header_bytes[8:12])[0]

        # Offset 12: channels (1 byte)
        channels = header_bytes[12]

        # Offset 13-14: imgsize (2 bytes, little-endian unsigned short)
        imgsize = struct.unpack("<H", header_bytes[13:15])[0]

        # Determine batch range
        if batch_start is None and batch_end is None:
            # Read all defects (backward compatible behavior)
            batch_start = 0
            batch_end = defect_count
        elif batch_start is None or batch_end is None:
            raise ValueError(
                "Both batch_start and batch_end must be provided together, or both None"
            )
        
        # Validate batch indices
        if batch_start < 0 or batch_end > defect_count or batch_start >= batch_end:
            raise ValueError(
                f"Invalid batch range: batch_start={batch_start}, batch_end={batch_end}, "
                f"defect_count={defect_count}. Must satisfy: 0 <= batch_start < batch_end <= defect_count"
            )

        # Calculate byte offsets for batch
        patch_size = imgsize * imgsize  # Number of bytes per single-channel image

        bytes_per_defect = channels * patch_size
        batch_size = batch_end - batch_start
        batch_byte_offset = data_offset + (batch_start * bytes_per_defect)
        batch_total_bytes = batch_size * bytes_per_defect

        # Seek to batch start and read only batch data
        f.seek(batch_byte_offset)
        batch_bytes = f.read(batch_total_bytes)

        if len(batch_bytes) < batch_total_bytes:
            raise ValueError(
                f"Unexpected EOF: expected {batch_total_bytes} bytes for batch "
                f"[{batch_start}:{batch_end}], got {len(batch_bytes)}"
            )

        # Single conversion + reshape to 4D array: (batch_size, channels, height, width)
        # This vectorized approach is more memory and compute efficient than nested loops
        batch_patches = np.frombuffer(batch_bytes, dtype=np.uint8).reshape(
            batch_size, channels, imgsize, imgsize
        )

        # Convert to List[List[np.ndarray]] for backward compatibility with return type
        patch_data = [
            [batch_patches[defect_idx, channel_idx].copy() for channel_idx in range(channels)]
            for defect_idx in range(batch_size)
        ]

    return header_size, data_offset, defect_count, channels, imgsize, patch_data

def calculate_optimal_batch_size(
    imgsize: int, target_memory_mb: float = 32.0, channels: int = 3
) -> int:
    """Calculate optimal batch size for defect patch processing.

    Determines the number of defects to process in a single batch based on
    patch image dimensions and target memory usage. Optimized for efficient
    writing to Hugging Face datasets.

    Parameters
    ----------
    imgsize : int
        Width/height of each patch image in pixels (e.g., 32, 64, 128, 256).
    target_memory_mb : float, optional
        Target memory usage per batch in megabytes (default: 32.0 MB).
        Typical values:
        - 16 MB: Conservative for memory-constrained environments
        - 32 MB: Balanced for most use cases (default)
        - 64 MB: Aggressive for high-performance systems
    channels : int, optional
        Number of image channels per defect (default: 3 for raw/ref/mask).

    Returns
    -------
    int
        Optimal number of defects per batch (minimum 1).

    Notes
    -----
    Memory calculation:
    - bytes_per_defect = imgsize² x channels x 1 byte (uint8)
    - batch_size = target_memory_mb x 1_000_000 / bytes_per_defect

    Performance considerations:
    - Larger batches reduce I/O overhead and improve throughput
    - Smaller batches reduce memory footprint and enable streaming
    - Batch size is clamped to minimum of 1 for safety

    Examples
    --------
    >>> calculate_optimal_batch_size(64, target_memory_mb=32)
    2604
    >>> calculate_optimal_batch_size(128, target_memory_mb=16)
    325
    """
    bytes_per_defect = imgsize * imgsize * channels
    batch_size = int((target_memory_mb * 1000000) // bytes_per_defect)
    
    return max(1, batch_size)

def extract_defect_patches_batch(
    file_path: Path, batch_size: int | None = None, target_memory_mb: float = 32.0
) -> Iterator[list[tuple[np.ndarray, np.ndarray, np.ndarray]]]:
    """Extract defect patch images in batches from patch-containing defect
    files.

    Yields batches of defect patch triplets (raw/reference/mask) for efficient
    processing and writing to Hugging Face datasets. Batch size is automatically
    optimized based on patch dimensions if not explicitly provided.

    This function provides a memory-efficient streaming interface for processing
    large defect files without loading all patches into memory at once.

    Parameters
    ----------
    file_path : Path
        Path to defect file containing patch images.
        Supported formats: .ddf, .patch, .defect (as defined by DefectFileExtension.with_patch_data()).
    batch_size : int | None, optional
        Number of defects per batch. If None (default), automatically calculated
        based on imgsize and target_memory_mb for optimal performance.
    target_memory_mb : float, optional
        Target memory usage per batch in megabytes (default: 32.0 MB).
        Only used when batch_size is None. See calculate_optimal_batch_size().

    Yields
    ------
    list[Tuple[np.ndarray, np.ndarray, np.ndarray]]
        Batch of defect patch triplets. Each tuple contains:
        - raw_patch: Original defect patch image (uint8, shape: imgsize x imgsize)
        - reference_patch: Reference/background patch (uint8, shape: imgsize x imgsize)
        - mask_patch: Mask indicating defect region (uint8, shape: imgsize x imgsize)

        Batch size varies:
        - For intermediate batches: batch_size defects
        - For final batch: remaining defects (may be < batch_size)

    Raises
    ------
    ValueError
        - If file extension is not supported (must be .ddf or .patch)
        - If file is corrupted or has invalid structure
        - If batch_size is provided but <= 0
    FileNotFoundError
        - If file_path does not exist.

    Notes
    -----
    Supported file formats:
    - .patch: Fast (single read), higher memory usage.
    - .ddf: Memory-efficient (streaming), moderate I/O

    Performance: Larger batches improve throughput but increase memory.
    Auto-batching (batch_size=None) balances both.

    Potential Refactoring
    ---------------------
    Current implementation always loads all 3 channels (raw/reference/mask).
    If DLFE or other deep learning applications only train on a single channel,
    consider adding a `channels` parameter to support selective channel loading
    (e.g., `channels='raw'` or `channels=['raw', 'mask']`). This would reduce
    memory usage and I/O overhead for single-channel use cases.
    """

    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Defect file not found: {file_path}")

    # Get file extension and validate format
    file_ext = file_path.suffix.lower()
    if file_ext not in DefectFileExtension.with_patch_data():
        raise ValueError(
            f"Unsupported file format: {file_ext}. "
            f"Must be one of: {DefectFileExtension.with_patch_data()}"
        )

    # Validate batch_size if provided
    if batch_size is not None and batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")

    # Handle .patch files
    if file_ext == DefectFileExtension.PATCH.value:
        logger.info(f"Streaming .patch file: {file_path}")

        # Read only header to get metadata (memory-efficient)
        _, _, defect_count, channels, imgsize, _ = read_patch_file_binary(
            file_path, batch_start=0, batch_end=1
        )

        logger.info(
            f"Patch file contains {defect_count} defects with {channels} channels, "
            f"imgsize={imgsize}x{imgsize}"
        )

        # Calculate batch size if not provided
        if batch_size is None:
            batch_size = calculate_optimal_batch_size(imgsize, target_memory_mb, channels)
            logger.info(f"Auto-calculated batch_size={batch_size}")

        # Validate channel count (expect 3: raw, ref, mask)
        if channels != 3:
            logger.warning(
                f"Expected 3 channels (raw/ref/mask), got {channels}. Using first 3 channels only."
            )

        # Yield batches with true streaming (load only current batch into memory)
        for batch_start_idx in range(0, defect_count, batch_size):
            batch_end_idx = min(batch_start_idx + batch_size, defect_count)

            # Read only current batch from file (memory-efficient)
            _, _, _, _, _, patch_data = read_patch_file_binary(
                file_path, batch_start=batch_start_idx, batch_end=batch_end_idx
            )

            batch = []
            for defect_idx in range(len(patch_data)):
                channels_data = patch_data[defect_idx]
                # Extract first 3 channels (raw, ref, mask)
                raw_patch = channels_data[0]
                reference_patch = (
                    channels_data[1] if len(channels_data) > 1 else np.zeros_like(raw_patch)
                )
                mask_patch = channels_data[2] if len(channels_data) > 2 else np.zeros_like(raw_patch)

                batch.append((raw_patch, reference_patch, mask_patch))

            logger.debug(
                f"Yielding batch {batch_start_idx // batch_size + 1}: "
                f"defects {batch_start_idx}-{batch_end_idx - 1} ({len(batch)} defects)"
            )
            yield batch

    # Handle .ddf files (streaming approach)
    elif file_ext == DefectFileExtension.DDF.value:
        logger.info(f"Streaming .ddf file: {file_path}")

        with fsspec.open(str(file_path), "rb") as f:
            # Read DDF header (32 bytes based on typical DDF structure)
            header_bytes = f.read(32)
            if len(header_bytes) < 32:
                raise ValueError(
                    f"Invalid .ddf file header: expected 32 bytes, got {len(header_bytes)}"
                )
            
            # Parse header fields
        # Offset 0-3: defect_count (4 bytes, little-endian unsigned int)
        # Offset 4-7: imgsize (4 bytes, little-endian unsigned int)
        defect_count, imgsize = struct.unpack("<II", header_bytes[0:8])

        logger.info(f"DDF file contains {defect_count} defects, imgsize={imgsize}x{imgsize}")

        # Calculate batch size if not provided
        if batch_size is None:
            batch_size = calculate_optimal_batch_size(imgsize, target_memory_mb, channels=3)
            logger.info(f"Auto-calculated batch_size={batch_size}")

        # Calculate bytes per defect
        # Structure: PycDefectBase (1024 bytes) + 3 patch images
        defect_struct_size = 1024
        patch_bytes_per_defect = imgsize * imgsize * 3
        bytes_per_defect = defect_struct_size + patch_bytes_per_defect

        # Process defects in batches using vectorized approach
        for batch_start in range(0, defect_count, batch_size):
            batch_end = min(batch_start + batch_size, defect_count)
            actual_batch_size = batch_end - batch_start

            # Vectorized read: fetch entire batch in one operation
            batch_byte_offset = 32 + (batch_start * bytes_per_defect)
            batch_total_bytes = actual_batch_size * bytes_per_defect

            f.seek(batch_byte_offset)
            batch_bytes = f.read(batch_total_bytes)

            if len(batch_bytes) < batch_total_bytes:
                raise ValueError(
                    f"Unexpected EOF: expected {batch_total_bytes} bytes for batch "
                    f"[{batch_start}:{batch_end}], got {len(batch_bytes)}"
                )

            # Convert to numpy array for efficient slicing
            batch_array = np.frombuffer(batch_bytes, dtype=np.uint8)

            # Extract patches using vectorized slicing (skip metadata portions)
            batch = []
            for local_idx in range(actual_batch_size):
                # Calculate offset within batch_array for this defect
                defect_offset = local_idx * bytes_per_defect

                # Skip 1024-byte metadata struct, extract 3 consecutive patch images
                patches_start = defect_offset + defect_struct_size
                patches_end = patches_start + patch_bytes_per_defect
                patches = batch_array[patches_start:patches_end]

                # Reshape to (3, imgsize, imgsize) for 3 channels
                patches_reshaped = patches.reshape(3, imgsize, imgsize)

                raw_patch = patches_reshaped[0]
                reference_patch = patches_reshaped[1]
                mask_patch = patches_reshaped[2]

                batch.append((raw_patch, reference_patch, mask_patch))

            logger.debug(
                f"Yielding batch {batch_start // batch_size + 1}: "
                f"defects {batch_start}-{batch_end - 1} ({len(batch)} defects)"
            )
            yield batch

    # Handle .defect files (protobuf format)
    elif file_ext == DefectFileExtension.DEFECT.value:
        raise NotImplementedError(
            f"Batch extraction for .defect (protobuf) format is not yet implemented. "
            f"File: {file_path}"
        )

    else:
        # This should never happen due to earlier validation
        raise ValueError(f"Unsupported file extension: {file_ext}")

# --- PROTOBUF CONVERSION ---

def convert_protobuf_to_defect_record(pd) -> DefectRecord:
    """Convert protobuf DEFECT to DefectRecord model.

    Based on pbddf.py toDefect() function (lines 163-211) and Pb2ddf.MP mapping.

    Parameters
    ----------
    pd : DEFECTSTRUCT_pb2.DEFECT
        Protobuf defect object.

    Returns
    -------
    DefectRecord
        Validated DefectRecord dataclass model.

    Notes
    -----
    Field mapping based on Pb2ddf.MP (pbddf.py lines 22-67).
    Format: protobuf_field -> (ddf_field, type, array_length)

    Examples
    --------
    >>> from asml.hmi.DatasetReader.parsers import DEFECTSTRUCT_pb2
    >>> pd = DEFECTSTRUCT_pb2.DEFECT()
    >>> defect_record = convert_protobuf_to_defect_record(pd)
    >>> defect_record.i_defect_id
    0
    """

    # Handle array fields with defaults
    i_type = list(pd.iType) if len(pd.iType) > 0 else [0] * 5
    f_strength = list(pd.fStrength) if len(pd.fStrength) > 0 else [0.0] * 5
    f_feature = list(pd.fFeature) if len(pd.fFeature) > 0 else [0.0] * 128
    # iFeatureSelected is bytes field - unpack 128 bytes into 32 int32 values
    i_feature_selected = (
        list(struct.unpack("32i", pd.iFeatureSelected))
        if len(pd.iFeatureSelected) == 128
        else [0] * 32
    )
    f_d_feature = list(pd.fDFeature) if len(pd.fDFeature) > 0 else [0.0] * 32

    # Handle iVersion special case: use iPatchSize for backwards compatibility
    # Based on pbddf.py toDefect() lines 189-191
    # The old version: iVersion is actually patch size
    # The new version: 2024 new version; all old version 0
    i_version = pd.iPatchSize

    # Create DefectRecord directly from protobuf fields
    return DefectRecord(
        i_version=i_version,
        i_defect_id=pd.iDefectID,
        i_i_id=pd.iIID,
        i_column=pd.iColumn,
        i_die_index=pd.iDieIndex,
        i_die_x=pd.iDieX,
        i_die_y=pd.iDieY,
        i_flag=pd.iFlag,
        i_selected=pd.iSelected,
        i_i_id1=pd.iIID1,
        i_i_id2=pd.iIID2,
        i_temp_flag=pd.iTempFlag,
        img_x=pd.imgX,
        img_y=pd.imgY,
        img_x_size=pd.imgXSize,
        img_y_size=pd.imgYSize,
        f_x=pd.fX,
        f_y=pd.fY,
        f_x_size=pd.fXSize,
        f_y_size=pd.fYSize,
        threshold=pd.threshold,
        strength1=pd.strength1,
        strength2=pd.strength2,
        sigma1=pd.sigma1,
        sigma2=pd.sigma2,
        i_type=i_type,
        f_strength=f_strength,
        i_feature=pd.iFeature,
        f_feature=f_feature,
        i_feature_selected=i_feature_selected,
        f_d_feature=f_d_feature,
    )