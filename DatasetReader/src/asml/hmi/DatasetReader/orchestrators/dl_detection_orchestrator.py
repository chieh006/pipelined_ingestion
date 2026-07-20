"""DL Detection pipeline orchestrator.

This module coordinates .dzf + .raw parsing for DL Detection training
workflows. It handles pipeline-specific post-processing such as MBI mode
detection, coordinate transformations, and defect patch extraction from full
FOV images.
"""

import logging
from pathlib import Path

import numpy as np

from asml.hmi.DatasetReader.parsers.dzf_parser import DzfFileParser
from asml.hmi.DatasetReader.parsers.raw_file_parser import RawFileParser
from asml.hmi.DatasetSchemas.bronze.parse_results import DzfParseResult
from asml.hmi.DatasetSchemas.bronze.raw_file import RawImageMetadata

logger = logging.getLogger(__name__)


def load_dzf_for_detection(dzf_path: str | Path) -> DzfParseResult:
    """Load .dzf file for DL Detection training.

    This function parses the .dzf file and prepares it for DL Detection
    training workflows, which require full FOV images from .raw files.

    Parameters
    ----------
    dzf_path : str | Path
        Path to the .dzf file.

    Returns
    -------
    DzfParseResult
        Full parse result containing defect metadata and class type table.
    """
    parser = DzfFileParser(dzf_path)
    return parser.parse()
	

def create_image_filename_mapping(dzf_result: DzfParseResult) -> dict:
    """Create mapping from defect IDs to image filenames.

    Extracts the image filenames from parsed defects and builds a lookup
    dictionary for resolving which .raw file contains each defect.

    Parameters
    ----------
    dzf_result : DzfParseResult
        Parsed .dzf file result.

    Returns
    -------
    dict
        Mapping of {defect_id: image_id, ...}

    Examples
    --------
    >>> result = load_dzf_for_detection("data/inspection.dzf")
    >>> mapping = create_image_filename_mapping(result)
    >>> print(mapping[12345])  # Image ID for defect 12345
    """
	
	mapping = {}
    for defect in dzf_result.parsed_defects:
        if defect.defect_id is not None and defect.image_id is not None:
            mapping[defect.defect_id] = defect.image_id
    return mapping
	

# =============================================================================
# .raw Image Orchestration
# =============================================================================

def adjust_position_for_mbi(
    metadata: RawImageMetadata,
) -> tuple[float, float]:
    """Compute MBI-adjusted image position.

    Replaces: hmdldet/utils/image_io.py:HMIImageCoordProcessor._adjust_image_positions_for_mbi()

    For MBI mode, adjusts ImgPosX/Y using JobId, image dimensions,
    and pixel sizes to account for multi-beam offset.

    Parameters
    ----------
    metadata : RawImageMetadata
        Parsed .raw metadata (must include JobId, ImgWidth, ImgHeight,
        PixelSizeX, PixelSizeY, ImgPosX, ImgPosY).
		
	Returns
    -------
    tuple[float, float]
        (adjusted_pos_x, adjusted_pos_y). If ``JobId`` is None, logs a
        warning and returns ``(ImgPosX or 0.0, ImgPosY or 0.0)`` without
        MBI adjustment. Other missing fields (``PixelSizeX/Y``) fall back
        to 1.0 silently.
    """ 
	pos_x = metadata.ImgPosX or 0.0
    pos_y = metadata.ImgPosY or 0.0
    job_id = metadata.JobId
    if job_id is None:
        logger.warning(
            "JobId missing from RawImageMetadata; "
            "skipping MBI position adjustment and returning "
            "(ImgPosX, ImgPosY) with None\u21920.0 fallback."
        )
        return pos_x, pos_y
    pixel_size_x = metadata.PixelSizeX or 1.0
    pixel_size_y = metadata.PixelSizeY or 1.0
    adj_x = pos_x + (job_id + 0.5) * metadata.ImgWidth * pixel_size_x / 1e6
    adj_y = pos_y + 0.5 * metadata.ImgHeight * pixel_size_y / 1e6
    return adj_x, adj_y 
	
def _is_pixel_in_bounds(image_x: int, image_y: int, metadata: RawImageMetadata) -> bool:
    """Return True when pixel coordinates are within image boundaries."""
    return 0 <= image_x < metadata.ImgWidth and 0 <= image_y < metadata.ImgHeight
	
def get_die_position(
    image_x: int,
    image_y: int,
    metadata: RawImageMetadata,
    img_pos_x: float,
    img_pos_y: float,
    use_offsets: bool = True,
) -> tuple[float, float]:
	"""Convert pixel coordinates to die-relative coordinates.

    Replaces: hmdldet/utils/image_io.py:HMIImageCoordProcessor.getDiePos()

    Computes die-relative position by converting pixel offsets from the
    image centre to physical units (µm) and subtracting the die origin.

    Parameters
    ----------
    image_x : int
        Defect X pixel coordinate in the .raw image.
    image_y : int
        Defect Y pixel coordinate in the .raw image.
    metadata : RawImageMetadata
        Parsed .raw metadata.
    img_pos_x : float
        Image position X in µm (MBI-adjusted if applicable).
    img_pos_y : float
        Image position Y in µm (MBI-adjusted if applicable).
    use_offsets : bool
        Whether to apply global and local footer offsets.
		
	Returns
    -------
    tuple[float, float]
        (die_pos_x, die_pos_y) in µm. Returns (-1.0, -1.0) if ``DiePosX``
        or ``DiePosY`` is falsy (die position missing), or if the pixel
        coordinates exceed the image dimensions.
    """ 
	
	die_pos_x = metadata.DiePosX
    die_pos_y = metadata.DiePosY
    if not die_pos_x or not die_pos_y:
        logger.debug("Die position info missing in raw metadata.")
        return -1.0, -1.0
    if not _is_pixel_in_bounds(image_x, image_y, metadata):
        logger.warning(
            f"Defect ({image_x}, {image_y}) out of image bounds "
            f"({metadata.ImgWidth}, {metadata.ImgHeight})."
        )
        return -1.0, -1.0

    pixel_size_x = metadata.PixelSizeX
    pixel_size_y = metadata.PixelSizeY

    if pixel_size_x is None or pixel_size_y is None:
        logger.warning("Malformed metadata: PixelSizeX/Y missing.")
        return -1.0, -1.0

    half_w = metadata.ImgWidth / 2.0
    half_h = metadata.ImgHeight / 2.0
	
	global_offset_x = 0.0
    global_offset_y = 0.0
    local_offset_x = 0.0
    local_offset_y = 0.0 
	
	if use_offsets and metadata.GlobalOffsetX is not None:
        global_offset_x = metadata.GlobalOffsetX
    if use_offsets and metadata.GlobalOffsetY is not None:
        global_offset_y = metadata.GlobalOffsetY
		
	if (
        use_offsets
        and metadata.LocalOffsetX is not None
        and metadata.LocalOffsetY is not None
        and metadata.BlockSizeX is not None
        and metadata.BlockSizeY is not None
        and metadata.BlockNumX is not None
    ):
        block_col = int((image_x - half_w + metadata.ImgWidth / 2) / metadata.BlockSizeX)
        block_row = int((image_y - half_h + metadata.ImgHeight / 2) / metadata.BlockSizeY)
        block_idx = block_row * metadata.BlockNumX + block_col
        if 0 <= block_idx < len(metadata.LocalOffsetX):
            local_offset_x = metadata.LocalOffsetX[block_idx]
        if 0 <= block_idx < len(metadata.LocalOffsetY):
            local_offset_y = metadata.LocalOffsetY[block_idx]
			
	wafer_x = img_pos_x + (image_x - half_w) * pixel_size_x + global_offset_x + local_offset_x
    wafer_y = img_pos_y + (image_y - half_h) * pixel_size_y + global_offset_y + local_offset_y

    result_x = wafer_x - die_pos_x
    result_y = wafer_y - die_pos_y
    return result_x, result_y
	


def get_gds_position(
    image_x: int,
    image_y: int,
    metadata: RawImageMetadata,
    img_pos_x: float,
    img_pos_y: float,
    y_flip: bool = False,
    use_offsets: bool = True,
) -> tuple[int, int]:
	"""Convert pixel coordinates to GDS coordinates.

    Replaces: hmdldet/utils/image_io.py:HMIImageCoordProcessor.getGdsPos()

    Parameters
    ----------
    image_x : int
        Defect X pixel coordinate.
    image_y : int
        Defect Y pixel coordinate.
    metadata : RawImageMetadata
        Parsed .raw metadata.
    img_pos_x : float
        Image position X in µm (MBI-adjusted if applicable).
    img_pos_y : float
        Image position Y in µm (MBI-adjusted if applicable).
    y_flip : bool
        Whether to flip Y coordinate before computation.
    use_offsets : bool
        Whether to apply global and local footer offsets.
		
	Returns
    -------
    tuple[int, int]
        (gds_x, gds_y). Returns (0, 0) if ``GdsPosX`` or ``GdsPosY`` is
        falsy (GDS position missing), or if the pixel coordinates exceed
        the image dimensions.
    """
	gds_pos_x = metadata.GdsPosX
    gds_pos_y = metadata.GdsPosY
    if not gds_pos_x or not gds_pos_y:
        logger.debug("GDS position info missing in raw metadata.")
        return 0, 0
    if not _is_pixel_in_bounds(image_x, image_y, metadata):
        logger.warning(
            f"Defect ({image_x}, {image_y}) out of image bounds "
            f"({metadata.ImgWidth}, {metadata.ImgHeight})."
        )
        return 0, 0
		
	pixel_size_x = metadata.PixelSizeX
    pixel_size_y = metadata.PixelSizeY

    if pixel_size_x is None or pixel_size_y is None:
        logger.warning("Malformed metadata: PixelSizeX/Y missing.")
        return 0, 0

    half_w = metadata.ImgWidth / 2.0
    half_h = metadata.ImgHeight / 2.0
    eff_y = metadata.ImgHeight - 1 - image_y if y_flip else image_y

    global_offset_x = metadata.GlobalOffsetX or 0.0 if use_offsets else 0.0
    global_offset_y = metadata.GlobalOffsetY or 0.0 if use_offsets else 0.0

    wafer_x = img_pos_x + (image_x - half_w) * pixel_size_x + global_offset_x
    wafer_y = img_pos_y + (eff_y - half_h) * pixel_size_y + global_offset_y

    result_x = round(gds_pos_x + wafer_x)
    result_y = round(gds_pos_y + wafer_y)
    return result_x, result_y
	

def get_wafer_position(
    image_x: int,
    image_y: int,
    metadata: RawImageMetadata,
    img_pos_x: float,
    img_pos_y: float,
    y_flip: bool = False,
    use_offsets: bool = False,
) -> tuple[float, float]: 
	"""Convert pixel coordinates to wafer coordinates.

    Replaces: hmdldet/utils/image_io.py:HMIImageCoordProcessor.getWaferPos()

    Parameters
    ----------
    image_x : int
        Defect X pixel coordinate.
    image_y : int
        Defect Y pixel coordinate.
    metadata : RawImageMetadata
        Parsed .raw metadata.
    img_pos_x : float
        Image position X in µm (MBI-adjusted if applicable).
    img_pos_y : float
        Image position Y in µm (MBI-adjusted if applicable).
    y_flip : bool
        Whether to flip Y coordinate before computation.
    use_offsets : bool
        Whether to apply global and local footer offsets.
		
	Returns
    -------
    tuple[float, float]
        (wafer_x, wafer_y) in µm. Returns (-1.0, -1.0) if ``img_pos_x``
        or ``img_pos_y`` is falsy (wafer position missing), or if the
        pixel coordinates exceed the image dimensions.
    """
	if not img_pos_x or not img_pos_y:
        logger.debug("Wafer position info missing in raw metadata.")
        return -1.0, -1.0
    if not _is_pixel_in_bounds(image_x, image_y, metadata):
        logger.warning(
            f"Defect ({image_x}, {image_y}) out of image bounds "
            f"({metadata.ImgWidth}, {metadata.ImgHeight})."
        )
        return -1.0, -1.0

    pixel_size_x = metadata.PixelSizeX
    pixel_size_y = metadata.PixelSizeY
	
	if pixel_size_x is None or pixel_size_y is None:
        logger.warning("Malformed metadata: PixelSizeX/Y missing.")
        return -1.0, -1.0

    half_w = metadata.ImgWidth / 2.0
    half_h = metadata.ImgHeight / 2.0
    eff_y = metadata.ImgHeight - 1 - image_y if y_flip else image_y

    global_offset_x = metadata.GlobalOffsetX or 0.0 if use_offsets else 0.0
    global_offset_y = metadata.GlobalOffsetY or 0.0 if use_offsets else 0.0

    wafer_x = img_pos_x + (image_x - half_w) * pixel_size_x + global_offset_x
    wafer_y = img_pos_y + (eff_y - half_h) * pixel_size_y + global_offset_y
    return wafer_x, wafer_y
	
	
def create_image_mapping_dict(
    raw_images_folder: Path,
    image_ext: str = "raw",
) -> dict[str, str]:
    """Build wildcard-to-actual image name mapping from folder contents.

    Replaces: hmdldet/utils/dzf_io.py:create_image_mapping_dict()

    Globs the raw image folder, constructs a wildcard key for each file
    (4th segment replaced by '*'), and maps it to the actual filename.

    Parameters
    ----------
    raw_images_folder : Path
        Directory containing .raw image files.
    image_ext : str
        File extension without dot.
		
	Returns
    -------
    dict[str, str]
        {wildcard_key: actual_image_name}
    """
	raw_images_folder = Path(raw_images_folder)
    mapping: dict[str, str] = {}
    for raw_image in raw_images_folder.glob(f"*.{image_ext}"):
        image_name = raw_image.stem
        segments = image_name.split("-")
        if len(segments) >= 4:
            segments[3] = "*"
        wildcard_key = "-".join(segments)
        mapping[wildcard_key] = image_name
    return mapping
	

def load_raw_image(
    raw_image_path: Path,
) -> tuple[np.ndarray, RawImageMetadata]:
    """Load a .raw image and return the per-channel stack + metadata.

    Convenience function wrapping RawFileParser.parse() for orchestration use.

    Parameters
    ----------
    raw_image_path : Path
        Path to the .raw file.

    Returns
    -------
    tuple[np.ndarray, RawImageMetadata]
        (image_3d_uint8, metadata) where image_3d_uint8 has shape
        ``(n_channels, H, W)``.
    """
    parser = RawFileParser(str(raw_image_path))
    result = parser.parse()
    return result.image_data, result.metadata
	
	

def load_ref_images(
    raw_image_path: Path,
    ref_image_dict: dict[str, list[str]],
) -> tuple[np.ndarray | None, np.ndarray | None]:
    """Load reference images associated with a defect image.

    Replaces: hmdldet/utils/image_io.py:HMIImageCoordProcessor.read_ref_images()

    Parameters
    ----------
    raw_image_path : Path
        Path to the primary .raw defect image.
    ref_image_dict : dict[str, list[str]]
        Mapping of image stem (without extension) to
        [ref1_stem, ref2_stem] (without extension).
		
	Returns
    -------
    tuple[np.ndarray | None, np.ndarray | None]
        (ref1_image, ref2_image) as 3-D ``(n_channels, H, W)`` uint8 arrays,
        or None if ref images are not found in the dict.
    """
	raw_image_path = Path(raw_image_path)
    image_stem = raw_image_path.stem
    ref_names = ref_image_dict.get(image_stem)
    if ref_names is None or len(ref_names) < 2:
        logger.warning("No reference images found for '%s' in ref_image_dict.", image_stem)
        return None, None

    ref_dir = raw_image_path.parent

    def _load_ref(ref_stem: str) -> np.ndarray | None:
        ref_path = ref_dir / f"{ref_stem}{raw_image_path.suffix}"
        if not ref_path.exists():
            logger.warning("Reference image not found: %s", ref_path)
            return None
        image, _ = load_raw_image(ref_path)
        return image

    ref1 = _load_ref(ref_names[0])
    ref2 = _load_ref(ref_names[1])
    return ref1, ref2
	

# --- Placeholder for patch alignment ---
# Patch alignment and cropping depends on hmpybind.imageprc (C++ binding).
# Functions to add when the IQE dependency is resolved:
# - crop_defect_patch(image, ref1, ref2, defect_center, patch_size, ...) -> tuple[np.ndarray, ...]
# - align_and_crop(def_img, ref1_img, ref2_img, point, patch_size, ...) -> tuple[np.ndarray, ...]


# --- Placeholders for Steps 5-7 (non-.raw parsing, implemented in later steps) ---

# Step 5: .ddf pixel reading
# - resolve_ddf_path(dzf_parse_result) -> Path
# - load_ddf_patches(ddf_path, defect_indices) -> dict[int, np.ndarray]

# Step 6: .patch pixel reading
# - load_patch_images(patch_path, defect_indices) -> dict[int, np.ndarray]

# Step 7: Retain extra DatasetReader functionality
# - Any DatasetReader .raw/.ddf/.patch features not used by arceus
#   remain accessible since all parsing lives in DatasetReader.
