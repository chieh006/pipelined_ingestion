"""DL FE pipeline orchestrator.

This module coordinates .dzf + .ddf/.patch parsing for DL FE training
workflows. It handles pipeline-specific post-processing such as label dict
construction, DOI/nuisance partitioning, patch image loading, and feature
vector extraction.
"""

import struct
from pathlib import Path

import cv2
import numpy as np

from asml.hmi.DatasetReader.parsers.ddf_parser import DdfFileParser
from asml.hmi.DatasetReader.parsers.dzf_parser import DzfFileParser
from asml.hmi.DatasetReader.parsers.patch_parser import PatchFileParser
from asml.hmi.DatasetSchemas.bronze.parse_results import DzfParseResult


def load_dzf_with_labels(dzf_path: str | Path) -> tuple[DzfParseResult, dict]:
    """Load .dzf file and construct label dictionaries for DL FE training.

    This function parses the .dzf file and creates pipeline-specific label
    structures used by the DL FE training workflow.

    Parameters
    ----------
    dzf_path : str | Path
        Path to the .dzf file.

    Returns
    -------
    tuple[DzfParseResult, dict]
        - DzfParseResult: Full parse result from DatasetReader
        - dict: Label dictionary with structure:
          {
              "labels": {defect_id: [review_type, patch_index], ...},
              "patch_indices": {defect_id: patch_index, ...},
              "review_types": {defect_id: review_type, ...}
          }
    """
    parser = DzfFileParser(dzf_path)
    result = parser.parse()

    # Construct label dictionaries from parsed defects
    labels = {}
    patch_indices = {}
    review_types = {}

    for defect in result.parsed_defects:
        defect_id = defect.defect_id
        review_type = defect.review_type
        # patch_index is defect.index - 1 (0-based indexing)
        patch_index = defect.index - 1

        labels[defect_id] = [review_type, patch_index]
        patch_indices[defect_id] = patch_index
        review_types[defect_id] = review_type

    label_dict = {
        "labels": labels,
        "patch_indices": patch_indices,
        "review_types": review_types,
    }

    return result, label_dict


def separate_doi_nuisance(review_types: dict, doi_values: list[int] | int) -> tuple[list, list]:
    """Partition defect IDs into DOI and nuisance lists.

    Parameters
    ----------
    review_types : dict
        Dictionary mapping defect_id -> review_type integer.
    doi_values : list[int] | int
        ReviewType value(s) that represent DOI defects (required).
        Can be a single integer or list of integers.
        Must be explicitly specified based on your dataset convention.

        Common conventions:
        - Standard: doi_values=1 (or [1, 2, ...]) where 0=Nuisance, 1+=DOI
        - Legacy: doi_values=0 where 0=DOI, 1=Nuisance

    Returns
    -------
    tuple[list, list]
        - List of DOI defect IDs
        - List of nuisance defect IDs

    Notes
    -----
    **No default convention is assumed.** You must explicitly specify which
    ReviewType value(s) represent DOI defects for your specific dataset.

    Common conventions observed in practice:
    - Standard convention: ReviewType 0=Nuisance, 1/2/...=DOI subtypes
    - Legacy convention: ReviewType 0=DOI, 1=Nuisance

    Always verify your dataset's ReviewType convention before using this function.
    """
    # Normalize doi_values to a set for efficient lookup
    doi_set = {doi_values} if isinstance(doi_values, int) else set(doi_values)

    doi_ids = []
    nuisance_ids = []

    for defect_id, review_type in review_types.items():
        if review_type in doi_set:
            doi_ids.append(defect_id)
        else:
            nuisance_ids.append(defect_id)

    return doi_ids, nuisance_ids


# =============================================================================
# .ddf/.patch Pixel Reading Orchestration
# =============================================================================


def _get_patch_parser(patch_path: Path) -> DdfFileParser | PatchFileParser:
    """Instantiate the correct parser based on file extension.

    Parameters
    ----------
    patch_path : Path
        Path to the .ddf or .patch file.

    Returns
    -------
    DdfFileParser | PatchFileParser
        Appropriate parser instance.

    Raises
    ------
    ValueError
        If the file extension is not .ddf or .patch.
    """
    suffix = patch_path.suffix.lower()
    if suffix == ".ddf":
        return DdfFileParser(str(patch_path))
    elif suffix == ".patch":
        return PatchFileParser(str(patch_path))
    raise ValueError(f"Unsupported patch file format: {suffix!r}. Expected .ddf or .patch")


def resolve_patch_file(dzf_parse_result: DzfParseResult) -> Path:
    """Resolve the patch file path (.ddf or .patch) from a .dzf parse result.

    Replaces: hmdlex/DataIO/read_dzf.py:get_patch_filename()

    Parameters
    ----------
    dzf_parse_result : DzfParseResult
        Parsed .dzf result containing the result file reference.

    Returns
    -------
    Path
        Absolute path to the .ddf or .patch file.

    Raises
    ------
    FileNotFoundError
        If the resolved patch file does not exist.
    """
    resolved_path = Path(dzf_parse_result.result_file_resolved_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Patch file not found: {resolved_path}")
    return resolved_path


def load_patch_images_batch(
    patch_path: Path,
    patch_index_dict: dict[int, int],
    defect_ids: list[int],
    normalize: bool = True,
) -> dict[int, np.ndarray]:
    """Batch-load 3-channel patch images for multiple defects.

    Replaces: hmdlex/image_utils.py:read_in_image_from_file_batch()

    Dispatches to .ddf or .patch parser based on file extension.
    Returns images as [C, H, W] float arrays normalized to [0, 1].

    Parameters
    ----------
    patch_path : Path
        Path to the .ddf or .patch file.
    patch_index_dict : dict[int, int]
        Mapping of defect_id to 0-based patch index in the file.
    defect_ids : list[int]
        List of defect IDs to load.
    normalize : bool
        If True, normalize uint8 [0, 255] to float [0, 1].

    Returns
    -------
    dict[int, np.ndarray]
        {defect_id: np.ndarray shape [C, H, W]}
        C=3 (ref1, defect, ref2), dtype float64 if normalized, uint8 otherwise.
    """
    indices = [patch_index_dict[d_id] for d_id in defect_ids]
    parser = _get_patch_parser(patch_path)
    result: dict[int, np.ndarray] = {}
    for d_id, patches in zip(defect_ids, parser.stream_pixels(indices=indices)):
        img: np.ndarray = patches.astype(np.float64) / 255.0 if normalize else patches
        result[d_id] = img
    return result


def load_patch_images_single(
    patch_path: Path,
    patch_index: int,
    normalize: bool = True,
) -> np.ndarray:
    """Load a single 3-channel patch image by index.

    Replaces: hmdlex/image_utils.py:read_in_image_from_file()

    Parameters
    ----------
    patch_path : Path
        Path to the .ddf or .patch file.
    patch_index : int
        0-based patch index.
    normalize : bool
        If True, normalize uint8 [0, 255] to float [0, 1].

    Returns
    -------
    np.ndarray
        Shape [3, H, W], dtype float64 if normalized, uint8 otherwise.
    """
    parser = _get_patch_parser(patch_path)
    patches = next(iter(parser.stream_pixels(indices=[patch_index])))
    return patches.astype(np.float64) / 255.0 if normalize else patches


def load_single_channel_batch(
    patch_path: Path,
    patch_index_dict: dict[int, int],
    defect_ids: list[int],
    channel: int = 1,
    normalize: bool = True,
) -> dict[int, np.ndarray]:
    """Batch-load single-channel (defect-only) patch images.

    Replaces: hmdlex/image_utils.py:read_in_single_image_from_file_batch()

    Parameters
    ----------
    patch_path : Path
        Path to the .ddf or .patch file.
    patch_index_dict : dict[int, int]
        Mapping of defect_id to 0-based patch index.
    defect_ids : list[int]
        List of defect IDs to load.
    channel : int
        Channel index to extract (default 1 = defect image).
        Falls back to channel 0 if fewer than ``channel + 1`` channels available.
    normalize : bool
        If True, normalize to float [0, 1].

    Returns
    -------
    dict[int, np.ndarray]
        {defect_id: np.ndarray shape [1, H, W]}
    """
    indices = [patch_index_dict[d_id] for d_id in defect_ids]
    parser = _get_patch_parser(patch_path)
    result: dict[int, np.ndarray] = {}
    for d_id, patches in zip(defect_ids, parser.stream_pixels(indices=indices)):
        actual_channel = channel if patches.shape[0] >= channel + 1 else 0
        single = patches[actual_channel : actual_channel + 1]  # shape [1, H, W]
        img: np.ndarray = single.astype(np.float64) / 255.0 if normalize else single
        result[d_id] = img
    return result


def cut_patch_to_target_size(
    img: np.ndarray,
    target_size: int,
) -> np.ndarray:
    """Center-crop or resize patch to target dimensions.

    Replaces: hmdlex/image_utils.py:cut_patch_to_target_size()

    Parameters
    ----------
    img : np.ndarray
        Input image of shape [C, H, W].
    target_size : int
        Target spatial dimension (both H and W).

    Returns
    -------
    np.ndarray
        Image of shape [C, target_size, target_size].
    """
    img_h = img.shape[1]  # [C, H, W]
    if img_h > target_size:
        center = img_h // 2
        half = target_size // 2
        start = center - half
        return img[:, start : start + target_size, start : start + target_size]
    if img_h < target_size:
        img_resized = np.zeros((img.shape[0], target_size, target_size), dtype=img.dtype)
        for c in range(img.shape[0]):
            img_resized[c] = cv2.resize(
                img[c], (target_size, target_size), interpolation=cv2.INTER_LINEAR
            )
        return img_resized
    return img


def extract_defect_box_from_feature(
    f_feature: list[float],
    imgsize: int,
) -> list[int]:
    """Extract defect bounding box from fFeature[103] byte reinterpretation.

    Replaces: hmpy/iddf.py:read_nth_defect_from_ddf(..., get_defectbox=True)

    The defect box is encoded as a 4-byte float at fFeature[103].
    The 4 bytes are reinterpreted as unsigned chars [y0, x0, y1, x1].

    Parameters
    ----------
    f_feature : list[float]
        128-element feature vector from DefectRecord.
    imgsize : int
        Patch image size for bounds validation.

    Returns
    -------
    list[int]
        [x0, y0, x1, y1] or [0, 0, 0, 0] if invalid (any value
        negative or >= imgsize).
    """
    if len(f_feature) < 104:
        return [0, 0, 0, 0]
    raw_bytes = struct.pack("<f", f_feature[103])
    y0, x0, y1, x1 = struct.unpack("4B", raw_bytes)
    if any(v >= imgsize for v in (y0, x0, y1, x1)):
        return [0, 0, 0, 0]
    return [x0, y0, x1, y1]
