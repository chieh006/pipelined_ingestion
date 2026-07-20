import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from asml.hmi.DatasetSchemas.bronze.parse_results import (
    DdfParseResult,
    DefectParseResult,
    DzfParseResult,
    PatchParseResult,
)
from asml.hmi.DatasetSchemas.defect_enums import DefectFileExtension
from asml.hmi.DatasetSchemas.unified_schema_config import (
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE_VALIDATION_CONFIG,
    DEFAULT_VALIDATION_CONFIG,
    DEFECT_LEVEL_ATTRIBUTES,
    FILE_LEVEL_ATTRIBUTES,
    IMAGE_ATTRIBUTES,
    LEVEL_TO_DEFAULT_PRIORITY,
    PRIORITY_OVERRIDE,
    SOURCE_MAPPING,
    AttributeResolutionResult,
    ConsolidationResult,
    UnifiedAttributeName,
    ValidationMode,
)

if TYPE_CHECKING:
    from datetime import datetime

logger = logging.getLogger(__name__)

# Combined list of pre-defect attributes (defect-level + image-level)
_PER_DEFECT_ATTRIBUTES: list[UnifiedAttributeName] = DEFECT_LEVEL_ATTRIBUTES + IMAGE_ATTRIBUTES


@dataclass
class AttributeConsolidationResult:
    """Result container returned by attribute consolidation functions.

    Bundles three outputs of consolidation into a named
    structure instead of a positional tuple.

    Parameters
    ----------
    attributes : dict[str, Any]
        Mapping of attribute name to resolved value.
    source_attribution : dict[str, str]
        Mapping of attribute name to the source that provided the value.
    validation_warnings : list[str]
        Mismatch warnings accumulated during consolidation.
    """

    attributes: dict[str, Any]
    source_attribution: dict[str, str]
    validation_warnings: list[str]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _snake_to_pascal(name: str) -> str:
    """Convert a snake_case identifier to PascalCase.

    Parameters
    ----------
    name : str
        A snake_case string (e.g. ``""defect_pos_on_wafer_x"``).

    Returns
    -------
    str
        The PascalCase equivalent (e.g. ``""DefectPosOnWaferX"``).
    """
    return "".join(word.capitalize() for word in name.split("_"))


def _serialize_value(value: Any) -> Any:
    """Normalize a single value to a Polars compatible Python type.

    Convert ``pathlib.Path`` objects to POSIX strings and ``numpy.ndarray``
    objects to Python lists. Enum values are converted to their
    string representation. All other types pass through unchanged,
    including ``None``.

    Parameters
    ----------
    value : Any
        The raw field value to normalize.

    Returns
    -------
    Any
        Polars compatible value.
    """
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, np.ndarray):
        # TODO: Convert via pa.array(value) instead of .tolist() for
        # near zero-copy from numpy->PyArrow; Polars accepts PyArrow natively.
        return value.tolist()
    if isinstance(value, DefectFileExtension):
        return value.value
    return value


def get_priority_list_for_attribute(
    attr_name: UnifiedAttributeName,
) -> list[DefectFileExtension]:
    """Compute priority list for an attribute using centralized priority rules.

    This function implements the core priority resolution logic:
    1. Check PRIORITY_OVERRIDE for exceptions
    2. Fall back to LEVEL_TO_DEFAULT_PRIORITY based on attribute level
    3. Filter by sources that actually have the attribute (from SOURCE_MAPPING)

    Parameters
    ----------
    attr_name : UnifiedAttributeName
        The attribute to get priority list for.

    Returns
    -------
    List[DefectFileExtension]
        Priority-ordered list of sources for this attribute.
        Empty list if attribute not found in mappings.
    """
    # Check if attribute exists in SOURCE_MAPPING
    if attr_name not in SOURCE_MAPPING:
        return []

    # Step 1: Get base priority list (override or default)
    if attr_name in PRIORITY_OVERRIDE:
        # Use explicit override
        base_priority = list(PRIORITY_OVERRIDE[attr_name])
    else:
        # Use default priority based on attribute level
        if attr_name not in ATTRIBUTE_CATEGORY:
            # Unknown attribute level, return empty list
            return []

        attr_level = ATTRIBUTE_CATEGORY[attr_name]
        if attr_level not in LEVEL_TO_DEFAULT_PRIORITY:
            # Unknown level, return empty list
            return []

        base_priority = list(LEVEL_TO_DEFAULT_PRIORITY[attr_level])

    # Step 2: Filter by sources that actually have this attribute
    # Only include sources that are in SOURCE_MAPPING for this attribute
    available_source_types = set(SOURCE_MAPPING[attr_name].keys())
    filtered_priority = [source for source in base_priority if source in available_source_types]

    return filtered_priority


def get_nested_attribute(obj: Any, path: str) -> Any | None:
    """Safely retrieve a nested attribute using dot notation.

    This function traverses the object hierarchy using the dot-separated path
    and returns the final attribute value. If any intermediate attribute
    doesn't exist, returns None instead of raising an exception.

    Parameters
    ----------
    obj : Any
        The object to retrieve the attribute from (e.g., DzfParseResult instance).
    path : str
        Dot-separated path to the attribute (e.g., "dzf_file.defect_count").

    Returns
    -------
    Optional[Any]
        The attribute value, or None if any part of the path doesn't exist.
    """
    try:
        # Handle empty path - return the object itself
        if not path:
            return obj

        # Start with the root object
        current = obj

        # Split path into components
        # Example: "dzf_file.defect_count" -> ["dzf_file", "defect_count"]
        parts = path.split(".")

        # Traverse each component
        for part in parts:
            # Use getattr to access the next level
            # Example iteration 1: current = dzf_result, part = "dzf_file"
            #   -> current = dzf_result.dzf_file (now a DzfFile object)
            # Example iteration 2: current = dzf_file, part = "defect_count"
            #   -> current = dzf_file.defect_count (now an int: 42)
            current = getattr(current, part)

        return current

    except (AttributeError, TypeError):
        # AttributeError: attribute doesn't exist
        # TypeError: current is None and we tried to access an attribute
        return None


def validate_source_completeness(
    available_sources: dict[DefectFileExtension, Any],
) -> list[str]:
    """Validate that the combination of sources is trustworthy.

    Flags scenarios where cross-source validation is not possible,
    such as DZF-only ingestion where the file may contain filtered
    or subset data from application/algorithm manipulation.

    Parameters
    ----------
    available_sources : dict[DefectFileExtension, Any]
        Mapping of source enum to parse result objects.

    Returns
    -------
    list[str]
        List of warning messages about source completeness issues.
        Empty list if no issues detected.
    """
    warnings: list[str] = []

    if DefectFileExtension.DZF in available_sources and len(available_sources) == 1:
        warnings.append(
            "DZF-only ingestion: no cross-source validation possible. "
            "DZF files may contain filtered/subset data."
        )

    return warnings


def get_defects_list(
    source_enum: DefectFileExtension,
    parse_result: Any,
) -> list[Any] | None:
    """Extract the defects list from a parse result by source type.

    Each file format stores defects under a different accessor path.
    This function centralizes the accessor logic so it can be reused
    for both defect iteration and list-length validation.

    Parameters
    ----------
    source_enum : DefectFileExtension
        The file format type indicating which accessor path to use.
    parse_result : Any
        The parse result object for the given source type.

    Returns
    -------
    list[Any] | None
        The defects list from the parse result, or None if not found.
    """
    accessor_map: dict[DefectFileExtension, str] = {
        DefectFileExtension.DEFECT: "defect_file.defects",
        DefectFileExtension.PATCH: "patch_file.defects",
        DefectFileExtension.DDF: "ddf_file.defects",
        DefectFileExtension.DZF: "parsed_defects",
    }

    path = accessor_map.get(source_enum)
    if path is None:
        return None

    return get_nested_attribute(parse_result, path)


def resolve_attribute_with_validation(
    attr_name: UnifiedAttributeName,
    available_sources: dict[DefectFileExtension, Any],
    validation_mode: ValidationMode = ValidationMode.WARN_ON_MISMATCH,
    absolute_tolerance: float | None = None,
) -> AttributeResolutionResult:
    """Resolve a single attribute with optional cross-source validation.

    This function checks ALL available sources for the attribute
    and can detect/report mismatches between sources.

    Parameters
    ----------
    attr_name : UnifiedAttributeName
        Name of the unified attribute to resolve (as enum value).
        Example: UnifiedAttributeName.DEFECT_COUNT
    available_sources : Dict[DefectFileExtension, Any]
        Mapping of source enum to parse result objects.
        Example: {DefectFileExtension.DEFECT: defect_result, DefectFileExtension.PATCH: patch_result}
    validation_mode : ValidationMode
        How to handle mismatches between sources:
        - NO_VALIDATION: Fast mode, return first match (no validation)
        - WARN_ON_MISMATCH: Log warnings if sources disagree
        - FAIL_ON_MISMATCH: Raise ValueError if sources disagree
    absolute_tolerance : Optional[float]
        For numeric attributes, allow small absolute differences.
        Example: absolute_tolerance=0.01 means values within 0.01 of each other are considered equal.

    Returns
    -------
    AttributeResolutionResult
        Contains resolved value, source, all values, and mismatch flag.

    Raises
    ------
    ValueError
        If validation_mode=FAIL_ON_MISMATCH and sources have different values.
    """
    # Compute priority list dynamically using centralized system
    priority_list = get_priority_list_for_attribute(attr_name)

    # If no priority list, attribute is not configured for consolidation
    if not priority_list:
        return AttributeResolutionResult(None, None, {}, False)

    # ==========================================================================
    # PHASE 1: Collect values from ALL available sources
    # ==========================================================================
    all_source_values: dict[DefectFileExtension, Any] = {}

    for source_enum in priority_list:
        if source_enum not in available_sources:
            continue

        # Check if this source has a mapping for this attribute
        if source_enum not in SOURCE_MAPPING[attr_name]:
            continue

        parse_result_obj = available_sources[source_enum]
        accessor_path = SOURCE_MAPPING[attr_name][source_enum]
        value = get_nested_attribute(parse_result_obj, accessor_path)

        if value is not None:
            all_source_values[source_enum] = value

    # If no sources have this attribute, return None
    if not all_source_values:
        return AttributeResolutionResult(None, None, {}, False)

    # ==========================================================================
    # PHASE 2: Select primary value (highest-priority source)
    # ==========================================================================
    primary_source: DefectFileExtension | None = None
    primary_value: Any = None

    for source_enum in priority_list:
        if source_enum in all_source_values:
            primary_source = source_enum
            primary_value = all_source_values[source_enum]
            break

    # ==========================================================================
    # PHASE 3: Validate across sources (if requested)
    # ==========================================================================
    has_mismatch = False

    if validation_mode != ValidationMode.NO_VALIDATION and len(all_source_values) > 1:
        # Check if all values are equal (with optional tolerance for numeric types)
        if absolute_tolerance is not None:
            # For numeric comparison with absolute tolerance, check if all values are
            # within absolute_tolerance of each other (not just within absolute_tolerance
            # of a reference value)
            values_list = list(all_source_values.values())

            # Check if all values are numeric
            all_numeric = all(isinstance(v, (int, float)) for v in values_list)

            if all_numeric:
                # Compare each pair: within tolerance if max diff <= absolute_tolerance
                max_diff = max(values_list) - min(values_list)
                has_mismatch = max_diff > absolute_tolerance
            else:
                # If not all numeric, fall back to exact comparison
                unique_values = set()
                for value in values_list:
                    try:
                        unique_values.add(value)
                    except TypeError:
                        unique_values.add(str(value))
                has_mismatch = len(unique_values) > 1
        else:
            # Exact equality check (works for strings, ints, etc.)
            unique_values = set()
            for value in all_source_values.values():
                # Handle unhashable types (lists, numpy arrays)
                try:
                    unique_values.add(value)
                except TypeError:
                    # For unhashable types, convert to tuple or string
                    unique_values.add(str(value))

            has_mismatch = len(unique_values) > 1

        if has_mismatch:
            assert primary_source is not None
            # Build mismatch message
            mismatch_details = ", ".join(
                [f"{src.value}={val}" for src, val in all_source_values.items()]
            )

            if validation_mode == ValidationMode.WARN_ON_MISMATCH:
                logger.warning(
                    f"Attribute '{attr_name}' mismatch across sources: {mismatch_details}. "
                    f"Using value from {primary_source.value}: {primary_value}"
                )
            elif validation_mode == ValidationMode.FAIL_ON_MISMATCH:
                raise ValueError(
                    f"Attribute '{attr_name}' has mismatched values across sources: "
                    f"{mismatch_details}. Cannot consolidate with FAIL_ON_MISMATCH mode."
                )

    return AttributeResolutionResult(
        value=primary_value,
        primary_source=primary_source,
        all_source_values=all_source_values,
        has_mismatch=has_mismatch,
    )


def consolidate_file_level_attributes(
    available_sources: dict[DefectFileExtension, Any],
) -> AttributeConsolidationResult:
    """Consolidate all file-level attributes with cross-source validation.

    Parameters
    ----------
    available_sources : Dict[DefectFileExtension, Any]
        Mapping of source enum to parse result objects.

    Returns
    -------
    AttributeConsolidationResult
        Named result container with fields:
        - attributes: Maps attr_name to resolved value
        - source_attribution: Maps attr_name to source name that provided it
        - validation_warnings: List of mismatch warnings
    """
    consolidated_attributes: dict[str, Any] = {}
    source_attribution: dict[str, str] = {}
    validation_warnings: list[str] = []

    for attr_name in FILE_LEVEL_ATTRIBUTES:
        # Skip if attribute not in SOURCE_MAPPING
        if attr_name not in SOURCE_MAPPING:
            continue

        # Get validation config for this attribute
        config = ATTRIBUTE_VALIDATION_CONFIG.get(attr_name, DEFAULT_VALIDATION_CONFIG)
        mode = config.get("mode", ValidationMode.WARN_ON_MISMATCH)
        absolute_tolerance = config.get("absolute_tolerance", None)

        # Resolve with validation using centralized priority system
        result = resolve_attribute_with_validation(
            attr_name,
            available_sources,
            validation_mode=mode,
            absolute_tolerance=absolute_tolerance,
        )

        if result.value is not None:
            consolidated_attributes[attr_name.value] = result.value
            source_attribution[attr_name.value] = (
                result.primary_source.value if result.primary_source else "Unknown"
            )

            if result.has_mismatch:
                mismatch_details = ", ".join(
                    [f"{src.value}={val}" for src, val in result.all_source_values.items()]
                )
                validation_warnings.append(f"{attr_name.value}: mismatch - {mismatch_details}")

    return AttributeConsolidationResult(
        consolidated_attributes, source_attribution, validation_warnings
    )


# =============================================================================
# VECTORIZED DEFECT-LEVEL CONSOLIDATION
# =============================================================================


def _extract_source_columns(
    source_enum: DefectFileExtension,
    defects_list: list[Any],
) -> dict[str, list[Any]]:
    """Extract all mapped defect-level attributes from a source as column
    lists.

    Iterates over each defect in the source's defect list and extracts
    attribute values using the accessor paths defined in SOURCE_MAPPING.
    Values are serialized for Polars compatibility.

    Parameters
    ----------
    source_enum : DefectFileExtension
        The file format source type.
    defects_list : list[Any]
        List of defect objects from this source.

    Returns
    -------
    dict[str, list[Any]]
        Mapping of ``"{source_value}__{attr_value}"`` to list of
        per-defect values. Lists are aligned by defect index.
    """
    source_attrs = [
        attr
        for attr in _PER_DEFECT_ATTRIBUTES
        if attr in SOURCE_MAPPING and source_enum in SOURCE_MAPPING[attr]
    ]

    columns: dict[str, list[Any]] = {}
    for attr in source_attrs:
        col_name = f"{source_enum.value}__{attr.value}"
        accessor = SOURCE_MAPPING[attr][source_enum]
        columns[col_name] = [
            _serialize_value(get_nested_attribute(defect, accessor)) for defect in defects_list
        ]

    return columns


def _build_joined_source_dataframe(
    available_sources: dict[DefectFileExtension, Any],
    defect_count: int,
) -> tuple[pl.DataFrame, list[str]]:
    """Build a single DataFrame joining all source defect columns on
    defect_index.

    For each available source, extracts per-defect columns and joins them
    on ``defect_index`` using a left join. Sources with fewer defects than
    ``defect_count`` contribute nulls for missing rows.

    Parameters
    ----------
    available_sources : dict[DefectFileExtension, Any]
        Mapping of source enum to parse result objects.
    defect_count : int
        Expected number of defects (determines base DataFrame size).

    Returns
    -------
    tuple[pl.DataFrame, list[str]]
        - Joined DataFrame with all source-prefixed columns.
        - List of warning messages about sources with fewer defects.
    """
    warnings: list[str] = []

    if defect_count == 0:
        return pl.DataFrame({"defect_index": pl.Series([], dtype=pl.Int64)}), warnings

    base_df = pl.DataFrame({"defect_index": list(range(defect_count))})

    for source_enum, parse_result in available_sources.items():
        defects_list = get_defects_list(source_enum, parse_result)
        if defects_list is None or len(defects_list) == 0:
            continue

        if len(defects_list) < defect_count:
            warnings.append(
                f"Source {source_enum.value} has only {len(defects_list)} "
                f"defects, expected {defect_count}"
            )

        columns = _extract_source_columns(source_enum, defects_list)
        columns["defect_index"] = list(range(len(defects_list)))

        source_df = pl.DataFrame(columns)
        base_df = base_df.join(source_df, on="defect_index", how="left")

    return base_df, warnings


def _coalesce_by_priority(
    joined_df: pl.DataFrame,
) -> list[pl.Expr]:
    """Build coalesce expressions for each attribute using priority order.

    For each per-defect attribute, produces a ``pl.coalesce(...)``
    expression that selects the first non-null value across sources
    in priority order.

    Parameters
    ----------
    joined_df : pl.DataFrame
        DataFrame with source-prefixed columns from
        :func:`_build_joined_source_dataframe`.

    Returns
    -------
    list[pl.Expr]
        One coalesce expression per attribute that has at least one
        source column present in the DataFrame.
    """
    coalesce_exprs: list[pl.Expr] = []
    available_cols = set(joined_df.columns)

    for attr in _PER_DEFECT_ATTRIBUTES:
        if attr not in SOURCE_MAPPING:
            continue

        priority = get_priority_list_for_attribute(attr)
        source_cols = [
            f"{src.value}__{attr.value}"
            for src in priority
            if f"{src.value}__{attr.value}" in available_cols
        ]

        if source_cols:
            coalesce_exprs.append(pl.coalesce([pl.col(c) for c in source_cols]).alias(attr.value))

    return coalesce_exprs


def _build_attribution_expressions(
    joined_df: pl.DataFrame,
) -> list[pl.Expr]:
    """Build source attribution expressions for each attribute.

    For each attribute, creates a ``pl.when(...).then(...)`` chain
    that records which source provided the resolved value, checking
    sources in priority order.

    Parameters
    ----------
    joined_df : pl.DataFrame
        DataFrame with source-prefixed columns.

    Returns
    -------
    list[pl.Expr]
        One expression per attribute, aliased as
        ``"{attr_value}__source"``.
    """
    attribution_exprs: list[pl.Expr] = []
    available_cols = set(joined_df.columns)

    for attr in _PER_DEFECT_ATTRIBUTES:
        if attr not in SOURCE_MAPPING:
            continue

        priority = get_priority_list_for_attribute(attr)
        source_cols = [
            (src, f"{src.value}__{attr.value}")
            for src in priority
            if f"{src.value}__{attr.value}" in available_cols
        ]

        if not source_cols:
            continue

        # Build when/then chain: highest priority first
        expr: pl.Expr | None = None
        for src, col_name in reversed(source_cols):
            if expr is None:
                expr = (
                    pl.when(pl.col(col_name).is_not_null())
                    .then(pl.lit(src.value))
                    .otherwise(pl.lit(None).cast(pl.Utf8))
                )
            else:
                expr = (
                    pl.when(pl.col(col_name).is_not_null()).then(pl.lit(src.value)).otherwise(expr)
                )

        if expr is not None:
            attribution_exprs.append(expr.alias(f"{attr.value}__source"))

    return attribution_exprs


def _detect_mismatches_vectorized(
    joined_df: pl.DataFrame,
) -> list[str]:
    """Detect cross-source mismatches for attributes with validation enabled.

    Uses vectorized Polars operations to compare attribute values across
    sources. Only checks attributes whose validation mode is
    ``WARN_ON_MISMATCH`` or ``FAIL_ON_MISMATCH``.

    Parameters
    ----------
    joined_df : pl.DataFrame
        DataFrame with source-prefixed columns.

    Returns
    -------
    list[str]
        Validation warning messages for detected mismatches.

    Raises
    ------
    ValueError
        If a ``FAIL_ON_MISMATCH`` attribute has mismatched values.
    """
    warnings: list[str] = []
    available_cols = set(joined_df.columns)

    if joined_df.is_empty():
        return warnings

    for attr in _PER_DEFECT_ATTRIBUTES:
        config = ATTRIBUTE_VALIDATION_CONFIG.get(attr, DEFAULT_VALIDATION_CONFIG)
        mode = config.get("mode", ValidationMode.WARN_ON_MISMATCH)

        if mode == ValidationMode.NO_VALIDATION:
            continue

        if attr not in SOURCE_MAPPING:
            continue

        priority = get_priority_list_for_attribute(attr)
        source_cols = [
            (src, f"{src.value}__{attr.value}")
            for src in priority
            if f"{src.value}__{attr.value}" in available_cols
        ]

        if len(source_cols) < 2:
            continue

        tolerance = config.get("absolute_tolerance")

        # Compare pairwise: check rows where both sources are non-null
        first_src, first_col = source_cols[0]
        for other_src, other_col in source_cols[1:]:
            both_present = joined_df.filter(
                pl.col(first_col).is_not_null() & pl.col(other_col).is_not_null()
            )

            if both_present.is_empty():
                continue

            if tolerance is not None:
                # Numeric tolerance comparison
                mismatch_mask = (
                    pl.col(first_col).cast(pl.Float64) - pl.col(other_col).cast(pl.Float64)
                ).abs() > tolerance
            else:
                # Exact equality comparison
                mismatch_mask = pl.col(first_col) != pl.col(other_col)

            mismatched_rows = both_present.filter(mismatch_mask)
            if mismatched_rows.is_empty():
                continue

            mismatch_count = len(mismatched_rows)
            # Get sample values for the warning message
            sample_first = mismatched_rows[first_col][0]
            sample_other = mismatched_rows[other_col][0]

            msg = (
                f"{attr.value}: {mismatch_count} row(s) mismatch between "
                f"{first_src.value}={sample_first} and "
                f"{other_src.value}={sample_other}"
            )

            if mode == ValidationMode.FAIL_ON_MISMATCH:
                raise ValueError(
                    f"Attribute '{attr.value}' has mismatched values across sources: "
                    f"{msg}. Cannot consolidate with FAIL_ON_MISMATCH mode."
                )

            logger.warning(f"Attribute '{attr.value}' mismatch: {msg}")
            warnings.append(msg)

    return warnings


def _build_file_level_columns(
    file_attrs: dict[UnifiedAttributeName | str, Any],
) -> list[pl.Expr]:
    """Build Polars broadcast literal expressions for file-level attributes.

    Parameters
    ----------
    file_attrs : dict[UnifiedAttributeName | str, Any]
        Mapping of file-level attribute name to resolved value.

    Returns
    -------
    list[pl.Expr]
        One ``pl.lit(value).alias(PascalCaseName)`` expression per
        attribute, suitable for ``DataFrame.with_columns``.
    """
    exprs: list[pl.Expr] = []
    for attr_name, value in file_attrs.items():
        col_name = _snake_to_pascal(
            attr_name.value if isinstance(attr_name, UnifiedAttributeName) else attr_name
        )
        exprs.append(pl.lit(_serialize_value(value)).alias(col_name))
    return exprs


# =============================================================================
# MAIN CONSOLIDATION FUNCTION
# =============================================================================


def consolidate_parse_results(
    dzf_result: DzfParseResult | None = None,
    ddf_result: DdfParseResult | None = None,
    defect_result: DefectParseResult | None = None,
    patch_result: PatchParseResult | None = None,
) -> ConsolidationResult:
    """
    Main consolidation function: merge all available parse results with validation.

    This is the primary entry point for the consolidation process.
    Designed to be called from DefectResultManager.run_pipeline() or
    directly as a Dagster op/asset.

    Parameters
    ----------
    dzf_result : Optional[DzfParseResult]
        Parsed .dzf file result (if available).
    ddf_result : Optional[DdfParseResult]
        Parsed .ddf file result (if available).
    defect_result : Optional[DefectParseResult]
        Parsed .defect file result (if available).
    patch_result : Optional[PatchParseResult]
        Parsed .patch file result (if available).

    Returns
    -------
    ConsolidationResult
        Complete consolidation result with unified schema, metadata, and validation info.

    Raises
    ------
    ValueError
        If no parse results are provided.
        If FAIL_ON_MISMATCH validation fails for any critical attribute.
    """
    # ==========================================================================
    # Step 1: Build available_sources dict from non-None inputs
    # ==========================================================================
    available_sources: dict[DefectFileExtension, Any] = {}
    if dzf_result is not None:
        available_sources[DefectFileExtension.DZF] = dzf_result
    if ddf_result is not None:
        available_sources[DefectFileExtension.DDF] = ddf_result
    if defect_result is not None:
        available_sources[DefectFileExtension.DEFECT] = defect_result
    if patch_result is not None:
        available_sources[DefectFileExtension.PATCH] = patch_result

    # Validate at least one source is provided
    if not available_sources:
        raise ValueError(
            "At least one parse result must be provided. "
            "Got: dzf_result=None, ddf_result=None, defect_result=None, patch_result=None"
        )

    logger.info(
        f"Consolidating parse results from {len(available_sources)} sources: "
        f"{[s.value for s in available_sources]}"
    )

    # Validate source completeness (tampered DZF protection)
    completeness_warnings = validate_source_completeness(available_sources)
    for warning in completeness_warnings:
        logger.warning(warning)

    # ==========================================================================
    # Step 2: Consolidate file-level attributes with validation
    # ==========================================================================
    file_result = consolidate_file_level_attributes(available_sources)
    file_attrs = file_result.attributes
    file_attribution = file_result.source_attribution
    file_warnings = file_result.validation_warnings

    # ==========================================================================
    # Step 3: Compute derived timestamp attributes (EARLIEST and LATEST)
    # ==========================================================================
    parsed_at_timestamps: list[datetime] = []

    # Collect all available parsed_at timestamps
    if UnifiedAttributeName.DZF_PARSED_AT in file_attrs:
        parsed_at_timestamps.append(file_attrs[UnifiedAttributeName.DZF_PARSED_AT])
    if UnifiedAttributeName.DDF_PARSED_AT in file_attrs:
        parsed_at_timestamps.append(file_attrs[UnifiedAttributeName.DDF_PARSED_AT])
    if UnifiedAttributeName.DEFECT_PARSED_AT in file_attrs:
        parsed_at_timestamps.append(file_attrs[UnifiedAttributeName.DEFECT_PARSED_AT])
    if UnifiedAttributeName.PATCH_PARSED_AT in file_attrs:
        parsed_at_timestamps.append(file_attrs[UnifiedAttributeName.PATCH_PARSED_AT])

    # Compute earliest and latest if we have any timestamps
    if parsed_at_timestamps:
        file_attrs[UnifiedAttributeName.EARLIEST_PARSED_AT] = min(parsed_at_timestamps)
        file_attrs[UnifiedAttributeName.LATEST_PARSED_AT] = max(parsed_at_timestamps)
        file_attribution[UnifiedAttributeName.EARLIEST_PARSED_AT] = "Computed"
        file_attribution[UnifiedAttributeName.LATEST_PARSED_AT] = "Computed"

        logger.debug(
            f"Computed derived timestamps: earliest={file_attrs[UnifiedAttributeName.EARLIEST_PARSED_AT]}, "
            f"latest={file_attrs[UnifiedAttributeName.LATEST_PARSED_AT]}"
        )

    # Determine defect_count for iteration
    defect_count = file_attrs.get(UnifiedAttributeName.DEFECT_COUNT, 0)

    # Cross-validate actual defect list lengths against defect_count
    defect_list_warnings: list[str] = []
    for source_enum, parse_result in available_sources.items():
        actual_list = get_defects_list(source_enum, parse_result)
        if actual_list is not None and len(actual_list) != defect_count:
            msg = (
                f"Source {source_enum.value} has {len(actual_list)} defects "
                f"but consolidated defect_count is {defect_count}"
            )
            logger.warning(msg)
            defect_list_warnings.append(msg)

    # ==========================================================================
    # Step 4: Vectorized defect-level consolidation
    # ==========================================================================
    joined_df, partial_coverage_warnings = _build_joined_source_dataframe(
        available_sources,
        defect_count,
    )

    # Detect mismatch across sources (vectorized)
    defect_mismatch_warnings = _detect_mismatches_vectorized(joined_df)

    # Build coalesce expressions for priority resolution
    coalesce_exprs = _coalesce_by_priority(joined_df)
    attribution_exprs = _build_attribution_expressions(joined_df)

    if coalesce_exprs:
        defect_df = joined_df.select([pl.col("defect_index"), *coalesce_exprs, *attribution_exprs])
    else:
        defect_df = (
            pl.DataFrame({"defect_index": list(range(defect_count))})
            if defect_count > 0
            else pl.DataFrame()
        )

    # Rename columns to PascalCase
    rename_map: dict[str, str] = {}
    for col in defect_df.columns:
        if col.endswith("__source"):
            # Attribute columns: keep as is (excluded from final output later)
            continue
        rename_map[col] = _snake_to_pascal(col)

    defect_df = defect_df.rename(rename_map)

    # ==========================================================================
    # Step 5: Add file-level attributes as broadcase columns
    # ==========================================================================
    if not defect_df.is_empty() and file_attrs:
        file_exprs = _build_file_level_columns(file_attrs)
        defect_df = defect_df.with_columns(file_exprs)

    # Add available_sources as file-level column
    if not defect_df.is_empty():
        defect_df = defect_df.with_columns(
            pl.lit([s.value for s in available_sources]).alias("AvailableSources")
        )

    # ==========================================================================
    # Step 6: Combine all validation warnings
    # ==========================================================================
    all_warnings = (
        completeness_warnings
        + defect_list_warnings
        + file_warnings
        + partial_coverage_warnings
        + defect_mismatch_warnings
    )

    logger.info(
        f"Consolidation completed: {defect_count} defects from "
        f"{len(available_sources)} sources with {len(all_warnings)} warnings"
    )

    # ==========================================================================
    # Step 8: Build and return ConsolidationResult
    # ==========================================================================
    return ConsolidationResult(
        defect_df=defect_df,
        consolidation_metadata={
            "source_count": len(available_sources),
            "sources": [s.value for s in available_sources],
            "file_level_attributes_consolidated": [
                a.value if isinstance(a, UnifiedAttributeName) else a for a in file_attrs
            ],
        },
        validation_warnings=all_warnings,
        validation_passed=True,  # If we got here, no FAIL_ON_MISMATCH errors occurred
        file_level_attribution=file_attribution,
    )
