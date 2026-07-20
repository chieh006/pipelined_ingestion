import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import ClassVar

import fsspec

from asml.hmi.DatasetSchemas.bronze.defect_models import DzfDefect
from asml.hmi.DatasetSchemas.bronze.parse_results import (
    DzfFile,
    DzfParseResult,
)
from asml.hmi.DatasetSchemas.defect_enums import (
    DefectFileExtension,
    DefectTag,
    DzfXmlAttributes,
)

from .base_parser import BaseParser
from .defect_parser_util import create_parse_metadata, parse_defect_attribute

# Create logger for this module
logger = logging.getLogger(__name__)


_DZF_TEST_ELEMENT_PATH = "Test"
_DZF_DEFECT_LIST_ELEMENT_PATH = f"{_DZF_TEST_ELEMENT_PATH}/DefectList"
_DZF_IMAGE_PARAM_ELEMENT_PATH = "ImageParam"
_DZF_STRIPE_INFORMATION_ELEMENT_PATH = "TestArea/StripeInformation"
_LEGACY_DZF_IMAGE_METADATA_ATTRIBUTES: dict[str, str] = {
    DzfXmlAttributes.PIXEL_SIZE.value: DzfXmlAttributes.PIXEL_SIZE.value,
    DzfXmlAttributes.DOT_AVE.value: "DotAverage",
    DzfXmlAttributes.LINE_AVE.value: "LineAverage",
    DzfXmlAttributes.FRAME_AVE.value: "FrameAverage",
}


_DZF_IMAGE_METADATA_SOURCES: tuple[tuple[str, dict[str, str]], ...] = (
    (
        _DZF_IMAGE_PARAM_ELEMENT_PATH,
        {
            DzfXmlAttributes.PIXEL_SIZE.value: DzfXmlAttributes.PIXEL_SIZE.value,
            DzfXmlAttributes.DOT_AVE.value: DzfXmlAttributes.DOT_AVE.value,
            DzfXmlAttributes.LINE_AVE.value: DzfXmlAttributes.LINE_AVE.value,
            DzfXmlAttributes.FRAME_AVE.value: DzfXmlAttributes.FRAME_AVE.value,
        },
    ),
    (_DZF_STRIPE_INFORMATION_ELEMENT_PATH, _LEGACY_DZF_IMAGE_METADATA_ATTRIBUTES),
)


# --- DZF XML PARSING UTILITIES ---


def parse_dzf_defect_list(defect_list_elem, no_array: bool = False) -> list[DzfDefect]:
    """Parse defect list from XML DefectList element.

    Implements the core parsing logic from arceus/defects.py DefectListIO.parse()
    (lines 37-38).

    Parameters
    ----------
    defect_list_elem : xml.etree.ElementTree.Element
        XML <DefectList> element containing <Defect> children.
    no_array : bool
        If True, skip full array parsing (only extract confidence index).

    Returns
    -------
    list[DzfDefect]
        List of parsed DzfDefect objects with order set.

    Examples
    --------
    >>> import xml.etree.ElementTree as ET
    >>> root = ET.fromstring("<DefectList>...</DefectList>")
    >>> defects = parse_dzf_defect_list(root)
    """
    defects = []

    for i, defect_elem in enumerate(defect_list_elem):
        # Extract attributes that match known tags
        # Source: defects.py line 37
        parsed_attrs = {}
        for tag in DefectTag:
            tag_name = tag.value
            if tag_name in defect_elem.attrib:
                parsed_value = parse_defect_attribute(
                    tag_name, defect_elem.attrib[tag_name], no_array
                )
                parsed_attrs[tag_name] = parsed_value

        # Create DzfDefect object using factory method and set order
        # Source: defects.py line 38
        defect = DzfDefect.from_xml_kwargs(no_array, **parsed_attrs).set_order(i)
        defects.append(defect)

    return defects


def parse_class_type_table(root: ET.Element) -> dict[int, str]:
    """Parse ClassTypeTable from .dzf XML root.

    Extracts the ClassTypeTable element which contains a mapping of
    class type indices to human-readable names. Used by DL FE for
    defect classification.

    Parameters
    ----------
    root : xml.etree.ElementTree.Element
        XML root element from .dzf file.

    Returns
    -------
    dict[int, str]
        Mapping from class type index to class type name.
        Returns empty dict if ClassTypeTable is not present.
    """
    class_type_table = {}

    # Find ClassTypeTable element (may not exist in all .dzf files)
    class_table_elem = root.find("ClassTypeTable")
    if class_table_elem is None:
        logger.warning("No ClassTypeTable found in .dzf XML")
        return class_type_table

    # Parse each ClassType entry
    for class_type_elem in class_table_elem.findall("ClassType"):
        index_str = class_type_elem.get("Index")
        name_str = class_type_elem.get("Name")

        if index_str is not None and name_str is not None:
            try:
                index = int(index_str)
                class_type_table[index] = name_str
            except ValueError:
                logger.warning(f"Invalid ClassType Index '{index_str}' - skipping")

    logger.info(f"Parsed ClassTypeTable with {len(class_type_table)} entries")
    return class_type_table


def _extract_image_metadata(test_elem: ET.Element) -> tuple[float, int, int, int]:
    """Extract image metadata from supported DZF XML layouts."""
    for element_path, attribute_names in _DZF_IMAGE_METADATA_SOURCES:
        image_metadata = test_elem.find(element_path)
        if image_metadata is None:
            continue

        pixel_size = float(
            image_metadata.get(attribute_names[DzfXmlAttributes.PIXEL_SIZE.value], "0")
        )
        dot_avg = int(image_metadata.get(attribute_names[DzfXmlAttributes.DOT_AVE.value], "0"))
        line_avg = int(image_metadata.get(attribute_names[DzfXmlAttributes.LINE_AVE.value], "0"))
        frame_avg = int(image_metadata.get(attribute_names[DzfXmlAttributes.FRAME_AVE.value], "0"))
        return pixel_size, dot_avg, line_avg, frame_avg

    raise ValueError("Missing <ImageParam> or <TestArea>/<StripeInformation> element in .dzf XML")


class DzfFileParser(BaseParser[DzfParseResult]):
    """Parser for .dzf (DefectZoneFile) XML format.

    Parses .dzf XML files containing:
    - Test metadata (Id, PixelSize, DotAvg, LineAvg, FrameAvg)
    - Defect list with coordinates and review information
    - ResultFile reference to associated .ddf or .patch file
    """

    has_binary_images: ClassVar[bool] = False
    is_multi_row: ClassVar[bool] = True
    file_extensions: ClassVar[tuple[str, ...]] = (".dzf",)

    def parse_metadata(self) -> list[DzfDefect]:
        """Extract defect metadata from .dzf XML without full parse-result
        construction.

        Returns
        -------
        list[DzfDefect]
            One DzfDefect per defect element.
        """
        logger.info(f"Parsing .dzf metadata: {self.file_path}")

        with self.fs.open(self.file_path, "rb") as f:
            tree = ET.parse(f)
        root = tree.getroot()

        defect_list_elem = root.find(_DZF_DEFECT_LIST_ELEMENT_PATH)
        if defect_list_elem is None:
            raise ValueError("Missing <DefectList> element in .dzf XML")

        return parse_dzf_defect_list(defect_list_elem, no_array=False)

    def parse(self, **kwargs) -> DzfParseResult:
        """Parse .dzf XML file with full DefectListIO logic.

        Returns
        -------
        DzfParseResult
            Validated parse result containing DzfFile and metadata.

        Raises
        ------
        ValueError
            If XML structure is invalid, missing required elements,
            or ResultFile attribute is empty.
        FileNotFoundError
            If referenced ResultFile does not exist.

        Notes
        -----
        XML structure (from iddf.py lines 505-533 and defects.py lines 23-60)::

            <Result>
                            <Test Id="..." ResultFile="path/to/file.ddf" (or PatchFile="...")>
                                <ImageParam PixelSize="..." DotAve="..." LineAve="..."
                                                        FrameAve="..." />
                                <!-- or -->
                                <TestArea>
                                    <StripeInformation PixelSize="..." DotAverage="..."
                                                                         LineAverage="..." FrameAverage="..." />
                                </TestArea>
                <DefectList DefectCount="...">
                  <Defect DefectID="..." TestID="..." ImageID="..."
                          Threshold="..." Strength="..."
                          Pos="fX fY" ImagePos="imgX imgY"
                          ReviewType="0" Value="..." ValueExtended="..." />
                </DefectList>
              </Test>
            </Result>

        ResultFile Handling (from defects.py lines 43-60):

        - Must have either "ResultFile" or "PatchFile" attribute
        - If empty, raise ValueError
        - Supports .ddf and .patch file formats
        - Resolves relative paths from dzf directory
        """
        logger.info(f"Parsing .dzf XML file: {self.file_path}")

        # Parse XML file (from defects.py lines 26-27)
        with self.fs.open(self.file_path, "rb") as f:
            tree = ET.parse(f)
        root = tree.getroot()

        # Navigate to Test element
        # Source: defects.py line 30
        test_elem = root.find(_DZF_TEST_ELEMENT_PATH)
        if test_elem is None:
            raise ValueError("Missing <Test> element in .dzf XML")

        # Extract and validate ResultFile/PatchFile attribute
        # Priority: PatchFile (higher) > ResultFile (fallback)
        # Source: defects.py lines 48-54
        result_file_attr = test_elem.get(DzfXmlAttributes.PATCH_FILE.value)
        if result_file_attr is None:
            # Fall back to ResultFile if PatchFile doesn't exist
            result_file_attr = test_elem.get(DzfXmlAttributes.RESULT_FILE.value)
            if result_file_attr is None:
                raise ValueError(
                    "Missing 'PatchFile' or 'ResultFile' attribute in <Test> element. "
                    "At least one must be specified."
                )

        # Validate attribute is not empty
        if not result_file_attr or result_file_attr.strip() == "":
            raise ValueError(
                "Empty 'PatchFile'/'ResultFile' attribute in <Test> element. "
                "Must specify a valid .ddf or .patch file path."
            )

        # Resolve file path (handle relative paths)
        # Source: defects.py lines 56-61
        result_file_path = Path(result_file_attr)
        if not self.fs.exists(str(result_file_path)):
            # Try relative to dzf directory
            dzf_dir = Path(self.file_path).parent
            result_file_path = dzf_dir / result_file_attr
            if not self.fs.exists(str(result_file_path)):
                raise FileNotFoundError(
                    f"Referenced file not found: '{result_file_attr}'. "
                    f"Tried absolute path and relative to {dzf_dir}"
                )

        logger.info(f"Found ResultFile: {result_file_path.name}")

        pixel_size, dot_avg, line_avg, frame_avg = _extract_image_metadata(test_elem)

        # Extract DefectList
        # Source: defects.py line 30
        defect_list_elem = root.find(_DZF_DEFECT_LIST_ELEMENT_PATH)
        if defect_list_elem is None:
            raise ValueError("Missing <DefectList> element in .dzf XML")

        defect_count = int(defect_list_elem.get(DzfXmlAttributes.DEFECT_COUNT.value, "0"))

        logger.info(f"Found {defect_count} defects in DefectList")

        # Parse defects using modernized utility function
        # Source: defects.py lines 37-38
        # TestID for each defect is now parsed automatically from <Defect TestID="..."> attribute
        no_array = False  # Full feature parsing enabled
        parsed_defects = parse_dzf_defect_list(defect_list_elem, no_array=no_array)

        logger.info(f"Parsed {len(parsed_defects)} DzfDefect objects with features")

        # Parse ClassTypeTable (may not exist in all .dzf files)
        class_type_table = parse_class_type_table(root)

        # Create validated DzfFile model (metadata only)
        dzf_file = DzfFile(
            PixelSize=pixel_size,
            DotAvg=dot_avg,
            LineAvg=line_avg,
            FrameAvg=frame_avg,
            defect_count=defect_count,
            source_file=Path(self.file_path),
        )

        metadata = create_parse_metadata(self.file_path, DefectFileExtension.DZF.value, fs=self.fs)

        logger.info(f"Successfully parsed .dzf: {dzf_file.defect_count} defects")

        return DzfParseResult(
            dzf_file=dzf_file,
            metadata=metadata,
            parsed_defects=parsed_defects,
            result_file_attr=result_file_attr,
            result_file_resolved_path=str(result_file_path.resolve()),
            result_file_name=result_file_path.name,
            class_type_table=class_type_table,
        )

    @classmethod
    def write(cls, file_path: Path | str, parse_result: DzfParseResult, **kwargs) -> None:
        """Write a DzfParseResult back to .dzf XML format.

        Parameters
        ----------
        file_path : Path or str
            Target file path.
        parse_result : DzfParseResult
            Parse result to write.
        """
        dzf = parse_result.dzf_file

        root = ET.Element("Result")
        test_elem = ET.SubElement(root, "Test")
        test_elem.set("Id", "TEST001")
        test_elem.set(DzfXmlAttributes.RESULT_FILE.value, parse_result.result_file_attr)

        img_param = ET.SubElement(test_elem, "ImageParam")
        img_param.set(DzfXmlAttributes.PIXEL_SIZE.value, str(dzf.PixelSize))
        img_param.set(DzfXmlAttributes.DOT_AVE.value, str(dzf.DotAvg))
        img_param.set(DzfXmlAttributes.LINE_AVE.value, str(dzf.LineAvg))
        img_param.set(DzfXmlAttributes.FRAME_AVE.value, str(dzf.FrameAvg))

        defect_list = ET.SubElement(test_elem, "DefectList")
        defect_list.set(DzfXmlAttributes.DEFECT_COUNT.value, str(dzf.defect_count))

        for d in parse_result.parsed_defects:
            de = ET.SubElement(defect_list, "Defect")
            if d.defect_id is not None:
                de.set("DefectID", str(d.defect_id))
            if d.test_id is not None:
                de.set("TestID", str(d.test_id))
            if d.threshold is not None:
                de.set("Threshold", str(d.threshold))
            if d.review_type is not None:
                de.set("ReviewType", str(d.review_type))
            if d.value is not None:
                de.set("Value", " ".join(str(v) for v in d.value))
            if d.value_extended is not None:
                de.set("ValueExtended", " ".join(str(v) for v in d.value_extended))
            if d.f_d_feature is not None:
                de.set("fDFeature", " ".join(str(v) for v in d.f_d_feature))
            if d.defect_box is not None:
                de.set("DefectBox", " ".join(str(v) for v in d.defect_box))
            if d.index is not None:
                de.set("Index", str(d.index))
            if d.adc_type is not None:
                de.set("ADCType", str(d.adc_type))
            if d.number_hradc is not None:
                de.set("NumberHRADC", str(d.number_hradc))
            if d.value_hradc is not None:
                de.set("ValueHRADC", " ".join(str(v) for v in d.value_hradc))
            if d.result_index is not None:
                de.set("ResultIndex", str(d.result_index))

        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        write_fs = kwargs.get("fs") or fsspec.filesystem("file")
        with write_fs.open(str(file_path), "wb") as f:
            tree.write(f, encoding="utf-8", xml_declaration=True)
