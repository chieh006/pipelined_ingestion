import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar, Generic, TypeVar

import fsspec
import numpy as np

# Create logger for this module
logger = logging.getLogger(__name__)

# TypeVar for generic parse result
ParseResultT = TypeVar("ParseResultT")


def sanitize_uri_to_path(file_path: Path | str) -> str:
    """Convert URI-style paths to filesystem-native path strings.

    Examples
    --------
    - `s3://bucket/key` -> `bucket/key`
    - `/tmp/data.raw` -> `/tmp/data.raw`
    """
    path_str = str(file_path)
    if "://" not in path_str:
        return path_str
    _, sanitized = path_str.split("://", 1)
    return sanitized


class BaseParser(ABC, Generic[ParseResultT]):
    """Abstract base for all HMI file parsers. Pure extraction only.

    Type Parameters
    ---------------
    ParseResultT : TypeVar
        The type of structured result returned by parse().

    Class Attributes
    ---------------
    has_binary_images : bool
        True if the file format contains binary pixel data.
    is_multi_row : bool
        True if one file produces multiple rows in silver tables.
    file_extensions : tuple[str, ...]
        File extensions handled by this parser (e.g. `(".raw",)`).
    """

    has_binary_images: ClassVar[bool] = False
    is_multi_row: ClassVar[bool] = False
    file_extensions: ClassVar[tuple[str, ...]] = ()

    def __init__(
        self,
        file_path: Path | str,
        fs: fsspec.AbstractFileSystem | None = None,
    ) -> None:
        self.file_path = sanitize_uri_to_path(file_path)
        self.fs = fs or fsspec.filesystem("file")
        if not self.fs.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

    @abstractmethod
    def parse_metadata(self) -> list:
        """Extract metadata without loading binary data.

        Returns
        -------
        list
            List of dataclass instances (one per logical row).
        """

    @abstractmethod
    def parse(self, **kwargs) -> ParseResultT:
        """Parse the file and return the full structured result.

        Returns
        -------
        ParseResultT
            Parsed result as a structured model.
        """

    def stream_pixels(self, **kwargs) -> Iterator[np.ndarray]:
        """Yield pixel arrays.

        Only available when `has_binary_images` is True.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not contain binary image data.")

    def save_output(self, output_dir: Path | str, **kwargs) -> None:
        """Save parsed data to open-source formats."""
        logger.debug(
            f"{self.__class__.__name__} does not implement save_output(). "
            f"Use parse() to get the result directly."
        )

    @classmethod
    def write(cls, file_path: Path | str, parse_result: ParseResultT, **kwargs) -> None:
        """Write a parse result back into the proprietary binary format."""
        raise NotImplementedError(f"{cls.__name__} does not implement write().")
        