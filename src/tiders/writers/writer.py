"""Factory for instantiating the correct :class:`DataWriter` from a :class:`Writer` config."""

from ..writers.base import DataWriter
from ..config import (
    Writer,
    WriterKind,
    IcebergWriterConfig,
    ClickHouseWriterConfig,
    DeltaLakeWriterConfig,
    PyArrowDatasetWriterConfig,
    DuckdbWriterConfig,
)
import logging

logger = logging.getLogger(__name__)


def create_writer(writer: Writer) -> DataWriter:
    """Create and return a concrete :class:`DataWriter` based on the writer kind.

    Args:
        writer: A :class:`Writer` instance pairing a :class:`WriterKind` with
            its backend-specific configuration.

    Returns:
        A concrete :class:`DataWriter` subclass ready to accept data.

    Raises:
        ValueError: If ``writer.kind`` does not match any supported backend.
    """
    match writer.kind:
        case WriterKind.ICEBERG:
            from . import iceberg
            assert isinstance(writer.config, IcebergWriterConfig)
            return iceberg.Writer(writer.config)
        case WriterKind.CLICKHOUSE:
            from . import clickhouse
            assert isinstance(writer.config, ClickHouseWriterConfig)
            return clickhouse.Writer(writer.config)
        case WriterKind.DELTA_LAKE:
            from . import delta_lake
            assert isinstance(writer.config, DeltaLakeWriterConfig)
            return delta_lake.Writer(writer.config)
        case WriterKind.PYARROW_DATASET:
            from . import pyarrow_dataset
            assert isinstance(writer.config, PyArrowDatasetWriterConfig)
            return pyarrow_dataset.Writer(writer.config)
        case WriterKind.DUCKDB:
            from . import duckdb
            assert isinstance(writer.config, DuckdbWriterConfig)
            return duckdb.Writer(writer.config)
        case _:
            raise ValueError(f"Invalid writer kind: {writer.kind}")
