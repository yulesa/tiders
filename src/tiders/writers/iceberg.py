"""Apache Iceberg writer backend.

Creates Iceberg tables from Arrow schemas and appends data using the
``pyiceberg`` catalog API.
"""

import logging
from typing import Dict, Optional
from ..writers.base import DataWriter
from ..config import IcebergWriterConfig
import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    """Iceberg writer that creates tables on first write and appends data.

    The writer creates the Iceberg namespace on initialisation (if it doesn't
    already exist) and lazily creates tables using the Arrow schema of the
    first incoming batch.
    """

    def __init__(self, config: IcebergWriterConfig):
        logger.debug("Initializing Iceberg writer...")

        if config.catalog is not None:
            catalog = config.catalog
        else:
            from pyiceberg.catalog import load_catalog

            catalog = load_catalog(
                "tiders",
                type=config.catalog_type,
                uri=config.catalog_uri,
                warehouse=config.warehouse,
            )

        try:
            catalog.create_namespace(
                config.namespace,
            )
        except Exception as e:
            logger.warning(f"Error creating namespace: {e}")

        logger.debug(f"Created namespace: {config.namespace}")

        self.namespace = config.namespace
        self.first_write = True
        self.write_location = config.write_location or config.warehouse
        self.catalog = catalog

    async def write_table(self, table_name: str, arrow_table: pa.Table) -> None:
        """Append an Arrow Table to an existing Iceberg table."""
        logger.debug(f"Writing table: {table_name}")

        table_identifier = f"{self.namespace}.{table_name}"

        iceberg_table = self.catalog.load_table(table_identifier)
        iceberg_table.append(arrow_table)

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Create tables on first call (if needed) and append data to Iceberg."""
        if self.first_write:
            for table_name, table_data in data.items():
                table_identifier = f"{self.namespace}.{table_name}"
                self.catalog.create_table_if_not_exists(
                    identifier=table_identifier,
                    schema=table_data.schema,
                    location=self.write_location,
                )

            self.first_write = False

        for table_name, arrow_table in data.items():
            await self.write_table(table_name, arrow_table)

    async def read_max_block(self, table: str, column: str) -> Optional[int]:
        """Return MAX(column) from the Iceberg table, or None if missing or empty."""
        try:
            from pyiceberg.exceptions import NoSuchTableError
        except ImportError:
            NoSuchTableError = Exception  # type: ignore[assignment,misc]

        try:
            iceberg_table = self.catalog.load_table(f"{self.namespace}.{table}")
            arrow_table = iceberg_table.scan(selected_fields=(column,)).to_arrow()
            if arrow_table.num_rows == 0:
                return None
            value = pc.max(arrow_table.column(column)).as_py()  # type: ignore[attr-defined]
            return int(value) if value is not None else None
        except NoSuchTableError:
            return None
