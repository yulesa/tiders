"""DuckDB writer backend.

Inserts Arrow data into DuckDB tables, auto-creating them on the first push.
"""

import logging
from typing import Dict, Optional
import pyarrow as pa
from .base import DataWriter
from ..config import DuckdbWriterConfig
import asyncio

try:
    import duckdb
except ImportError:
    duckdb = None

logger = logging.getLogger(__name__)


def _downcast_decimal256(table: pa.Table) -> pa.Table:
    """Downcast any ``Decimal256`` columns to ``Decimal128(38, scale)``.

    DuckDB does not support ``Decimal256``. This function finds such columns
    and casts them to ``Decimal128`` with a maximum precision of 38. Values
    exceeding 38 digits will cause a cast error unless handled upstream.

    Args:
        table: A PyArrow Table that may contain ``Decimal256`` columns.

    Returns:
        The table with ``Decimal256`` columns downcast, or the original table
        if none were found.
    """
    new_fields = []
    needs_cast = False
    for f in table.schema:
        if pa.types.is_decimal256(f.type):
            new_fields.append(
                f.with_type(pa.decimal128(min(f.type.precision, 38), f.type.scale))
            )
            needs_cast = True
        else:
            new_fields.append(f)
    if not needs_cast:
        return table
    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return table.cast(new_schema, safe=True)


class Writer(DataWriter):
    """DuckDB writer that creates tables on the first push and inserts thereafter.

    All inserts are wrapped in a transaction. ``Decimal256`` columns are
    automatically downcast to ``Decimal128(38, 0)`` since DuckDB does not
    support 256-bit decimals.
    """

    def __init__(self, config: DuckdbWriterConfig):
        if duckdb is None:
            raise ImportError(
                "DuckDB writer requires the duckdb package. "
                "Install it with: pip install tiders[duckdb]"
            )
        if config.connection is not None:
            self.connection = config.connection
        elif config.path is not None:
            from pathlib import Path

            Path(config.path).parent.mkdir(parents=True, exist_ok=True)
            self.connection = duckdb.connect(database=config.path)
        else:
            raise ValueError(
                "DuckdbWriterConfig requires either 'connection' or 'path'."
            )
        self.first_push = True
        logger.warning(
            "DuckDB does not support Decimal256. "
            "Tiders will try to automatically downcast Decimal256 columns to Decimal128(38, 0). "
            "Any data exceeding 38 digits of precision will cause a failure. "
            "To handle this, add a cast_by_type step before the writer with `allow_cast_fail=True` (overflowing values become null) "
            "or cast to a different type that preserves the full value (e.g. string, binary)."
        )

    def push_data_impl(self, data: Dict[str, pa.Table]) -> None:
        """Synchronous implementation of data insertion into DuckDB.

        On the first call, tables are created via ``CREATE TABLE ... AS SELECT``
        if they don't exist, or inserted into if they do. Subsequent calls
        insert directly.
        """
        data = {name: _downcast_decimal256(t) for name, t in data.items()}
        self.connection.begin()

        if self.first_push:
            for table_name, table_data in data.items():
                try:
                    self.connection.table(table_name)
                    self.connection.sql(
                        f"INSERT INTO {table_name} SELECT * FROM table_data"
                    )
                except duckdb.CatalogException:  # type: ignore[union-attr]
                    logger.debug(
                        f"creating table {table_name} as it doesn't exist in the database yet"
                    )
                    self.connection.sql(
                        f"CREATE TABLE {table_name} AS SELECT * FROM table_data"
                    )

                # ignore lint warning relating to unused variable
                # this variable is used in the sql query string
                _ = table_data

            self.first_push = False
        else:
            for table_name, table_data in data.items():
                self.connection.sql(
                    f"INSERT INTO {table_name} SELECT * FROM table_data"
                )
                # ignore lint warning relating to unused variable
                # this variable is used in the sql query string
                _ = table_data

        self.connection.commit()

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Insert data into DuckDB, running the blocking operation in a background thread."""
        await asyncio.to_thread(self.push_data_impl, data)

    async def read_max_block(self, table: str, column: str) -> Optional[int]:
        """Return MAX(column) from table, or None if the table is missing or empty."""

        def _query() -> Optional[int]:
            try:
                result = self.connection.execute(
                    f"SELECT MAX({column}) FROM {table}"
                ).fetchone()
                if result is None or result[0] is None:
                    return None
                return int(result[0])
            except duckdb.CatalogException:  # type: ignore[union-attr]
                return None

        return await asyncio.to_thread(_query)
