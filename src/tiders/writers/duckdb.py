import logging
from typing import Dict
import pyarrow as pa
from .base import DataWriter
from ..config import DuckdbWriterConfig
import asyncio
import duckdb

logger = logging.getLogger(__name__)


def _downcast_decimal256(table: pa.Table) -> pa.Table:
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
    def __init__(self, config: DuckdbWriterConfig):
        self.connection = config.connection
        self.first_push = True
        logger.warning(
            "DuckDB does not support Decimal256.\n"
            "Tiders will try to automatically downcast Decimal256 columns to Decimal128(38, 0). "
            "Any data exceeding 38 digits of precision will cause a failure. "
            "To handle this, add a cast_by_type step before the writer with `allow_cast_fail=True` (overflowing values become null) "
            "or cast to a different type that preserves the full value (e.g. string, binary)."
        )

    def push_data_impl(self, data: Dict[str, pa.Table]) -> None:
        data = {name: _downcast_decimal256(t) for name, t in data.items()}
        self.connection.begin()

        if self.first_push:
            for table_name, table_data in data.items():
                try:
                    self.connection.table(table_name)
                    self.connection.sql(
                        f"INSERT INTO {table_name} SELECT * FROM table_data"
                    )
                except duckdb.CatalogException:
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
        await asyncio.to_thread(self.push_data_impl, data)
