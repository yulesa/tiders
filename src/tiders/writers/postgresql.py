"""PostgreSQL writer backend.

Automatically creates tables from Arrow schemas and inserts data using the
``psycopg`` v3 async client and the COPY protocol for efficient bulk inserts.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, LiteralString, cast as type_cast
from ..config import PostgresqlWriterConfig

import psycopg.sql as sql
import pyarrow as pa

from .base import DataWriter

logger = logging.getLogger(__name__)


def pyarrow_type_to_postgresql(dt: pa.DataType) -> str:
    """Convert a PyArrow data type to its PostgreSQL SQL type string.

    Supports scalar types (bool, integers, floats, strings, binary, dates,
    timestamps, decimals). Raises for unsupported compound types (List, Struct,
    Map) — use a step to flatten or drop those columns first.

    Args:
        dt: A PyArrow ``DataType`` to convert.

    Returns:
        The equivalent PostgreSQL type name (e.g. ``"BIGINT"``, ``"NUMERIC(76,0)"``).

    Raises:
        Exception: If the PyArrow type has no known PostgreSQL mapping (e.g. List,
            Struct, Map). See docs for which raw blockchain fields are affected.
    """
    if pa.types.is_boolean(dt):
        return "BOOLEAN"
    elif pa.types.is_int8(dt):
        return "SMALLINT"
    elif pa.types.is_int16(dt):
        return "SMALLINT"
    elif pa.types.is_int32(dt):
        return "INTEGER"
    elif pa.types.is_int64(dt):
        return "BIGINT"
    elif pa.types.is_uint8(dt):
        return "SMALLINT"
    elif pa.types.is_uint16(dt):
        return "SMALLINT"
    elif pa.types.is_uint32(dt):
        return "BIGINT"
    elif pa.types.is_uint64(dt):
        return "NUMERIC(20,0)"
    elif pa.types.is_float16(dt):
        return "REAL"
    elif pa.types.is_float32(dt):
        return "REAL"
    elif pa.types.is_float64(dt):
        return "DOUBLE PRECISION"
    elif pa.types.is_string(dt) or pa.types.is_large_string(dt):
        return "TEXT"
    elif pa.types.is_binary(dt) or pa.types.is_large_binary(dt):
        return "BYTEA"
    elif pa.types.is_date32(dt) or pa.types.is_date64(dt):
        return "DATE"
    elif pa.types.is_timestamp(dt):
        return "TIMESTAMP"
    elif pa.types.is_decimal128(dt):
        dt = type_cast(pa.Decimal128Type, dt)
        return f"NUMERIC({dt.precision},{dt.scale})"
    elif pa.types.is_decimal256(dt):
        dt = type_cast(pa.Decimal256Type, dt)
        return f"NUMERIC({dt.precision},{dt.scale})"
    elif pa.types.is_list(dt) or pa.types.is_large_list(dt):
        raise Exception(
            f"Unsupported PyArrow type for PostgreSQL: {dt}. "
            "List columns cannot be written directly to PostgreSQL. "
            "Use a step to flatten or drop these columns first. "
            "See the PostgreSQL writer docs for the full list of affected raw fields."
        )
    elif pa.types.is_struct(dt):
        raise Exception(
            f"Unsupported PyArrow type for PostgreSQL: {dt}. "
            "Struct columns cannot be written directly to PostgreSQL. "
            "Use a step to flatten or drop these columns first. "
            "See the PostgreSQL writer docs for the full list of affected raw fields."
        )
    elif pa.types.is_map(dt):
        raise Exception(
            f"Unsupported PyArrow type for PostgreSQL: {dt}. "
            "Map columns cannot be written directly to PostgreSQL. "
            "Use a step to flatten or drop these columns first. "
            "See the PostgreSQL writer docs for the full list of affected raw fields."
        )
    else:
        raise Exception(f"Unimplemented PyArrow type for PostgreSQL: {dt}")


def _arrow_value_to_python(value: Any, dt: pa.DataType) -> Any:
    """Convert a single Arrow scalar value to a Python type suitable for psycopg COPY.

    Args:
        value: The raw value from an Arrow column (may be None).
        dt: The Arrow DataType of the column.

    Returns:
        A Python-native value compatible with the matching PostgreSQL type.
    """
    if value is None:
        return None
    if pa.types.is_decimal128(dt) or pa.types.is_decimal256(dt):
        return Decimal(str(value))
    if pa.types.is_uint64(dt):
        return Decimal(int(value))
    if pa.types.is_boolean(dt):
        return bool(value)
    if pa.types.is_binary(dt) or pa.types.is_large_binary(dt):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        return bytes(value)
    return value


class Writer(DataWriter):
    """PostgreSQL writer that auto-creates tables and inserts Arrow data via COPY.

    On the first :meth:`push_data` call, tables are created using
    ``CREATE TABLE IF NOT EXISTS`` derived from the Arrow schema. Subsequent
    calls stream data via the PostgreSQL COPY protocol for efficient bulk inserts.
    If an ``anchor_table`` is configured, it is always inserted last to provide
    ordering guarantees.
    """

    def __init__(self, config: PostgresqlWriterConfig):
        self.connection = config.connection
        self.schema = config.schema
        self.anchor_table = config.anchor_table
        self.create_tables = config.create_tables
        self.first_insert = True

    async def _create_table_if_not_exists(
        self, table_name: str, schema: pa.Schema
    ) -> None:
        """Create a PostgreSQL table from an Arrow schema using IF NOT EXISTS."""
        col_defs = sql.SQL(",\n    ").join(
            sql.SQL("{} {}").format(
                sql.Identifier(field.name),
                sql.SQL(
                    type_cast(LiteralString, pyarrow_type_to_postgresql(field.type))
                ),
            )
            for field in schema
        )
        qualified_id = sql.SQL("{}.{}").format(
            sql.Identifier(self.schema), sql.Identifier(table_name)
        )
        ddl = sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n    {}\n)").format(
            qualified_id, col_defs
        )
        logger.debug(f"creating table with: {ddl.as_string(self.connection)}")
        async with self.connection.cursor() as cur:
            await cur.execute(ddl)
        await self.connection.commit()

    async def _copy_table(self, table_name: str, table: pa.Table) -> None:
        """Stream Arrow table data into PostgreSQL via the COPY protocol."""
        qualified_id = sql.SQL("{}.{}").format(
            sql.Identifier(self.schema), sql.Identifier(table_name)
        )
        col_ids = sql.SQL(", ").join(sql.Identifier(f.name) for f in table.schema)
        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT TEXT, NULL '\\N')").format(
            qualified_id, col_ids
        )

        async with self.connection.cursor() as cur:
            async with cur.copy(copy_stmt) as copy:
                num_rows = table.num_rows
                columns = [table.column(i) for i in range(table.num_columns)]
                types = [table.schema.field(i).type for i in range(table.num_columns)]

                for row_idx in range(num_rows):
                    row_values = []
                    for col, dt in zip(columns, types):
                        raw = col[row_idx].as_py()
                        val = _arrow_value_to_python(raw, dt)
                        if val is None:
                            row_values.append("\\N")
                        elif isinstance(val, bool):
                            row_values.append("t" if val else "f")
                        elif isinstance(val, bytes):
                            row_values.append("\\\\x" + val.hex())
                        elif isinstance(val, Decimal):
                            row_values.append(str(val))
                        else:
                            row_values.append(
                                str(val)
                                .replace("\t", "\\t")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r")
                                .replace("\\", "\\\\")
                            )
                    await copy.write_row(row_values)

        await self.connection.commit()

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Insert Arrow Tables into PostgreSQL, creating tables on the first call if needed."""
        if self.create_tables and self.first_insert:
            for table_name, table_data in data.items():
                await self._create_table_if_not_exists(table_name, table_data.schema)
            self.first_insert = False

        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.anchor_table:
                continue
            tasks.append(
                asyncio.create_task(
                    self._copy_table(table_name, table_data),
                    name=f"write to {table_name}",
                )
            )

        for task in tasks:
            await task

        if self.anchor_table is not None and self.anchor_table in data:
            await self._copy_table(self.anchor_table, data[self.anchor_table])
