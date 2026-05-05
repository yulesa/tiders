"""CSV writer backend.

Writes tables as CSV files using ``pyarrow.csv.write_csv``.
Each table is written to ``<base_dir>/<table_name>.csv``. On successive pushes
the file is appended to; the header is written only on the first push for each
table. All tables except the ``anchor_table`` are written in parallel.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import asyncio

from .base import DataWriter
from ..config import CsvWriterConfig

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    """CSV writer that appends to ``<base_dir>/<table_name>.csv`` on each push.

    The header row is written only on the first push for each table. If an
    ``anchor_table`` is configured, it is written last.
    """

    def __init__(self, config: CsvWriterConfig):
        self.config = config
        self._written: Set[str] = set()

    def _file_path(self, table_name: str) -> Path:
        base_dir = Path(self.config.base_dir)
        if self.config.create_dir:
            base_dir.mkdir(parents=True, exist_ok=True)
        return base_dir / f"{table_name}.csv"

    def _write_table(self, table_name: str, table_data: pa.Table) -> None:
        """Append a table to its CSV file, writing the header only on first push."""
        file_path = self._file_path(table_name)
        first_write = table_name not in self._written
        write_options = pa_csv.WriteOptions(  # pyright: ignore[reportPrivateImportUsage]
            include_header=self.config.include_header and first_write,
            delimiter=self.config.delimiter,
        )
        mode = "wb" if first_write else "ab"
        with open(str(file_path), mode) as f:
            pa_csv.write_csv(table_data, f, write_options=write_options)  # pyright: ignore[reportPrivateImportUsage]
        self._written.add(table_name)

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Append all tables to their CSV files, with the anchor table written last."""
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.config.anchor_table:
                continue
            task = asyncio.create_task(
                asyncio.to_thread(self._write_table, table_name, table_data),
                name=f"write csv {table_name}",
            )
            tasks.append(task)

        for task in tasks:
            await task

        if self.config.anchor_table and self.config.anchor_table in data:
            await asyncio.to_thread(
                self._write_table,
                self.config.anchor_table,
                data[self.config.anchor_table],
            )

    async def read_max_block(self, table: str, column: str) -> Optional[int]:
        """Return MAX(column) from the CSV file, or None if missing or empty."""

        def _query() -> Optional[int]:
            file_path = Path(self.config.base_dir) / f"{table}.csv"
            if not file_path.is_file():
                return None
            schema = pa_csv.open_csv(str(file_path)).schema  # pyright: ignore[reportPrivateImportUsage]
            if column not in schema.names:
                raise ValueError(
                    f"Checkpoint column '{column}' not found in CSV table '{table}'. "
                    f"Check the 'column' field in your checkpoint config."
                )
            arrow_table = pa_csv.read_csv(
                str(file_path),
                convert_options=pa_csv.ConvertOptions(include_columns=[column]),  # pyright: ignore[reportPrivateImportUsage]
            )
            if arrow_table.num_rows == 0:
                return None
            value = pc.max(arrow_table.column(column)).as_py()
            return int(value) if value is not None else None

        return await asyncio.to_thread(_query)
