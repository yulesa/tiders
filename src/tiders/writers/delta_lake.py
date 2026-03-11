"""Delta Lake writer backend.

Appends data to Delta tables using ``deltalake.write_deltalake`` with schema
merging enabled.
"""

import logging
from typing import Dict
from copy import deepcopy
import asyncio

import pyarrow as pa

from ..config import DeltaLakeWriterConfig
from ..writers.base import DataWriter

try:
    from deltalake import write_deltalake
except ImportError:
    write_deltalake = None

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    """Delta Lake writer that appends Arrow Tables as Delta table partitions.

    Each table is stored at ``<data_uri>/<table_name>/``. Writes are performed
    in append mode with schema merging. If an ``anchor_table`` is configured,
    it is written last.
    """

    def __init__(self, config: DeltaLakeWriterConfig):
        if write_deltalake is None:
            raise ImportError(
                "Delta Lake writer requires the deltalake package. "
                "Install it with: pip install tiders[delta_lake]"
            )
        self.config = deepcopy(config)
        self.config.data_uri = self.config.data_uri.rstrip("/")

    async def write_table(self, table_name: str, table_data: pa.Table) -> None:
        """Write a single table to its Delta Lake location. Skips empty tables."""
        if table_data.num_rows == 0:
            return

        _write = write_deltalake
        assert _write is not None
        await asyncio.to_thread(
            lambda: _write(
                table_or_uri=f"{self.config.data_uri}/{table_name}",
                data=table_data,
                partition_by=self.config.partition_by.get(table_name, None),
                mode="append",
                schema_mode="merge",
                storage_options=self.config.storage_options,
            )
        )

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Write all tables to Delta Lake, with the anchor table written last."""
        # insert into all tables except the anchor table in parallel
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.config.anchor_table:
                continue

            task = asyncio.create_task(
                self.write_table(table_name, table_data),
                name=f"write to {table_name}",
            )

            tasks.append(task)

        for task in tasks:
            await task

        # insert into anchor table after all other inserts are done
        if self.config.anchor_table is not None:
            table_data = data[self.config.anchor_table]
            await self.write_table(self.config.anchor_table, table_data)
