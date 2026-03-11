"""Step that joins block fields into other tables."""

from typing import Dict

import pyarrow as pa

from ..config import JoinBlockDataConfig


def execute(data: Dict[str, pa.Table], config: JoinBlockDataConfig) -> Dict[str, pa.Table]:
    """Join block columns into the specified tables using a left outer join.

    For each target table, the block table (identified by
    ``config.block_table_name``) is joined on the configured key columns.
    Column collisions are prefixed with `<block_table_name>_`.

    When ``config.tables`` is ``None``, all tables except the block table
    itself are joined.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`JoinBlockDataConfig` controlling which tables to
            join and which columns to join on.

    Returns:
        A new data dictionary with block columns joined into the target tables.
    """
    block_table = data.get(config.block_table_name)
    if block_table is None:
        raise Exception(
            f"{config.block_table_name} table not found in tables. Existing tables: {list(data.keys())}."
        )

    table_names = (
        [n for n in data if n != config.block_table_name]
        if config.tables is None
        else config.tables
    )

    join_key_set = set(config.join_blocks_on)

    for table_name in table_names:
        left = data[table_name]
        left_cols = set(left.schema.names)

        # Rename only block columns that clash with the left table (excluding join keys).
        renamed_block = block_table
        cols_to_rename = {
            name: f"{config.block_table_name}_{name}"
            for name in block_table.schema.names
            if name not in join_key_set and name in left_cols
        }
        if cols_to_rename:
            renamed_block = renamed_block.rename_columns(
                [
                    cols_to_rename.get(name, name)
                    for name in renamed_block.schema.names
                ]
            )

        data[table_name] = left.join(
            renamed_block,
            keys=config.join_left_on,
            right_keys=config.join_blocks_on,
            join_type="left outer",
            coalesce_keys=True,
        )

    return data
