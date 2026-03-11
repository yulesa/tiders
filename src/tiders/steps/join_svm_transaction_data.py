"""Step that joins SVM transaction fields into other tables."""

from typing import Dict

import pyarrow as pa

from ..config import JoinSvmTransactionDataConfig


def execute(
    data: Dict[str, pa.Table], config: JoinSvmTransactionDataConfig
) -> Dict[str, pa.Table]:
    """Join SVM transaction columns into the specified tables using a left outer join.

    For each target table, the transactions table (identified by
    ``config.tx_table_name``) is joined on the configured key columns.
    Column collisions are prefixed with `<tx_table_name>_`.

    When ``config.tables`` is ``None``, all tables except the transactions
    table itself are joined.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`JoinSvmTransactionDataConfig` controlling which
            tables to join and which columns to join on.

    Returns:
        A new data dictionary with transaction columns joined into the target
        tables.
    """
    tx_table = data.get(config.tx_table_name)
    if tx_table is None:
        raise Exception(
            f"{config.tx_table_name} table not found in tables. Existing tables: {list(data.keys())}."
        )

    table_names = (
        [n for n in data if n != config.tx_table_name]
        if config.tables is None
        else config.tables
    )

    join_key_set = set(config.join_transactions_on)

    for table_name in table_names:
        left = data[table_name]
        left_cols = set(left.schema.names)

        # Rename only transaction columns that clash with the left table (excluding join keys).
        renamed_tx = tx_table
        cols_to_rename = {
            name: f"{config.tx_table_name}_{name}"
            for name in tx_table.schema.names
            if name not in join_key_set and name in left_cols
        }
        if cols_to_rename:
            renamed_tx = renamed_tx.rename_columns(
                [
                    cols_to_rename.get(name, name)
                    for name in renamed_tx.schema.names
                ]
            )

        data[table_name] = left.join(
            renamed_tx,
            keys=config.join_left_on,
            right_keys=config.join_transactions_on,
            join_type="left outer",
            coalesce_keys=True,
        )

    return data
