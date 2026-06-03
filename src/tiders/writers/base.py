"""Abstract base class for all data writer backends."""

from abc import ABC, abstractmethod
from typing import Dict, Optional
import logging
import pyarrow as pa

logger = logging.getLogger(__name__)


class DataWriter(ABC):
    """Abstract base class that all writer backends must implement.

    Subclasses must override :meth:`push_data` and :meth:`read_max_block`.
    """

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        """Persist a batch of named PyArrow Tables to the target storage.

        This method is called once per ingestion batch and may be called
        multiple times during a pipeline run.

        Args:
            data: A dictionary mapping table names to PyArrow Tables.
        """
        pass

    @abstractmethod
    async def read_max_block(self, table: str, column: str) -> Optional[int]:
        """Return the maximum value of ``column`` in ``table``, or ``None``.

        Used by the checkpoint feature to determine the last written block so
        the pipeline can resume from the next one.  Implementations must return
        ``None`` when the table does not exist or contains no rows, so the
        pipeline falls back to its configured ``from_block``.

        Args:
            table: Name of the destination table to query.
            column: Name of the block-number column.

        Returns:
            The maximum block number found, or ``None`` if unavailable.
        """
        pass

    async def close(self) -> None:
        """Release any resources held by the writer (connections, sessions).

        Called once when a pipeline run finishes. The default is a no-op;
        writers that own network clients should override this to close them so
        underlying connection pools are released cleanly instead of being
        reclaimed during interpreter/event-loop teardown.
        """
        return None
