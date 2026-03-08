"""Abstract base class for all data writer backends."""

from abc import ABC, abstractmethod
from typing import Dict
import logging
import pyarrow as pa

logger = logging.getLogger(__name__)


class DataWriter(ABC):
    """Abstract base class that all writer backends must implement.

    Subclasses must override :meth:`push_data` to persist a batch of tables to
    their target storage system.
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
