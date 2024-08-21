import os
from pathlib import Path
from typing import Generic, TypeVar, Optional

from .exceptions import PartitionExistsError, TableExistsError
from .table_partition import TablePartition
from ..serializer import Serializer

V = TypeVar('V')

DEFAULT_PARTITIONS = 10


class Table(Generic[V]):
    """
    A class to represent a table in the database.
    """

    def __init__(
            self,
            name: str,
            serializer: Serializer,
    ) -> None:
        self.name = name
        self.serializer = serializer
        try:
            self.partitions = self._init_partitions()
        except PartitionExistsError:
            raise TableExistsError(f"{self.name} table already exists")

    @staticmethod
    def from_path(path: Path, serializer: Serializer[V]) -> 'Table[V]':
        table_name = path.name
        # TODO: Find all partitions
        return Table(table_name, serializer)

    def delete(self) -> None:
        """
        Delete the table.

        :return:
        """

        for partition in self.partitions:
            partition.delete()

        self.path.rmdir()

    def set(self, key: str, value: V) -> None:
        """
        Set a value in the table.

        :param key:
        :param value:
        :return:
        """

        partition_id = hash(key) % DEFAULT_PARTITIONS
        self.partitions[partition_id].set(key, value)

    def get(self, key: str) -> Optional[V]:
        """
        Get a value from the table.

        :param key:
        :return:
        """

        partition_id = hash(key) % DEFAULT_PARTITIONS
        return self.partitions[partition_id].get(key)

    @property
    def path(self) -> Path:
        return Path(os.environ["SIMPLE_DB_PATH"]) / self.name

    def _init_partitions(self):
        db_path = Path(self.path.parent)
        table_name = self.path.name
        return [
            TablePartition(db_path, table_name, partition_id, self.serializer)
            for partition_id in range(DEFAULT_PARTITIONS)
        ]

    def _partition_path(self, partition_id: int) -> Path:
        return self.path / f"prt{partition_id}"
