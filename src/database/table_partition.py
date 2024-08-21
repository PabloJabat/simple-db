import os
import re
from pathlib import Path
from typing import List, Generic, TypeVar, Optional

from .exceptions import DbExistsError, SegmentSizeError, PartitionExistsError
from .segment import Segment
from ..serializer import Serializer

V = TypeVar('V')



class TablePartition(Generic[V]):
    """This table partition is in charge of a set of keys"""

    FIRST_SEGMENT_ID = 0

    def __init__(
            self,
            db_path: Path,
            table_name: str,
            partition_id: int,
            serializer: Serializer,
            segments: List[Segment] = None
    ) -> None:
        """
        If the segments for this partition are not provided, it is assumed that there is still not an existing directory
        for the partition, and therefore, the directory for this partition will be created.

        :param db_path:
        :param table_name:
        :param partition_id:
        :param serializer:
        :param segments:
        """
        if segments is None:
            segments = []
        self._db_path = db_path
        self._table_name = table_name
        self._partition_id = partition_id
        self._serializer = serializer
        if segments:
            self._segments: List[Segment] = segments
        else:
            try:
                # create directory
                # TODO: This line will fail if the folders exist already
                self.path.mkdir(parents=True)
            except FileExistsError:
                raise PartitionExistsError(f"{self.path} exists")
            self._segments = []

    @staticmethod
    def from_path(path: Path, serializer: Serializer[V]) -> 'TablePartition[V]':
        """
        Create a table partition from a path. It is assumed that the path exists.
        TODO: Create tests for when the directory for the table partition doesn't exist

        :param path:
        :param serializer:
        :return:
        """

        partition_id = re.match("^prt([0-9])$", path.parent.name).group(1)
        table_name = path.parent.parent.name
        db_path = path.parent.parent.parent
        segments = sorted(Segment.from_path(Path(file)) for file in os.listdir(path))
        return TablePartition(
            db_path,
            table_name,
            partition_id,
            serializer,
            segments
        )

    def delete(self) -> None:
        """
        Delete the partition table.

        :return:
        """

        if self.path.is_dir():
            for segment_path in Path(self.path).glob('*.sgmt'):
                segment_path.unlink()
            self.path.rmdir()
        else:
            raise DbExistsError(f"Table {self._table_name} doesn't exists.")

    def set(self, key: str, value: V) -> None:
        """
        Set a value in the table partition.

        :param key:
        :param value:
        :return:
        """

        if not self._segments:
            segment = Segment(
                self._db_path,
                self._table_name,
                self._partition_id,
                self.FIRST_SEGMENT_ID
            )
            self._segments.append(segment)

        serialized = self._serializer.encode(value)
        try:
            self._segments[-1].write(key, serialized)
        except SegmentSizeError:
            next_segment_id = len(self._segments) + 1
            segment = Segment(self._db_path, self._table_name, self._partition_id, next_segment_id)
            self._segments.append(segment)
            self._segments[-1].write(key, serialized)

    def get(self, key: str) -> Optional[V]:
        """
        Get a value from the table partition.

        :param key:
        :return:
        """

        for segment in reversed(self._segments):
            value = segment.read(key)
            if value is not None:
                return value

        return None

    @property
    def path(self) -> Path:
        return self._db_path / self._table_name / f"prt{self._partition_id}"

    @property
    def segments(self) -> List[Segment]:
        return self._segments
