import os
from pathlib import Path
from typing import Generic, TypeVar, List, Optional

from .exceptions import DbExistsError, DbCorruption, TableException, SegmentSizeError
from .segment import Segment
from ..serializer import Serializer

V = TypeVar('V')


class Table(Generic[V]):
    """
    A class to represent a table in the database.
    """

    TOMBSTONE = 'tombstone'

    def __init__(
            self,
            name: str,
            serializer: Serializer,
            path: Optional[Path] = None,
    ) -> None:
        self.name = name
        self._serializer = serializer
        self._table_path = path
        self._segments: List[Segment] = []

    @staticmethod
    def from_path(path: Path, serializer: Serializer[V]) -> 'Table[V]':
        table_name = path.name
        return Table(table_name, serializer, path)

    def init(self, override=False) -> None:
        """
        Initialise the table.

        :return:
        """

        self._table_path = Path(os.environ["SIMPLE_DB_PATH"])

        if os.path.isdir(self._table_location) and not override:
            raise DbExistsError(f"Table {self.name} already exists.")
        else:
            if not override or not os.path.isdir(self._table_location):
                os.mkdir(self._table_location)
                segment = Segment(self._table_location / f"s1.smt")
                self._segments.append(segment)
            else:
                # Try to load any pre-existing segments
                self._segments = sorted(Segment.from_path(self._table_location / Path(file)) for file
                                        in os.listdir(self._table_location))

                if not self._segments:
                    segment = Segment(self._table_location / f"s1.smt")
                    self._segments.append(segment)

    def delete(self) -> None:
        """
        Delete the table.

        :return:
        """

        if os.path.isdir(self._table_location):
            for f in Path(self._table_location).glob('*.smt'):
                os.remove(f)
            os.rmdir(self._table_location)
        else:
            raise DbExistsError(f"Table {self.name} doesn't exists.")

    def set(self, key: str, value: V) -> None:
        """
        Set a value in the table.

        :param key:
        :param value:
        :return:
        """

        if not self._segments:
            raise DbCorruption("No segments")
        else:
            serialized = self._serializer.encode(value)
            try:
                self._segments[-1].write(key, serialized)
            except SegmentSizeError:
                next_segment_id = str(len(self._segments) + 1)
                segment = Segment(self._table_location / f"s{next_segment_id}.smt")
                self._segments.append(segment)
                self._segments[-1].write(key, serialized)

    def get(self, key: str) -> Optional[V]:
        """
        Get a value from the table.

        :param key:
        :return:
        """

        if not self._segments:
            raise DbCorruption("No segments in this table")
        else:
            for segment in reversed(self._segments):
                value = segment.read(key)
                if value is not None:
                    if value != self.TOMBSTONE:
                        return value
                    else:
                        return None
            raise TableException(f"Table {self.name} doesn't contain {key}")

    def remove(self, key: str) -> None:
        """
        Remove a value from the table.

        :param key:
        :return:
        """

        self._segments[-1].write(key, "tombstone")

    @property
    def _table_location(self) -> Path:
        return self._table_path / self.name
