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

    def __init__(self, name: str, serializer: Serializer) -> None:
        self._name = name
        self._serializer = serializer
        self._table_path: Optional[Path] = None
        self._segments: List[Segment] = []

    def init(self, override=False) -> None:
        """
        Initialise the table.

        :return:
        """

        self._table_path = Path(os.environ["SIMPLE_DB_PATH"])

        if os.path.isdir(self._table_location) and not override:
            raise DbExistsError(f"Table {self._name} already exists.")
        else:
            if not override:
                os.mkdir(self._table_location)
            else:
                # Try to load any pre-existing segments
                self._segments = sorted(Segment.from_path(self._table_location / Path(file)) for file
                                        in os.listdir(self._table_location))

                if self._segments:
                    if self._segments[-1].is_full():
                        next_segment_id = str(len(self._segments) + 1)
                        segment = Segment(self._table_location / f"s{next_segment_id}.smt")
                        self._segments.append(segment)
                else:
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
            raise DbExistsError(f"Table {self._name} doesn't exists.")

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
                segment = Segment(self._table_path / f"s{next_segment_id}.smt")
                self._segments.append(segment)
                self._segments[-1].write(key, serialized)

    def get(self, key: str) -> V:
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
                    return value
            raise TableException(f"Table {self._name} doesn't contain {key}")

    @property
    def _table_location(self) -> Path:
        return self._table_path / self._name
