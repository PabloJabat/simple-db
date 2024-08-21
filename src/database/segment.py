import re
from pathlib import Path
from typing import Optional

from .exceptions import SegmentExistsError, SegmentSizeError


class Segment:
    MAX_SIZE = 100

    def __init__(self, db_path: Path, table_name: str, partition_id: int, segment_id: int, size: int = 0) -> None:
        """The path for the segment will be created upon initialization. The file shouldn't exist yet."""
        # TODO: At the moment, the segment is responsible of creating the file that it needs to be created.

        self._table_name = table_name
        self._partition_id = partition_id
        self._segment_id = segment_id
        self._size: int = size

        # Init segment path and try to create the file
        self._db_path = db_path
        try:
            self.path.touch(exist_ok=False)
        except FileExistsError:
            raise SegmentExistsError("The path where this segment is trying to init its backing file already exists")

    def is_full(self) -> bool:
        return self._size >= Segment.MAX_SIZE

    def is_empty(self) -> bool:
        return self._size == 0

    def write(self, key: str, value: str) -> None:
        # TODO: This method should fail if we try to write to a segment that doesn't have a file backing it up
        if not self.is_full():
            entry = f"{key},{value}"
            with open(self.path, 'ab') as f:
                f.write(entry.encode())
                self._size += len(entry)
                f.write(b"\n")
        else:
            raise SegmentSizeError(f"Segment {self.path} already full")

    def read(self, key) -> Optional[str]:
        with open(self.path, 'rb') as f:
            objects = []
            for encoded_line in f.readlines():
                line = encoded_line.decode()
                if line.startswith(key):
                    objects.append(line.removeprefix(f"{key},").removesuffix("\n"))

            if len(objects) > 0:
                return objects[-1]
            else:
                return None

    @staticmethod
    def from_path(path: Path) -> "Segment":  # pragma: no cover
        # TODO: This function will be deprecated if the creating of the file is moved away from the Segment creation
        # Extract the components
        segment_id = re.match("^([0-9])\.sgmt$", path.name).group(1)
        partition_id = re.match("^prt([0-9])$", path.parent.name).group(1)
        table_name = path.parent.parent.name
        db_path = path.parent.parent.parent

        with open(path, 'rb') as f:
            path_size = sum(len(line) for line in f.readlines())
            return Segment(db_path, table_name, partition_id, segment_id, path_size)

    @property
    def path(self) -> Path:
        return self._db_path / self._table_name / f"prt{self._partition_id}" / f"{self._segment_id}.sgmt"

    def __gt__(self, other: "Segment") -> bool:
        return self.path.name >= other.path.name

    def __lt__(self, other: "Segment") -> bool:
        return self.path.name < other.path.name
