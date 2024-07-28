from pathlib import Path
from uuid import UUID, uuid4

from .exceptions import SegmentSizeError


class Segment:
    MAX_SIZE = 100

    def __init__(self, path: Path) -> None:
        self.sid: UUID = uuid4()
        self._path: Path = path
        self._size: int = 0

    def is_full(self) -> bool:
        return self._size >= Segment.MAX_SIZE

    def write(self, data: bytes) -> None:
        if not self.is_full():
            self._size += len(data)
            with open(self._path, 'wb') as f:
                f.write(data)
        else:
            raise SegmentSizeError(f"Segment {self.sid} already full")
