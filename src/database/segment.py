from pathlib import Path
from typing import Optional

from .exceptions import SegmentSizeError


class Segment:
    MAX_SIZE = 100

    def __init__(self, path: Path, size: int = 0) -> None:
        self._path: Path = path
        self._size: int = size

    def is_full(self) -> bool:
        return self._size >= Segment.MAX_SIZE

    def write(self, key: str, value: str) -> None:
        if not self.is_full():
            entry = f"{key},{value}"
            with open(self._path, 'ab') as f:
                f.write(entry.encode())
                self._size += len(entry)
                f.write(b"\n")
        else:
            raise SegmentSizeError(f"Segment {self._path} already full")

    def read(self, key) -> Optional[str]:
        with open(self._path, 'rb') as f:
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
    def from_path(path: Path) -> "Segment":
        with open(path, 'rb') as f:
            path_size = sum(len(line) for line in f.readlines())
            return Segment(path, path_size)

    def __gt__(self, other: "Segment") -> bool:
        return self._path.name >= other._path.name

    def __lt__(self, other: "Segment") -> bool:
        return self._path.name < other._path.name
