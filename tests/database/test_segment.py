import os

import pytest

from src.database.exceptions import SegmentSizeError
from src.database.segment import Segment


@pytest.fixture
def segment(tmp_path):
    yield Segment(tmp_path / "s1.db")
    os.remove(tmp_path / "s1.db")


class TestSegment:
    def test_segment_full(self, segment: Segment):
        """Test that the segment raises error after writing 100 bytes"""

        for _ in range(25):
            # Write 4 bytes each iteration
            segment.write(b"data")

        with pytest.raises(SegmentSizeError):
            segment.write(b"data")
