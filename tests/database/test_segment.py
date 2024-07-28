import os

import pytest

from src.database.exceptions import SegmentSizeError
from src.database.segment import Segment


@pytest.fixture
def segment(tmp_path):
    yield Segment(tmp_path / "s1.smt")
    os.remove(tmp_path / "s1.smt")


class TestSegment:
    def test_segment_full(self, segment: Segment):
        """Test that the segment raises error after writing 100 bytes"""

        for _ in range(20):
            # Write 1 byte for the key, 3 for the value and 1 for the k-v separator
            segment.write("k", "val")

        with pytest.raises(SegmentSizeError):
            segment.write("k", "val")
