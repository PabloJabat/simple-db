import os

import pytest

from src.database.exceptions import SegmentSizeError, SegmentExistsError
from src.database.segment import Segment


@pytest.fixture
def segment(tmp_path):
    table_name = "test_table"
    partition_id = 0
    segment_id = 0
    # TODO: If the creating of the file was separate from the creating of the segment we wouldn't need to do this
    os.makedirs(tmp_path / table_name / f"prt{partition_id}")
    segment = Segment(tmp_path, table_name, partition_id, segment_id)
    return segment


class TestSegment:
    def test_segment_full(self, segment: Segment):
        """Test that the segment raises error after writing 100 bytes"""

        for _ in range(20):
            # Write 1 byte for the key, 3 for the value and 1 for the k-v separator
            segment.write("k", "val")

        with pytest.raises(SegmentSizeError):
            segment.write("k", "val")

    def test_fail_to_create_segment(self, segment: segment):
        """Test that the segment raises error after creating if it already exists"""

        with pytest.raises(SegmentExistsError):
            _ = Segment(segment._db_path, segment._table_name, segment._partition_id, segment._segment_id)
