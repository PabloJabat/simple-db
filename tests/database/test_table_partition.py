from typing import Any

import pytest

from src.database.table_partition import TablePartition
from src.serializer import Serializer


class NullSerializer(Serializer[Any]):
    def encode(self, value: Any) -> Any:
        return value

    def decode(self, value: Any) -> Any:
        return value


@pytest.fixture
def table_partition(tmp_path):
    null_serializer = NullSerializer()
    table_name = "test_table"
    partition_id = 0
    table_partition = TablePartition(tmp_path, table_name, partition_id, null_serializer)
    return table_partition


class TestTablePartition:

    def test_get_from_partition(self, table_partition):
        """Test that get from partition works properly."""

        table_partition.set("key_1", "value_1")
        table_partition.set("key_1", "value_2")
        table_partition.set("key_2", "value_3")
        assert table_partition.get("key_1") == "value_2"
        assert table_partition.get("key_2") == "value_3"

    def test_partition_adds_segment(self, table_partition):
        """Test that two segments are added to the same table."""

        for _ in range(20):
            table_partition.set("k", "val")

        assert len(table_partition.segments) == 1

        for _ in range(20):
            table_partition.set("k", "val")

        assert len(table_partition.segments) == 2

        segment_1 = table_partition.segments[0]
        segment_2 = table_partition.segments[1]

        assert segment_1.path.parent.name == segment_2.path.parent.name

    def test_deleting_partition(self, table_partition):
        """Test that deleting a partition works properly."""

        partition_path = table_partition.path

        # Delete partition
        table_partition.delete()

        assert partition_path.exists() is False
