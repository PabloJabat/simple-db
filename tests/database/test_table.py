import os
from typing import Any

import pytest

from src.database.table import Table
from src.serializer import Serializer


class NullSerializer(Serializer[Any]):
    def encode(self, value: Any) -> Any:
        return value

    def decode(self, value: Any) -> Any:
        return value


@pytest.fixture
def test_table(tmp_path):
    # Set db path
    os.environ["SIMPLE_DB_PATH"] = str(tmp_path)

    # Start table
    null_serializer = NullSerializer()
    table = Table("test_table", null_serializer)

    # Init table
    table.init()

    # Return the table for the test
    yield table

    # Clean-up the table after the test
    table.delete()


class TestTable:
    def test_set_object(self, test_table: Table):
        """Test setting one object and getting it"""

        test_table.set("key", "value")
        assert test_table.get("key") == "value"

    def test_get_latest_object_version(self, test_table: Table):
        """Test setting two objects and getting the latest version"""

        test_table.set("key", "value_1")
        test_table.set("key", "value_2")
        assert test_table.get("key") == "value_2"
