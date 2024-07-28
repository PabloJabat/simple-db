import os
from typing import Any

import pytest

from src.database.exceptions import DbExistsError
from src.database.table import Table
from src.serializer import Serializer


class NullSerializer(Serializer[Any]):
    def encode(self, value: Any) -> Any:
        return value

    def decode(self, value: Any) -> Any:
        return value


@pytest.fixture
def table_fixture(tmp_path):
    """This fixture creates a table in the database with two full segments"""

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


@pytest.fixture
def existing_table_fixture(tmp_path):
    # Set db path
    os.environ["SIMPLE_DB_PATH"] = str(tmp_path)

    # Start table
    null_serializer = NullSerializer()
    table = Table("test_table", null_serializer)

    # Init table
    table.init()

    for _ in range(40):
        table.set("k", "val")

    # Return the table for the test
    yield table.name, tmp_path

    # Clean-up the table after the test
    table.delete()


@pytest.fixture
def existing_table_fixture_2(tmp_path):
    # Set db path
    os.environ["SIMPLE_DB_PATH"] = str(tmp_path)

    # Start table
    null_serializer = NullSerializer()
    table = Table("test_table_2", null_serializer)

    # Init table
    table.init()

    for _ in range(40):
        table.set("k", "val")

    # Return the table for the test
    yield table.name, tmp_path

    # Clean-up the table after the test
    table.delete()


class TestTable:
    def test_load_existing_table(self, existing_table_fixture):
        """Test that an existing table can be loaded."""

        null_serializer = NullSerializer()
        table_name, table_path = existing_table_fixture
        table = Table.from_path(table_path / table_name, null_serializer)
        table.init(override=True)

        assert len(table._segments) == 2

    def test_create_table_override(self):
        # TODO: Add test that the table can be created if table.init(True) even if there is none to load
        pass

    def test_fail_load_existing_table(self, existing_table_fixture):
        """Test that init fails when trying to create an existing table when override is false."""

        null_serializer = NullSerializer()
        table_name, _ = existing_table_fixture
        table = Table(table_name, null_serializer)
        with pytest.raises(DbExistsError):
            table.init()

    def test_two_separate_tables(self, existing_table_fixture, existing_table_fixture_2):
        """Test that two separate tables can be loaded and don't conflict with each other"""

        null_serializer = NullSerializer()

        table_name, table_path = existing_table_fixture
        table = Table.from_path(table_path / table_name, null_serializer)
        table.init(override=True)

        table_name_1, table_path_2 = existing_table_fixture_2
        table_2 = Table.from_path(table_path_2 / table_name_1, null_serializer)
        table_2.init(override=True)

        assert len(table._segments) == 2
        assert len(table_2._segments) == 2

    def test_set_object(self, table_fixture: Table):
        """Test setting one object and getting it"""

        table_fixture.set("key", "value")
        assert table_fixture.get("key") == "value"

    def test_get_latest_object_version(self, table_fixture: Table):
        """Test setting two objects and getting the latest version"""

        table_fixture.set("key", "value_1")
        table_fixture.set("key", "value_2")
        assert table_fixture.get("key") == "value_2"
