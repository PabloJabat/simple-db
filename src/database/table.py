import subprocess
from typing import Generic, TypeVar

V = TypeVar('V')


class Table(Generic[V]):
    """
    A class to represent a table in the database.
    """

    def __init__(self, name: str, serializer) -> None:
        self._name = name
        self._serializer = serializer

    def init(self) -> None:
        """
        Initialise the table.

        :return:
        """
        subprocess.run(["bin/start_db.sh", self._table_location])

    def set(self, key: str, value: V) -> None:
        """
        Set a value in the table.

        :param key:
        :param value:
        :return:
        """
        serialized = self._serializer.encode(value)
        subprocess.run(["bin/set_db.sh", self._table_location, key, serialized])

    def get(self, key: str) -> V:
        """
        Get a value from the table.

        :param key:
        :return:
        """

        serialized = subprocess.check_output(["bin/get_db.sh", self._table_location, key]).decode()
        return self._serializer.decode(serialized)

    @property
    def _table_location(self) -> str:
        return f"db/{self._name}"
