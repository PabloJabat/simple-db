import os
import subprocess
from typing import Generic, TypeVar

from .exceptions import DbExistsError

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
        if os.path.isfile(self._table_location):
            raise DbExistsError(f"Table {self._name} already exists.")
        else:
            subprocess.run(['touch', self._table_location])

    def delete(self) -> None:
        """
        Delete the table.

        :return:
        """

        if os.path.isdir(self._table_location):
            os.rmdir(self._table_location)
        else:
            raise DbExistsError(f"Table {self._name} doesn't exists.")

    def set(self, key: str, value: V) -> None:
        """
        Set a value in the table.

        :param key:
        :param value:
        :return:
        """
        serialized = self._serializer.encode(value)
        with open(self._table_location, 'a') as f:
            f.write(f"{key},{serialized}")
            f.write('\n')

    def get(self, key: str) -> V:
        """
        Get a value from the table.

        :param key:
        :return:
        """

        with open(self._table_location, 'r') as f:
            objects = []
            for line in f.readlines():
                if line.startswith(key):
                    objects.append(line.removeprefix(f"{key},"))

        return self._serializer.decode(objects[-1])

    @property
    def _table_location(self) -> str:
        return f"db/{self._name}"
