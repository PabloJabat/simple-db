from abc import ABC
from typing import Generic, TypeVar

T = TypeVar('T')


class Serializer(Generic[T], ABC):
    """
    Abstract base class for serializing objects.
    """

    def __init__(self):
        pass

    @staticmethod
    def encode(val: T) -> str:
        """
        Encodes the given value into a string.

        :param val:
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def decode(val: str) -> T:
        """
        Decodes the given string into a value.

        :param val:
        :return:
        """
        raise NotImplementedError
