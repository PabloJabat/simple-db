import json

from src.database.table import Table
from src.serializer import Serializer


class People:
    def __init__(self, name) -> None:
        self.name = name

    def __str__(self):
        return json.dumps(self.__dict__)


class PeopleSerializer(Serializer[People]):
    @staticmethod
    def encode(value: People) -> str:
        return json.dumps(value.__dict__)

    @staticmethod
    def decode(value: str) -> People:
        return People(**json.loads(value))


if __name__ == "__main__":
    people_serializer = PeopleSerializer()

    people_tbl = Table[People]("people", people_serializer)

    people_tbl.set("1", People("pablo"))

    print(people_tbl.get("1"))
    people_tbl.delete()
