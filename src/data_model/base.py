from pydantic import BaseModel
from typing import ClassVar, Self

from .data_source import DataSource
from .sqlite3 import SQLite3DataSource


class DataModel(BaseModel):
    __data_source__: ClassVar[DataSource] = SQLite3DataSource(database="database.db")

    @classmethod
    def get_primary_key(cls) -> str:
        for field_name, field in cls.model_fields.items():
            if not field.json_schema_extra:
                field.json_schema_extra = {}
            if field.json_schema_extra.get("primary_key", False):
                return field_name
        raise ValueError(f"Missing primary key in {cls.__name__}")

    @classmethod
    def create_source(cls, ignore_if_exists: bool = False) -> None:
        cls.__data_source__.create_source(cls, ignore_if_exists)

    @classmethod
    def get_one(cls, **where) -> Self | None:
        return cls.__data_source__.get_one(cls, where)

    @classmethod
    def get_all(cls, **where) -> list[Self]:
        return cls.__data_source__.get_all(cls, where)

    def save(self) -> None:
        primary_key = self.get_primary_key()
        setattr(self, primary_key, self.__data_source__.save(self))

    def delete(self) -> None:
        self.__data_source__.delete(self)
