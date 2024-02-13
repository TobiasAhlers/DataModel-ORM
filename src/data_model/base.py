from pydantic import BaseModel
from typing import ClassVar, Self

from .errors import MissingPrimaryKeyError
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
        raise MissingPrimaryKeyError(f"Missing primary key in {cls.__name__}")

    @classmethod
    def create_source(cls, ignore_if_exists: bool = False) -> None:
        raise NotImplementedError()

    @classmethod
    def get_one(cls, where: dict) -> Self | None:
        raise NotImplementedError()

    @classmethod
    def get_all(cls, where: dict = None) -> list[Self]:
        raise NotImplementedError()

    def save(self) -> None:
        raise NotImplementedError()

    def delete(self) -> None:
        raise NotImplementedError()
