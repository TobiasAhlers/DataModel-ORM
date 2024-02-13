from typing import get_origin, Union, TYPE_CHECKING
from types import UnionType
from datetime import datetime, date
from pydantic import BaseModel
from collections.abc import Iterable

from ..errors import (
    UnsupportedTypeError,
    MultiplePrimaryKeysError,
    MissingPrimaryKeyError,
    AutoIncrementError,
    InvalidForeignKeyError,
    QueryError,
)

if TYPE_CHECKING:
    from ..base import DataModel


def convert_type_to_sqlite3(type_: type) -> str:
    nullable = " NOT NULL"
    if get_origin(type_) is UnionType or get_origin(type_) is Union:
        if len(type_.__args__) > 2:
            raise UnsupportedTypeError(
                f"Union with more than 2 types is not supported: {type_}"
            )
        if type(None) in type_.__args__:
            nullable = ""
            type_ = next(t for t in type_.__args__ if t is not type(None))
        else:
            raise UnsupportedTypeError(f"Union without None is not supported: {type_}")

    if type_ is int:
        return f"INTEGER{nullable}"
    if type_ is float:
        return f"REAL{nullable}"
    if type_ is str:
        return f"TEXT{nullable}"
    if type_ is bytes:
        return f"BLOB{nullable}"
    if type_ is bool:
        return f"INTEGER{nullable}"
    if type_ is datetime:
        return f"TIMESTAMP{nullable}"
    if type_ is date:
        return f"DATE{nullable}"

    if issubclass(type_, BaseModel):
        try:
            primary_key = type_.get_primary_key()
            field = type_.model_fields[primary_key]
            if isinstance(field.annotation, BaseModel):
                raise UnsupportedTypeError(
                    f"Nested models are not supported as primary key: {type_}"
                )
            return f"{convert_type_to_sqlite3(field.annotation)}{nullable} FOREIGN KEY REFERENCES {type_.__name__} ({primary_key})"
        except AttributeError:
            return f"JSON{nullable}"
    if type_ is dict:
        return f"JSON{nullable}"

    if issubclass(type_, (Iterable, dict)):
        return f"JSON{nullable}"

    raise UnsupportedTypeError(f"Unsupported type: {type_}")


def generate_save_query(data_model: type["DataModel"]) -> str:
    fields = []
    for field_name, field in data_model.model_fields.items():
        if not field.json_schema_extra:
            field.json_schema_extra = {}
        if field.json_schema_extra.get("primary_key", False):
            continue
        fields.append(field_name)
    query = f"INSERT INTO {data_model.__name__} ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    query += f" ON CONFLICT ({data_model.get_primary_key()}) DO UPDATE SET {', '.join([f'{field}=excluded.{field}' for field in fields])}"
    return query


def generate_deletion_query(data_mode: "DataModel") -> str:
    return f"DELETE FROM {data_mode.__name__} WHERE {data_mode.get_primary_key()} = ?"


def generate_create_table_query(data_mode: "DataModel") -> str:
    fields = []
    for field_name, field in data_mode.model_fields.items():
        field_str = f"{field_name} {convert_type_to_sqlite3(field.annotation)}"
        if not field.json_schema_extra:
            field.json_schema_extra = {}

        if field.json_schema_extra.get("primary_key", False):
            field_str += " PRIMARY KEY"
            if field.json_schema_extra.get("autoincrement", False):
                if not (field.annotation is int or int in field.annotation.__args__):
                    raise AutoIncrementError(
                        f"autoincrement is only supported for int: {field_name}"
                    )
                field_str += " AUTOINCREMENT"
        if field.json_schema_extra.get(
            "autoincrement", False
        ) and not field.json_schema_extra.get("primary_key", False):
            raise AutoIncrementError(
                f"autoincrement is only supported for primary key: {field_name}"
            )
        if field.json_schema_extra.get("unique", False):
            field_str += " UNIQUE"
        if field.json_schema_extra.get("foreign_key", False):
            foreign_key = field.json_schema_extra["foreign_key"]
            if not isinstance(foreign_key, str):
                if not isinstance(foreign_key, list) and not all(
                    isinstance(fk, str) for fk in foreign_key
                ):
                    raise InvalidForeignKeyError(
                        f"foreign_key must be a string or a list of strings: {foreign_key}"
                    )
            field_str += (
                f" FOREIGN KEY REFERENCES {field.json_schema_extra['foreign_key']}"
            )
        fields.append(field_str)

    primary_keys = [
        field_name
        for field_name, field in data_mode.model_fields.items()
        if field.json_schema_extra.get("primary_key", False)
    ]
    if len(primary_keys) > 1:
        raise MultiplePrimaryKeysError(f"Multiple primary keys defined: {primary_keys}")
    if len(primary_keys) == 0:
        raise MissingPrimaryKeyError(f"Missing primary key")

    return f"CREATE TABLE {data_mode.__name__} ({', '.join(fields)})"


def generate_select_query(
    data_model: "DataModel", limit: int = None, where_attr: list[str] = None
) -> str:
    query = f"SELECT * FROM {data_model.__name__}"

    if where_attr:
        for attr in where_attr:
            if attr not in data_model.model_fields:
                raise QueryError(f"Unknown field: {attr}")
        query += f" WHERE {' AND '.join([f'{attr} = ?' for attr in where_attr])}"
    if limit:
        if limit < 0:
            raise QueryError(f"Limit must be a positive integer: {limit}")
        query += f" LIMIT {limit}"
    return query
