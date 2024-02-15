from typing import get_origin, Union, TYPE_CHECKING, Any
from types import UnionType
from datetime import datetime, date
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from collections.abc import Iterable
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Float,
    Boolean,
    LargeBinary,
    JSON,
    ForeignKey,
    Table,
    MetaData,
)


if TYPE_CHECKING:
    from ..base import DataModel


def is_nullable(type_: type) -> tuple[bool, type]:
    nullable = False
    if get_origin(type_) is UnionType or get_origin(type_) is Union:
        if len(type_.__args__) > 2:
            raise TypeError(f"Union with more than 2 types is not supported: {type_}")
        if type(None) in type_.__args__:
            nullable = True
            type_ = next(t for t in type_.__args__ if t is not type(None))
        else:
            raise TypeError(f"Union without None is not supported: {type_}")
    return nullable, type_


def get_sqlalchemy_type(type_: type) -> Any:
    nullable, type_ = is_nullable(type_)
    if issubclass(type_, BaseModel):
        try:
            primary_key = type_.get_primary_key()
            field = type_.model_fields[primary_key]
            if isinstance(field.annotation, BaseModel):
                raise TypeError(
                    f"Nested models are not supported as primary key: {type_}"
                )
            return get_sqlalchemy_type(field.annotation)
        except AttributeError:
            return JSON
    if issubclass(type_, str):
        return String
    if issubclass(type_, float):
        return Float
    if issubclass(type_, bool):
        return Boolean
    if issubclass(type_, int):
        return Integer
    if issubclass(type_, datetime):
        return DateTime
    if issubclass(type_, date):
        return Date
    if issubclass(type_, bytes):
        return LargeBinary
    if issubclass(type_, (Iterable, dict)):
        return JSON
    raise TypeError(f"Unsupported type: {type_}")


def get_foreign_key_from_field(field: FieldInfo) -> ForeignKey | None:
    nullable, type_ = is_nullable(field.annotation)
    if not field.json_schema_extra:
        field.json_schema_extra = {}
    foreign_key = field.json_schema_extra.get("foreign_key", None)
    if not foreign_key and issubclass(type_, BaseModel):
        try:
            primary_key = type_.get_primary_key()
            return ForeignKey(f"{type_.__name__}.{primary_key}")
        except AttributeError:
            return None
    if not foreign_key:
        return None
    if not isinstance(foreign_key, str):
        if not isinstance(foreign_key, list) and not all(
            isinstance(fk, str) for fk in foreign_key
        ):
            raise ValueError(
                f"foreign_key must be a string or a list of strings: {foreign_key}"
            )
    return ForeignKey(foreign_key)


def get_column_from_field(field_name: str, field: FieldInfo) -> Column:
    type_ = field.annotation
    if not field.json_schema_extra:
        field.json_schema_extra = {}
    primary_key = field.json_schema_extra.get("primary_key", False)
    unique = field.json_schema_extra.get("unique", None)
    foreign_key = get_foreign_key_from_field(field)

    nullable, type_ = is_nullable(type_)
    sqlalchemy_type = get_sqlalchemy_type(type_)

    return Column(
        name=field_name,
        type_=sqlalchemy_type,
        primary_key=primary_key,
        unique=unique,
        nullable=nullable,
        foreign_key=foreign_key,
    )


def get_sqlalchemy_table(data_model: type["DataModel"]) -> Table:
    columns = []
    for field_name, field in data_model.model_fields.items():
        columns.append(get_column_from_field(field_name, field))
    return Table(data_model.__name__, MetaData(), *columns)
