import pytest

from pydantic import Field
from typing import Optional
from sqlalchemy import (
    Integer,
    Float,
    Boolean,
    DateTime,
    Date,
    LargeBinary,
    JSON,
    String,
)

from data_model import *
from data_model.sqlite3.utils import *


def test_int():
    assert get_sqlalchemy_type(int) == Integer


def test_optional_int():
    assert get_sqlalchemy_type(Optional[int]) == Integer


def test_int_none():
    assert get_sqlalchemy_type(int | None) == Integer


def test_union_int_none():
    assert get_sqlalchemy_type(Union[int, None]) == Integer


def test_float():
    assert get_sqlalchemy_type(float) == Float


def test_bool():
    assert get_sqlalchemy_type(bool) == Boolean


def test_str():
    assert get_sqlalchemy_type(str) == String


def test_datetime():
    assert get_sqlalchemy_type(datetime) == DateTime


def test_date():
    assert get_sqlalchemy_type(date) == Date


def test_bytes():
    assert get_sqlalchemy_type(bytes) == LargeBinary


def test_dict():
    assert get_sqlalchemy_type(dict) == JSON


def test_list():
    assert get_sqlalchemy_type(list) == JSON


def test_set():
    assert get_sqlalchemy_type(set) == JSON


def test_BaseModel():
    class TestModel(BaseModel):
        pass

    assert get_sqlalchemy_type(TestModel) == JSON


def test_DataModel():
    class TestModel(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})

    assert get_sqlalchemy_type(TestModel) == Integer


def test_nested_model():
    class NestedModel(BaseModel):
        value: str

    class TestModel(BaseModel):
        nested: NestedModel

    assert get_sqlalchemy_type(TestModel) == JSON
