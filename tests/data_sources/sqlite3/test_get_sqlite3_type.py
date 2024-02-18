import pytest

from typing import Optional, Union
from pydantic import BaseModel, Field

from data_model_orm import *
from data_model_orm.data_sources.sqlite3 import *


def test_int():
    assert get_sqlite3_type(int) == "INTEGER"


def test_optional_int():
    assert get_sqlite3_type(Optional[int]) == "INTEGER"


def test_int_none():
    assert get_sqlite3_type(int | None) == "INTEGER"


def test_union_int_none():
    assert get_sqlite3_type(Union[int, None]) == "INTEGER"


def test_float():
    assert get_sqlite3_type(float) == "REAL"


def test_bool():
    assert get_sqlite3_type(bool) == "INTEGER"


def test_str():
    assert get_sqlite3_type(str) == "TEXT"


def test_datetime():
    assert get_sqlite3_type(datetime) == "TIMESTAMP"


def test_date():
    assert get_sqlite3_type(date) == "DATE"


def test_bytes():
    assert get_sqlite3_type(bytes) == "BLOB"


def test_dict():
    assert get_sqlite3_type(dict) == "JSON"


def test_list():
    assert get_sqlite3_type(list) == "JSON"


def test_set():
    assert get_sqlite3_type(set) == "JSON"


def test_BaseModel():
    class TestModel(BaseModel):
        pass

    assert get_sqlite3_type(TestModel) == "JSON"


def test_DataModel():
    class TestModel(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})

    assert get_sqlite3_type(TestModel) == "INTEGER"


def test_nested_model():
    class NestedModel(BaseModel):
        value: str

    class TestModel(BaseModel):
        nested: NestedModel

    assert get_sqlite3_type(TestModel) == "JSON"


def test_list_of_str():
    assert get_sqlite3_type(list[str]) == "JSON"
