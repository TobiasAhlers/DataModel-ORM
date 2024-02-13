import pytest
from typing import Optional
from pydantic import Field

from data_model import *
from data_model.sqlite3.utils import *


def test_int():
    assert convert_type_to_sqlite3(int) == "INTEGER NOT NULL"


def test_optional_int():
    assert convert_type_to_sqlite3(Optional[int]) == "INTEGER"


def test_int_none():
    assert convert_type_to_sqlite3(int | None) == "INTEGER"


def test_float():
    assert convert_type_to_sqlite3(float) == "REAL NOT NULL"


def test_str():
    assert convert_type_to_sqlite3(str) == "TEXT NOT NULL"


def test_bytes():
    assert convert_type_to_sqlite3(bytes) == "BLOB NOT NULL"


def test_bool():
    assert convert_type_to_sqlite3(bool) == "INTEGER NOT NULL"


def test_datetime():
    assert convert_type_to_sqlite3(datetime) == "TIMESTAMP NOT NULL"


def test_date():
    assert convert_type_to_sqlite3(date) == "DATE NOT NULL"


def test_dict():
    assert convert_type_to_sqlite3(dict) == "JSON NOT NULL"
    assert convert_type_to_sqlite3(Optional[dict]) == "JSON"
    assert convert_type_to_sqlite3(dict | None) == "JSON"


def test_list():
    assert convert_type_to_sqlite3(list) == "JSON NOT NULL"
    assert convert_type_to_sqlite3(Optional[list]) == "JSON"
    assert convert_type_to_sqlite3(list | None) == "JSON"


def test_set():
    assert convert_type_to_sqlite3(set) == "JSON NOT NULL"
    assert convert_type_to_sqlite3(Optional[set]) == "JSON"
    assert convert_type_to_sqlite3(set | None) == "JSON"


def test_tuple():
    assert convert_type_to_sqlite3(tuple) == "JSON NOT NULL"
    assert convert_type_to_sqlite3(Optional[tuple]) == "JSON"
    assert convert_type_to_sqlite3(tuple | None) == "JSON"


def test_iterable():
    assert convert_type_to_sqlite3(Iterable) == "JSON NOT NULL"


def test_nested_model():
    class NestedModel(BaseModel):
        value: str

    assert convert_type_to_sqlite3(NestedModel) == "JSON NOT NULL"


def test_nested_data_model():
    class NestedDataModel(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        value: str = Field(json_schema_extra={"unique": True})

    assert (
        convert_type_to_sqlite3(NestedDataModel)
        == "INTEGER NOT NULL FOREIGN KEY REFERENCES NestedDataModel (id)"
    )


def test_union_int_str():
    with pytest.raises(UnsupportedTypeError):
        convert_type_to_sqlite3(Union[int, str])


def test_union_3_items():
    with pytest.raises(UnsupportedTypeError):
        convert_type_to_sqlite3(Union[int, str, float])
