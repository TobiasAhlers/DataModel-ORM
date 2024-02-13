import pytest

from typing import Optional
from pydantic import Field, BaseModel
from data_model import *
from data_model.sqlite3.utils import *


def test_id_and_attr():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_create_table_query(ModelTest)
        == "CREATE TABLE ModelTest (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
    )


def test_id_nullable():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True, "autoincrement": True})
        name: Optional[str] = "NAME"

    assert (
        generate_create_table_query(ModelTest)
        == "CREATE TABLE ModelTest (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT)"
    )


def test_nested_model():
    class NestedModel(BaseModel):
        value: str

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"
        nested: NestedModel

    assert (
        generate_create_table_query(ModelTest)
        == "CREATE TABLE ModelTest (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, nested JSON NOT NULL)"
    )


def test_nested_data_model():
    class NestedDataModel(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"
        nested: Optional[NestedDataModel] = None

    assert (
        generate_create_table_query(ModelTest)
        == "CREATE TABLE ModelTest (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, nested INTEGER FOREIGN KEY REFERENCES NestedDataModel (id))"
    )


def test_nested_data_model_string_id():
    class NestedDataModel(DataModel):
        id: Optional[str] = Field(json_schema_extra={"primary_key": True})
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[str] = Field(json_schema_extra={"primary_key": True})
        name: str = "NAME"
        nested: Optional[NestedDataModel] = None

    assert (
        generate_create_table_query(ModelTest)
        == "CREATE TABLE ModelTest (id TEXT PRIMARY KEY, name TEXT NOT NULL, nested TEXT FOREIGN KEY REFERENCES NestedDataModel (id))"
    )


def test_nested_data_model_no_id():
    class NestedDataModel(DataModel):
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"
        nested: Optional[NestedDataModel] = None

    with pytest.raises(MissingPrimaryKeyError):
        generate_create_table_query(ModelTest)


def test_missing_id():
    class ModelTest(DataModel):
        name: str = "NAME"

    with pytest.raises(MissingPrimaryKeyError):
        generate_create_table_query(ModelTest)


def test_multiple_primary_keys():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: Optional[str] = Field(json_schema_extra={"primary_key": True})

    with pytest.raises(MultiplePrimaryKeysError):
        generate_create_table_query(ModelTest)


def test_unsupported_type():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: Optional[None]

    with pytest.raises(UnsupportedTypeError):
        generate_create_table_query(ModelTest)


def test_autoincrement_on_non_int():
    class ModelTest(DataModel):
        id: Optional[str] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    with pytest.raises(AutoIncrementError):
        generate_create_table_query(ModelTest)
