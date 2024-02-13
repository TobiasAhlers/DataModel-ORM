import pytest

from typing import Optional
from pydantic import Field
from data_model import *
from data_model.sqlite3.utils import *


def test_where_attr():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_select_query(data_model=ModelTest, limit=None, where_attr=["id"])
        == "SELECT * FROM ModelTest WHERE id = ?"
    )


def test_where_attr_limit():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_select_query(data_model=ModelTest, limit=1, where_attr=["id"])
        == "SELECT * FROM ModelTest WHERE id = ? LIMIT 1"
    )


def test_where_attr_multiple():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_select_query(
            data_model=ModelTest, limit=None, where_attr=["id", "name"]
        )
        == "SELECT * FROM ModelTest WHERE id = ? AND name = ?"
    )


def test_where_attr_nested_data_model():
    class NestedDataModel(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        nested: NestedDataModel

    assert (
        generate_select_query(
            data_model=ModelTest, limit=None, where_attr=["id", "nested"]
        )
        == "SELECT * FROM ModelTest WHERE id = ? AND nested = ?"
    )


def test_where_attr_nested_data_model_limit():
    class NestedDataModel(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        nested: NestedDataModel

    assert (
        generate_select_query(
            data_model=ModelTest, limit=1, where_attr=["id", "nested"]
        )
        == "SELECT * FROM ModelTest WHERE id = ? AND nested = ? LIMIT 1"
    )


def test_where_attr_unknown_field():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    with pytest.raises(QueryError):
        generate_select_query(
            data_model=ModelTest, limit=None, where_attr=["id", "nested"]
        )


def test_where_empty():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_select_query(data_model=ModelTest, limit=None)
        == "SELECT * FROM ModelTest"
    )


def test_where_empty_limit():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_select_query(data_model=ModelTest, limit=1)
        == "SELECT * FROM ModelTest LIMIT 1"
    )


def test_where_empty_limit_negative():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    with pytest.raises(QueryError):
        generate_select_query(data_model=ModelTest, limit=-1)
