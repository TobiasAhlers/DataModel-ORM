import pytest

from typing import Optional
from pydantic import Field
from data_model_orm import *
from data_model_orm.data_sources.sqlite3 import *


class NestedBaseModel(BaseModel):
    content: str = "CONTENT"


class NestedDataModel(DataModel):
    id: Optional[int] = Field(
        json_schema_extra={"primary_key": True, "autoincrement": True}, default=None
    )
    name: str = "NAME"


class DataModelTest(DataModel):
    id: Optional[int] = Field(
        json_schema_extra={"primary_key": True, "autoincrement": True}, default=None
    )
    string: str = "STRING"
    float_: float = 0.0
    int_: int = 0
    bool_: bool = False
    datetime_: datetime = datetime(2021, 1, 1, 0, 0, 0)
    date_: date = date(2021, 1, 1)
    bytes_: bytes = b"bytes"
    list_: list = [1, 2, 3]
    dict_: dict = {"key": "value"}
    nested_data_model: NestedDataModel = NestedDataModel()
    nested_base_model: NestedBaseModel = NestedBaseModel()




def test_where_attributes():
    assert (
        generate_get_query(data_model=DataModelTest, limit=None, where_attributes=["id"])
        == "SELECT * FROM DataModelTest WHERE id = ?"
    )


def test_where_attributes_limit():
    assert (
        generate_get_query(data_model=DataModelTest, limit=1, where_attributes=["id"])
        == "SELECT * FROM DataModelTest WHERE id = ? LIMIT 1"
    )


def test_where_attributes_multiple():
    assert (
        generate_get_query(
            data_model=DataModelTest, limit=None, where_attributes=["id", "string"]
        )
        == "SELECT * FROM DataModelTest WHERE id = ? AND string = ?"
    )


def test_where_attributes_nested_data_model():
    assert (
        generate_get_query(
            data_model=DataModelTest, limit=None, where_attributes=["id", "nested_data_model"]
        )
        == "SELECT * FROM DataModelTest WHERE id = ? AND nested_data_model = ?"
    )


def test_where_attributes_nested_data_model_limit():
    assert (
        generate_get_query(
            data_model=DataModelTest, limit=1, where_attributes=["id", "nested_data_model"]
        )
        == "SELECT * FROM DataModelTest WHERE id = ? AND nested_data_model = ? LIMIT 1"
    )


def test_where_empty():
    assert (
        generate_get_query(data_model=DataModelTest,where_attributes=[], limit=None)
        == "SELECT * FROM DataModelTest"
    )


def test_where_empty_limit():
    assert (
        generate_get_query(data_model=DataModelTest, where_attributes=[], limit=1)
        == "SELECT * FROM DataModelTest LIMIT 1"
    )


def test_where_empty_limit_negative():
    with pytest.raises(ValueError):
        generate_get_query(data_model=DataModelTest, where_attributes=[], limit=-1)
