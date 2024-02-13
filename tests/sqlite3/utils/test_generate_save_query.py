import pytest

from typing import Optional
from pydantic import Field
from data_model import *
from data_model.sqlite3.utils import *


def test_id_and_attr():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert (
        generate_save_query(ModelTest)
        == "INSERT INTO ModelTest (name) VALUES (?) ON CONFLICT (id) DO UPDATE SET name=excluded.name"
    )


def test_nested_data_model():
    class NestedModel(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"
        nested: Optional[NestedModel] = None

    assert (
        generate_save_query(ModelTest)
        == "INSERT INTO ModelTest (name, nested) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET name=excluded.name, nested=excluded.nested"
    )
