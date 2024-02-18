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


def test_id_and_attr():
    assert generate_delete_query(DataModelTest) == "DELETE FROM DataModelTest WHERE id = ?"
