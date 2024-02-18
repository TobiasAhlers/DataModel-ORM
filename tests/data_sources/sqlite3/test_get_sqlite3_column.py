import pytest

from typing import Optional, Union
from pydantic import Field

from data_model import *
from data_model.data_sources.sqlite3 import *


def test_int_no_extras():
    assert (
        get_sqlite3_column(field_name="field", field=FieldInfo(annotation=int))
        == "field INTEGER NOT NULL"
    )


def test_optional_int_no_extras():
    assert (
        get_sqlite3_column(
            field_name="field", field=FieldInfo(annotation=Optional[int])
        )
        == "field INTEGER"
    )


def test_int_none_no_extras():
    assert (
        get_sqlite3_column(field_name="field", field=FieldInfo(annotation=int | None))
        == "field INTEGER"
    )


def test_union_int_none_no_extras():
    assert (
        get_sqlite3_column(
            field_name="field", field=FieldInfo(annotation=Union[int, None])
        )
        == "field INTEGER"
    )


def test_primary_key():
    assert (
        get_sqlite3_column(
            field_name="field",
            field=FieldInfo(
                annotation=int,
                json_schema_extra={"primary_key": True},
            ),
        )
        == "field INTEGER NOT NULL PRIMARY KEY"
    )


def test_primary_key_null():
    assert (
        get_sqlite3_column(
            field_name="field",
            field=FieldInfo(
                annotation=Optional[int],
                json_schema_extra={"primary_key": True},
            ),
        )
        == "field INTEGER PRIMARY KEY"
    )


def test_foreign_key():
    assert (
        get_sqlite3_column(
            field_name="field",
            field=FieldInfo(
                annotation=int,
                json_schema_extra={
                    "json_schema_extra": {"foreign_key": "OtherModel(id)"}
                },
            ),
        )
        == "field INTEGER NOT NULL"
    )


def test_nested_base_model():
    class NestedModel(BaseModel):
        id: int

    assert (
        get_sqlite3_column(field_name="field", field=FieldInfo(annotation=NestedModel))
        == "field JSON NOT NULL"
    )


def test_nested_data_model():
    class NestedModel(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})

    assert (
        get_sqlite3_column(field_name="field", field=FieldInfo(annotation=NestedModel))
        == "field INTEGER NOT NULL"
    )


def test_autoincrement_on_non_int():
    with pytest.raises(ValueError):
        get_sqlite3_column(
            field_name="field",
            field=FieldInfo(
                annotation=str,
                json_schema_extra={"primary_key": True, "autoincrement": True},
            ),
        )