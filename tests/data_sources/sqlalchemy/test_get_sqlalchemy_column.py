from typing import Optional

import pytest
from pydantic import Field
from pydantic.fields import FieldInfo

from data_model import *
from data_model.data_sources.sqlalchemy.base import *


def test_int_no_extras():
    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=int)
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == False
    assert column.foreign_keys == set()


def test_optional_int_no_extras():
    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=Optional[int])
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == True
    assert column.foreign_keys == set()


def test_int_none_no_extras():
    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=int | None)
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == True
    assert column.foreign_keys == set()


def test_union_int_none_no_extras():
    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=Union[int, None])
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == True
    assert column.foreign_keys == set()


def test_primary_key():
    column: Column = get_sqlalchemy_column(
        field_name="field",
        field=FieldInfo(annotation=int, json_schema_extra={"primary_key": True}),
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == True
    assert column.unique == None
    assert column.nullable == False
    assert column.foreign_keys == set()


def test_unique():
    column: Column = get_sqlalchemy_column(
        field_name="field",
        field=FieldInfo(annotation=int, json_schema_extra={"unique": True}),
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == True
    assert column.nullable == False
    assert column.foreign_keys == set()


def test_foreign_key():
    column: Column = get_sqlalchemy_column(
        field_name="field",
        field=FieldInfo(
            annotation=int, json_schema_extra={"foreign_key": "other_table.id"}
        ),
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == False
    assert str(column.foreign_keys) == str({ForeignKey("other_table.id")})


def test_nested_base_model():
    class NestedModel(BaseModel):
        id: int

    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=NestedModel)
    )
    assert column.name == "field"
    assert isinstance(column.type, JSON)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == False
    assert column.foreign_keys == set()


def test_nested_data_model():
    class NestedModel(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})

    column: Column = get_sqlalchemy_column(
        field_name="field", field=FieldInfo(annotation=NestedModel)
    )
    assert column.name == "field"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == False
    assert str(column.foreign_keys) == str({ForeignKey("NestedModel.id")})


def test_from_data_model():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int = Field(json_schema_extra={"foreign_key": "other_table.id"})

    column: Column = get_sqlalchemy_column(
        field_name="other_id", field=ModelTest.model_fields["other_id"]
    )

    assert column.name == "other_id"
    assert isinstance(column.type, Integer)
    assert column.primary_key == False
    assert column.unique == None
    assert column.nullable == False
    assert str(column.foreign_keys) == str({ForeignKey("other_table.id")})
