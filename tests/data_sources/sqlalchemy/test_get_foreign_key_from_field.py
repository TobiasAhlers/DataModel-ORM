import pytest
from pydantic import Field

from data_model import *
from data_model.data_sources.sqlalchemy.base import *


def test_foreign_key_in_field():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int = Field(json_schema_extra={"foreign_key": "OtherModel.id"})

    assert (
        str(get_foreign_key_from_field(ModelTest.model_fields["other_id"]))
        == "ForeignKey('OtherModel.id')"
    )


def test_no_foreign_key_in_field():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int

    assert get_foreign_key_from_field(ModelTest.model_fields["other_id"]) is None


def test_nested_data_model():
    class OtherModel(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})

    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other: OtherModel

    assert (
        str(get_foreign_key_from_field(ModelTest.model_fields["other"]))
        == "ForeignKey('OtherModel.id')"
    )


def test_nested_base_model():
    class OtherModel(BaseModel):
        id: int

    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other: OtherModel

    assert get_foreign_key_from_field(ModelTest.model_fields["other"]) is None
