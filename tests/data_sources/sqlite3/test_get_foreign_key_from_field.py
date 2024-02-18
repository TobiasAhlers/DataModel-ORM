import pytest
from pydantic import Field

from data_model_orm import *
from data_model_orm.data_sources.sqlite3 import *


def test_foreign_key_in_field():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int = Field(json_schema_extra={"foreign_key": "OtherModel(id)"})

    assert (
        get_foreign_key_from_field("other_id", ModelTest.model_fields["other_id"])
        == f"FOREIGN KEY (other_id) REFERENCES OtherModel(id)"
    )


def test_no_foreign_key_in_field():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int

    assert (
        get_foreign_key_from_field("other_id", ModelTest.model_fields["other_id"])
        is None
    )


def test_nested_data_model():
    class OtherModel(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})

    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other: OtherModel

    assert (
        get_foreign_key_from_field("other", ModelTest.model_fields["other"])
        == f"FOREIGN KEY (other) REFERENCES OtherModel(id)"
    )


def test_nested_base_model():
    class OtherModel(BaseModel):
        id: int

    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other: OtherModel

    assert (
        get_foreign_key_from_field("other_id", ModelTest.model_fields["other"]) is None
    )


def test_invalid_foreign_key_format():
    class ModelTest(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})
        other_id: int = Field(json_schema_extra={"foreign_key": "OtherModel.id"})

    with pytest.raises(ValueError):
        get_foreign_key_from_field("other_id", ModelTest.model_fields["other_id"])
