import pytest

from pydantic import Field
from typing import Optional
from sqlalchemy import text

from data_model import *
from data_model.data_sources.sqlalchemy.base import *


@pytest.fixture
def data_source():
    return SQLAlchemyDataSource(
        database_url="sqlite:///:memory:",
    )


@pytest.fixture
def data_model(data_source):
    class NestedDataModel(DataModel):
        __data_source__ = data_source

        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        string: str
        other: int

    class DataModelTest(DataModel):
        __data_source__ = data_source

        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        string: str
        other: int
        nested: Optional[NestedDataModel] = None

    DataModelTest.__data_source__.create_source(DataModelTest, ignore_if_exists=True)

    return (DataModelTest, NestedDataModel)


def test_normal(data_model):
    assert convert_sqlalchemy_row_to_data_model(
        row=(1, "test", 1),
        data_model=data_model[0],
    ) == data_model[0](
        id=1,
        string="test",
        other=1,
    )


def test_nested_data_model(data_model):
    with data_model[0].__data_source__.engine.connect() as connection:
        connection.execute(
            text(
                "INSERT INTO NestedDataModel (id, string, other) VALUES (1, 'test', 1)"
            )
        )
        connection.commit()
    print()
    assert convert_sqlalchemy_row_to_data_model(
        row=(1, "test", 1, 1),
        data_model=data_model[0],
    ) == data_model[0](
        id=1,
        string="test",
        other=1,
        nested=data_model[1](
            id=1,
            string="test",
            other=1,
        ),
    )
