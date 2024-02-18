import pytest

from typing import Optional
from pydantic import Field
from sqlalchemy import *

from data_model import *
from data_model.data_sources.sqlalchemy.base import *


@pytest.fixture
def data_source():
    return SQLAlchemyDataSource(
        database_url="sqlite:///:memory:",
    )


@pytest.fixture()
def nested_data_model(data_source):
    class NestedDataModel(DataModel):
        __data_source__ = data_source

        id: Optional[int] = Field(json_schema_extra={"primary_key": True}, default=None)
        string: str
        other: int

    NestedDataModel.__data_source__.create_source(
        NestedDataModel, ignore_if_exists=True
    )

    return NestedDataModel


@pytest.fixture
def data_model(data_source, nested_data_model):
    class DataModelTest(DataModel):
        __data_source__ = data_source

        id: Optional[int] = Field(json_schema_extra={"primary_key": True}, default=None)
        string: str
        other: int
        nested: Optional[nested_data_model] = None

    DataModelTest.__data_source__.create_source(DataModelTest, ignore_if_exists=True)

    return DataModelTest


def test_get_simple(data_source: SQLAlchemyDataSource, data_model: DataModel):
    data_source.create_source(data_model, ignore_if_exists=True)
    with data_source.engine.connect() as connection:
        connection.execute(
            text("INSERT INTO DataModelTest (id, string, other) VALUES (1, 'test', 1)")
        )
        connection.commit()

    result = data_model.get_one(id=1)
    assert result is not None
    assert result.id == 1
    assert result.string == "test"
    assert result.other == 1


def test_get_nested(data_source: SQLAlchemyDataSource, data_model: DataModel):
    with data_source.engine.connect() as connection:
        connection.execute(
            text(
                "INSERT INTO NestedDataModel (id, string, other) VALUES (1, 'test', 1)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO DataModelTest (id, string, other, nested) VALUES (1, 'test', 1, 1)"
            )
        )
        connection.commit()

    result = data_model.get_one(id=1)
    assert result is not None
    assert result.id == 1
    assert result.string == "test"
    assert result.other == 1
    assert result.nested is not None
    assert result.nested.id == 1
    assert result.nested.string == "test"
    assert result.nested.other == 1


def test_get_not_existing(data_source: SQLAlchemyDataSource, data_model: DataModel):
    result = data_model.get_one(id=250)
    assert result is None


def test_empty(data_source: SQLAlchemyDataSource, data_model: DataModel):
    with data_source.engine.connect() as connection:
        connection.execute(
            text("INSERT INTO DataModelTest (id, string, other) VALUES (1, 'test', 1)")
        )
        connection.commit()

    result = data_model.get_one()
    assert result is not None
    assert result.id == 1
    assert result.string == "test"
    assert result.other == 1
    assert result.nested is None
