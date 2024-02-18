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


def test_delete_existing(
    data_source: SQLAlchemyDataSource,
    nested_data_model: type[DataModel],
    data_model: type[DataModel],
):
    data_source.create_source(data_model, ignore_if_exists=True)
    with data_source.engine.connect() as connection:
        connection.execute(
            text("INSERT INTO DataModelTest (id, string, other) VALUES (1, 'test', 1)")
        )
        connection.commit()

    data_model(id=1, string="test", other=1).delete()

    with data_source.engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM DataModelTest")).fetchall()
    assert result == []


def test_delete_non_existing(
    data_source: SQLAlchemyDataSource,
    nested_data_model: type[DataModel],
    data_model: type[DataModel],
):
    data_source.create_source(data_model, ignore_if_exists=True)

    with data_source.engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM DataModelTest")).fetchall()
    assert result == []

    data_model(id=1, string="test", other=1).delete()

    with data_source.engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM DataModelTest")).fetchall()
    assert result == []


def test_delete_nested_existing(
    data_source: SQLAlchemyDataSource,
    nested_data_model: type[DataModel],
    data_model: type[DataModel],
):
    data_source.create_source(data_model, ignore_if_exists=True)
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

    data_model(
        id=1,
        string="test",
        other=1,
        nested=nested_data_model(id=1, string="test", other=1),
    ).delete()

    with data_source.engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM DataModelTest")).fetchall()
    assert result == []
    with data_source.engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM NestedDataModel")).fetchall()
    assert result == [
        (1, "test", 1),
    ]
