import pytest

from typing import Optional
from pydantic import Field
from sqlalchemy import *
from sqlalchemy.exc import OperationalError

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
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        string: str
        other: int

    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        string: str
        other: int
        nested: NestedDataModel

    return DataModelTest


def create_not_existing_table(
    data_source: SQLAlchemyDataSource, data_model: type[DataModel]
):
    data_source.create_source(data_model)

    with Session(data_source.engine) as session:
        result = session.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='DataModelTest'"
        )
        assert result.fetchone() is not None

        result = session.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='NestedDataModel'"
        )
        assert result.fetchone() is not None


def test_create_existing_table_ignore_if_exists(
    data_source: SQLAlchemyDataSource, data_model
):
    data_source.create_source(data_model)
    data_source.create_source(data_model, ignore_if_exists=True)


def test_create_existing_table_raise_error(
    data_source: SQLAlchemyDataSource, data_model
):
    data_source.create_source(data_model)

    with pytest.raises(OperationalError):
        data_source.create_source(data_model)
