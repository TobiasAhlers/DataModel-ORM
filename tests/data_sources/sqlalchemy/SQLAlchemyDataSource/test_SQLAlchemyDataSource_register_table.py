from typing import Optional

import pytest
from pydantic import Field

from data_model import *
from data_model.data_sources.sqlalchemy.base import *


@pytest.fixture
def data_source():
    return SQLAlchemyDataSource(
        database_url="sqlite:///:memory:",
    )


@pytest.fixture
def table(data_source):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        string: str
        other: int

    return get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)


def test_fills_all_keys(data_source: SQLAlchemyDataSource, table: Table):
    data_source.register_table(table)

    assert "DataModelTest" in data_source.tables
    assert "table" in data_source.tables["DataModelTest"]
    assert "class" in data_source.tables["DataModelTest"]


def test_class_has_all_attributes(data_source: SQLAlchemyDataSource, table: Table):
    data_source.register_table(table)

    assert data_source.tables["DataModelTest"]["class"].__name__ == "DataModelTest"
    assert hasattr(data_source.tables["DataModelTest"]["class"], "id")
    assert hasattr(data_source.tables["DataModelTest"]["class"], "string")
    assert hasattr(data_source.tables["DataModelTest"]["class"], "other")
