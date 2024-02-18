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


def test_registered_table(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})

    data_source.tables["DataModelTest"] = (
        get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    )

    table = data_source.get_table(data_model=DataModelTest)
    print(table)
    assert table.name == "DataModelTest"
    assert len(table.c) == 1
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert table.c.id.foreign_keys == set()


def test_not_registered_table(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 1
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert table.c.id.foreign_keys == set()
