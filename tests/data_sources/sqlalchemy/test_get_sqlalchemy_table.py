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


def test_valid(data_source: SQLAlchemyDataSource):
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


def test_multiple_columns(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        name: str

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert table.c.id.foreign_keys == set()
    assert table.c.name.primary_key == False
    assert table.c.name.nullable == False
    assert table.c.name.unique == None
    assert table.c.name.foreign_keys == set()


def test_nested_data_model(data_source: SQLAlchemyDataSource):
    class NestedDataModel(DataModel):
        id: int = Field(json_schema_extra={"primary_key": True})

    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        nested: NestedDataModel

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.foreign_keys == set()
    assert table.c.nested.primary_key == False
    assert table.c.nested.nullable == False
    assert table.c.nested.unique == None
    assert isinstance(table.c.nested.type, Integer)
    assert str(table.c.nested.foreign_keys) == "{ForeignKey('NestedDataModel.id')}"
    assert table.c.nested.nullable == False
    assert table.c.nested.unique == None
    assert table.c.nested.primary_key == False


def test_list_column(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        names: list[str]

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.foreign_keys == set()
    assert table.c.names.primary_key == False
    assert table.c.names.nullable == False
    assert table.c.names.unique == None
    assert table.c.names.foreign_keys == set()
    assert isinstance(table.c.names.type, JSON)
    assert table.c.names.foreign_keys == set()
    assert table.c.names.nullable == False
    assert table.c.names.unique == None
    assert table.c.names.primary_key == False


def test_nested_base_model(data_source: SQLAlchemyDataSource):
    class NestedModel(BaseModel):
        id: int

    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        nested: NestedModel

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.foreign_keys == set()
    assert table.c.nested.primary_key == False
    assert table.c.nested.nullable == False
    assert table.c.nested.unique == None
    assert isinstance(table.c.nested.type, JSON)
    assert table.c.nested.foreign_keys == set()
    assert table.c.nested.nullable == False
    assert table.c.nested.unique == None
    assert table.c.nested.primary_key == False


def test_unique_column(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        name: str = Field(json_schema_extra={"unique": True})

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert isinstance(table.c.id.type, Integer)
    assert table.c.id.foreign_keys == set()
    assert table.c.name.primary_key == False
    assert table.c.name.nullable == False
    assert table.c.name.unique == True
    assert table.c.name.foreign_keys == set()
    assert isinstance(table.c.name.type, String)
    assert table.c.name.foreign_keys == set()
    assert table.c.name.nullable == False
    assert table.c.name.unique == True
    assert table.c.name.primary_key == False


def test_explicit_foreign_key(data_source: SQLAlchemyDataSource):
    class DataModelTest(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True})
        other_id: int = Field(json_schema_extra={"foreign_key": "other_table.id"})

    table = get_sqlalchemy_table(data_model=DataModelTest, data_source=data_source)
    assert table.name == "DataModelTest"
    assert len(table.c) == 2
    assert table.c.id.primary_key == True
    assert table.c.id.nullable == True
    assert table.c.id.unique == None
    assert table.c.id.foreign_keys == set()
    assert isinstance(table.c.id.type, Integer)
    assert table.c.other_id.primary_key == False
    assert table.c.other_id.nullable == False
    assert table.c.other_id.unique == None
    assert str(table.c.other_id.foreign_keys) == "{ForeignKey('other_table.id')}"
    assert isinstance(table.c.other_id.type, Integer)
    assert table.c.other_id.nullable == False
    assert table.c.other_id.unique == None
    assert table.c.other_id.primary_key == False
