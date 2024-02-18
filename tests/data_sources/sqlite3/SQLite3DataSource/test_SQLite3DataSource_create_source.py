import pytest

from sqlite3 import connect
from typing import Optional
from pydantic import BaseModel, Field

from data_model import *
from data_model.data_sources.sqlite3 import *


class NestedBaseModel(BaseModel):
    content: str = "CONTENT"


class NestedDataModel(DataModel):
    id: Optional[int] = Field(
        json_schema_extra={"primary_key": True, "autoincrement": True}, default=None
    )
    name: str = "NAME"


class DataModelTest(DataModel):
    id: Optional[int] = Field(
        json_schema_extra={"primary_key": True, "autoincrement": True}, default=None
    )
    string: str = "STRING"
    float_: float = 0.0
    int_: int = 0
    bool_: bool = False
    datetime_: datetime = datetime(2021, 1, 1, 0, 0, 0)
    date_: date = date(2021, 1, 1)
    bytes_: bytes = b"bytes"
    list_: list = [1, 2, 3]
    dict_: dict = {"key": "value"}
    nested_data_model: NestedDataModel = NestedDataModel()
    nested_base_model: NestedBaseModel = NestedBaseModel()
    unique_: str = Field(json_schema_extra={"unique": True}, default="UNIQUE")
    foreign_explicit: int = Field(
        json_schema_extra={"foreign_key": "NestedDataModel(id)"}, default=1
    )


@pytest.fixture
def data_source():
    with connect("database.db") as connection:
        connection.execute("DROP TABLE IF EXISTS NestedDataModel")
        connection.commit()

        connection.execute("DROP TABLE IF EXISTS DataModelTest")
        connection.commit()

    yield SQLite3DataSource(
        "database.db",
    )

    with connect("database.db") as connection:
        connection.execute("DROP TABLE IF EXISTS NestedDataModel")
        connection.commit()

        connection.execute("DROP TABLE IF EXISTS DataModelTest")
        connection.commit()


def test_create_source(data_source: SQLite3DataSource):
    data_source.create_source(DataModelTest)

    with connect("database.db") as connection:
        cursor = connection.execute(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='DataModelTest'"
        )
        result = cursor.fetchone()

    assert result[0] == "table"
    assert result[1] == "DataModelTest"
    assert result[2] == "DataModelTest"
    assert (
        result[4]
        == "CREATE TABLE DataModelTest (id INTEGER PRIMARY KEY AUTOINCREMENT, string TEXT NOT NULL, float_ REAL NOT NULL, int_ INTEGER NOT NULL, bool_ INTEGER NOT NULL, datetime_ TIMESTAMP NOT NULL, date_ DATE NOT NULL, bytes_ BLOB NOT NULL, list_ JSON NOT NULL, dict_ JSON NOT NULL, nested_data_model INTEGER NOT NULL, nested_base_model JSON NOT NULL, unique_ TEXT NOT NULL UNIQUE, foreign_explicit INTEGER NOT NULL, FOREIGN KEY (nested_data_model) REFERENCES NestedDataModel(id), FOREIGN KEY (foreign_explicit) REFERENCES NestedDataModel(id))"
    )

    with connect("database.db") as connection:
        cursor = connection.execute(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='NestedDataModel'"
        )
        result = cursor.fetchone()

    assert result[0] == "table"
    assert result[1] == "NestedDataModel"
    assert result[2] == "NestedDataModel"
    assert (
        result[4]
        == "CREATE TABLE NestedDataModel (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
    )


def test_create_existing_not_ignore(data_source: SQLite3DataSource):
    data_source.create_source(DataModelTest)

    with pytest.raises(DataSourceAlreadyExistsError):
        data_source.create_source(DataModelTest)


def test_create_existing_ignore(data_source: SQLite3DataSource):
    data_source.create_source(DataModelTest)

    data_source.create_source(DataModelTest, ignore_if_exists=True)
