import pytest

from sqlite3 import connect
from typing import Optional
from pydantic import BaseModel, Field

from data_model_orm import *
from data_model_orm.data_sources.sqlite3 import *


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

    data_source = SQLite3DataSource(
        "database.db",
    )
    data_source.create_source(DataModelTest)

    with connect("database.db") as connection:
        connection.execute("INSERT INTO NestedDataModel (id, name) VALUES (1, 'NAME')")
        connection.commit()

        connection.execute(
            "INSERT INTO DataModelTest (id, string, float_, int_, bool_, datetime_, date_, bytes_, list_, dict_, nested_data_model, nested_base_model, unique_, foreign_explicit) VALUES (1, 'STRING', 0.0, 0, 0, '2021-01-01 00:00:00', '2021-01-01', 'bytes', '[1, 2, 3]', '{\"key\": \"value\"}', 1, '{\"content\": \"CONTENT\"}', 'UNIQUE', 1)"
        )
        connection.commit()

    yield data_source

    with connect("database.db") as connection:
        connection.execute("DROP TABLE IF EXISTS NestedDataModel")
        connection.commit()

        connection.execute("DROP TABLE IF EXISTS DataModelTest")
        connection.commit()


def test_get_simple(data_source: SQLite3DataSource):
    assert data_source.get_one(
        data_model=DataModelTest, where={"id": 1}
    ) == DataModelTest(
        id=1,
        string="STRING",
        float_=0.0,
        int_=0,
        bool_=False,
        datetime_=datetime(2021, 1, 1, 0, 0, 0),
        date_=date(2021, 1, 1),
        bytes_=b"bytes",
        list_=[1, 2, 3],
        dict_={"key": "value"},
        nested_data_model=NestedDataModel(id=1, name="NAME"),
        nested_base_model=NestedBaseModel(content="CONTENT"),
        unique_="UNIQUE",
        foreign_explicit=1,
    )


def test_get_none(data_source: SQLite3DataSource):
    assert data_source.get_one(data_model=DataModelTest, where={"id": 10}) is None


def test_get_multiple(data_source: SQLite3DataSource):
    assert data_source.get_one(
        data_model=DataModelTest, where={"id": 1, "unique_": "UNIQUE"}
    ) == DataModelTest(
        id=1,
        string="STRING",
        float_=0.0,
        int_=0,
        bool_=False,
        datetime_=datetime(2021, 1, 1, 0, 0, 0),
        date_=date(2021, 1, 1),
        bytes_=b"bytes",
        list_=[1, 2, 3],
        dict_={"key": "value"},
        nested_data_model=NestedDataModel(id=1, name="NAME"),
        nested_base_model=NestedBaseModel(content="CONTENT"),
        unique_="UNIQUE",
        foreign_explicit=1,
    )


def test_get_base_model(data_source: SQLite3DataSource):
    assert data_source.get_one(
        data_model=DataModelTest,
        where={"nested_base_model": NestedBaseModel(content="CONTENT")},
    ) == DataModelTest(
        id=1,
        string="STRING",
        float_=0.0,
        int_=0,
        bool_=False,
        datetime_=datetime(2021, 1, 1, 0, 0, 0),
        date_=date(2021, 1, 1),
        bytes_=b"bytes",
        list_=[1, 2, 3],
        dict_={"key": "value"},
        nested_data_model=NestedDataModel(id=1, name="NAME"),
        nested_base_model=NestedBaseModel(content="CONTENT"),
        unique_="UNIQUE",
        foreign_explicit=1,
    )


def test_get_nested_data_model(data_source: SQLite3DataSource):
    assert data_source.get_one(
        data_model=DataModelTest,
        where={"nested_data_model": NestedDataModel(id=1, name="NAME")},
    ) == DataModelTest(
        id=1,
        string="STRING",
        float_=0.0,
        int_=0,
        bool_=False,
        datetime_=datetime(2021, 1, 1, 0, 0, 0),
        date_=date(2021, 1, 1),
        bytes_=b"bytes",
        list_=[1, 2, 3],
        dict_={"key": "value"},
        nested_data_model=NestedDataModel(id=1, name="NAME"),
        nested_base_model=NestedBaseModel(content="CONTENT"),
        unique_="UNIQUE",
        foreign_explicit=1,
    )


def test_get_nested_data_model_id(data_source: SQLite3DataSource):
    assert data_source.get_one(
        data_model=DataModelTest, where={"nested_data_model": 1}
    ) == DataModelTest(
        id=1,
        string="STRING",
        float_=0.0,
        int_=0,
        bool_=False,
        datetime_=datetime(2021, 1, 1, 0, 0, 0),
        date_=date(2021, 1, 1),
        bytes_=b"bytes",
        list_=[1, 2, 3],
        dict_={"key": "value"},
        nested_data_model=NestedDataModel(id=1, name="NAME"),
        nested_base_model=NestedBaseModel(content="CONTENT"),
        unique_="UNIQUE",
        foreign_explicit=1,
    )
