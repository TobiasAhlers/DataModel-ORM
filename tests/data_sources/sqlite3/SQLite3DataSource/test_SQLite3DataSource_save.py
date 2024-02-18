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
    string: Optional[str] = "STRING"
    float_: Optional[float] = 0.0
    int_: Optional[int] = 0
    bool_: Optional[bool] = False
    datetime_: Optional[datetime] = datetime(2021, 1, 1, 0, 0, 0)
    date_: Optional[date] = date(2021, 1, 1)
    bytes_: Optional[bytes] = b"bytes"
    list_: Optional[list] = [1, 2, 3]
    dict_: Optional[dict] = {"key": "value"}
    nested_data_model: Optional[NestedDataModel] = NestedDataModel()
    nested_base_model: Optional[NestedBaseModel] = NestedBaseModel()
    unique_: Optional[str] = Field(json_schema_extra={"unique": True}, default="UNIQUE")
    foreign_explicit: Optional[int] = Field(
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

    yield data_source

    with connect("database.db") as connection:
        connection.execute("DROP TABLE IF EXISTS NestedDataModel")
        connection.commit()

        connection.execute("DROP TABLE IF EXISTS DataModelTest")
        connection.commit()


def test_save_simple(data_source: SQLite3DataSource):
    primary_key = data_source.save(
        DataModelTest(
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
    )
    assert primary_key == 1

    with connect("database.db") as connection:
        cursor = connection.execute("SELECT * FROM DataModelTest")
        assert cursor.fetchall() == [
            (
                1,
                "STRING",
                0.0,
                0,
                0,
                "2021-01-01T00:00:00",
                "2021-01-01",
                b"bytes",
                "[1, 2, 3]",
                '{"key": "value"}',
                1,
                '{"content": "CONTENT"}',
                "UNIQUE",
                1,
            )
        ]


def test_save_update(data_source: SQLite3DataSource):
    primary_key = data_source.save(
        DataModelTest(
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
    )
    assert primary_key == 1

    primary_key = data_source.save(
        DataModelTest(
            id=1,
            string="STRING2",
            float_=0.1,
            int_=1,
            bool_=True,
            datetime_=datetime(2021, 1, 1, 0, 0, 1),
            date_=date(2021, 1, 2),
            bytes_=b"bytes2",
            list_=[1, 2, 3, 4],
            dict_={"key": "value2"},
            nested_data_model=NestedDataModel(id=2, name="NAME2"),
            nested_base_model=NestedBaseModel(content="CONTENT2"),
            unique_="UNIQUE2",
            foreign_explicit=2,
        )
    )
    assert primary_key == 1

    with connect("database.db") as connection:
        cursor = connection.execute("SELECT * FROM DataModelTest")
        assert cursor.fetchall() == [
            (
                1,
                "STRING2",
                0.1,
                1,
                1,
                "2021-01-01T00:00:01",
                "2021-01-02",
                b"bytes2",
                "[1, 2, 3, 4]",
                '{"key": "value2"}',
                2,
                '{"content": "CONTENT2"}',
                "UNIQUE2",
                2,
            )
        ]


def test_save_without_id(data_source: SQLite3DataSource):
    primary_key = data_source.save(
        DataModelTest(
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
    )
    assert primary_key == 1

    with connect("database.db") as connection:
        cursor = connection.execute("SELECT * FROM DataModelTest")
        assert cursor.fetchall() == [
            (
                1,
                "STRING",
                0.0,
                0,
                0,
                "2021-01-01T00:00:00",
                "2021-01-01",
                b"bytes",
                "[1, 2, 3]",
                '{"key": "value"}',
                1,
                '{"content": "CONTENT"}',
                "UNIQUE",
                1,
            )
        ]
