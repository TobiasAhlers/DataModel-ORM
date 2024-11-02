import pytest

from sqlmodel import select, Session
from data_model_orm import *
from conftest import *


def test_DataModel_delete_existing(test_data_model: TestDataModel):
    entry = TestDataModel(id=1, name="John Doe", age=30)

    with Session(test_data_model.__engine__) as session:
        session.add(entry)
        session.commit()
        session.refresh(entry)

    entry.delete()

    with Session(test_data_model.__engine__) as session:
        result = session.exec(select(TestDataModel)).all()

    assert len(result) == 0


def test_DataModel_delete_non_existing(test_data_model: TestDataModel):
    entry = TestDataModel(name="John Doe", age=30)

    with pytest.raises(Exception):
        entry.delete()


def test_DataModel_delete_multiple_existing(test_data_model: TestDataModel):
    entry1 = TestDataModel(id=1, name="John Doe", age=30)
    entry2 = TestDataModel(id=2, name="Jane Doe", age=30)

    with Session(test_data_model.__engine__) as session:
        session.add_all([entry1, entry2])
        session.commit()

    entry1.delete()

    with Session(test_data_model.__engine__) as session:
        result = session.exec(select(TestDataModel)).all()

    assert len(result) == 1
    assert result[0].id == 2
    assert result[0].name == "Jane Doe"
    assert result[0].age == 30
