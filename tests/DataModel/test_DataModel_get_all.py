import pytest

from data_model_orm import *
from sqlmodel import Session
from conftest import *


def test_DataModel_get_all_existing(test_data_model: TestDataModel):
    with Session(test_data_model.__engine__) as session:
        session.add_all(
            [
                TestDataModel(id=1, name="John Doe", age=30),
                TestDataModel(id=2, name="Jane Doe", age=30),
            ]
        )
        session.commit()

    result = TestDataModel.get_all()
    assert len(result) == 2
    assert result[0].id == 1
    assert result[0].name == "John Doe"
    assert result[0].age == 30
    assert result[1].id == 2
    assert result[1].name == "Jane Doe"
    assert result[1].age == 30


def test_DataModel_get_all_non_existing(test_data_model: TestDataModel):
    with Session(test_data_model.__engine__) as session:
        session.add_all(
            [
                TestDataModel(id=1, name="John Doe", age=30),
                TestDataModel(id=2, name="Jane Doe", age=30),
            ]
        )
        session.commit()

    result = TestDataModel.get_all(id=3)
    assert len(result) == 0
