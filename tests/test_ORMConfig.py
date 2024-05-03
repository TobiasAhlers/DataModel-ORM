import pytest

from data_model_orm.base import DataModel, ORMConfig


test_location = "test.db"

def test_default_orm_config():
    assert ORMConfig().db_location == "database.db"
    assert ORMConfig().__dict__ == {"db_location": "database.db", "ignore_if_exists": False}


def test_configured_orm_config_instance():
    test_instance = ORMConfig(db_location=test_location, ignore_if_exists=True)
    assert test_instance.db_location == test_location
    assert test_instance.ignore_if_exists == True
    assert test_instance.__dict__ == {"db_location": "test.db", "ignore_if_exists": True}


def test_default_data_model():
    assert DataModel.__orm_config__ == ORMConfig()
    assert DataModel.__orm_config__.db_location == "database.db"
    assert DataModel.__orm_config__.ignore_if_exists == False
    assert DataModel.__data_source__.database == "database.db"


def test_configured_data_model():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_location=test_location,
            ignore_if_exists=True
            )
    
    assert TestModel.__orm_config__ == ORMConfig(db_location=test_location, ignore_if_exists=True)
    assert TestModel.__orm_config__.ignore_if_exists == True
    assert TestModel.__orm_config__.db_location == test_location
    assert TestModel.__data_source__.database == test_location


def test_configured_data_model_instance():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_location=test_location,
            ignore_if_exists=True
            )
    
    test_instance = TestModel()
    
    assert test_instance.__orm_config__ == ORMConfig(db_location=test_location, ignore_if_exists=True)
    assert test_instance.__orm_config__.ignore_if_exists == True
    assert test_instance.__orm_config__.db_location == test_location
    assert test_instance.__data_source__.database == test_location


def test_config_not_accepted_as_argument():
    test_instance = DataModel(
        __orm_config__ = ORMConfig(
            db_location=test_location
            )
        )
    assert test_instance.__orm_config__ == ORMConfig()
    assert test_instance.__data_source__.database == "database.db"