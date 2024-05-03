import pytest

from data_model_orm.base import DataModel, ORMConfig
from data_model_orm.data_sources.sqlite3 import SQLite3DataSource


test_location = "test.db"

def test_default_orm_config():
    test_instance = ORMConfig()
    assert test_instance.db_path == "database.db"
    assert test_instance.ignore_if_exists == False
    assert test_instance.db_backend == SQLite3DataSource


def test_configured_orm_config_instance():
    test_instance = ORMConfig(db_path=test_location, ignore_if_exists=True, db_backend=SQLite3DataSource)
    assert test_instance.db_path == test_location
    assert test_instance.ignore_if_exists == True
    assert test_instance.db_backend == SQLite3DataSource


def test_default_data_model():
    assert DataModel.__orm_config__ == ORMConfig()
    assert DataModel.__orm_config__.db_path == "database.db"
    assert DataModel.__orm_config__.ignore_if_exists == False
    assert DataModel.__orm_config__.db_backend == SQLite3DataSource
    assert DataModel.__data_source__.database == "database.db"


def test_configured_data_model():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_path=test_location,
            ignore_if_exists=True,
            db_backend=SQLite3DataSource
            )
    
    assert TestModel.__orm_config__ == ORMConfig(db_path=test_location, ignore_if_exists=True)
    assert TestModel.__orm_config__.ignore_if_exists == True
    assert TestModel.__orm_config__.db_path == test_location
    assert TestModel.__data_source__.database == test_location


def test_configured_data_model_instance():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_path=test_location,
            ignore_if_exists=True
            )
    
    test_instance = TestModel()
    
    assert test_instance.__orm_config__ == ORMConfig(db_path=test_location, ignore_if_exists=True)
    assert test_instance.__orm_config__.ignore_if_exists == True
    assert test_instance.__orm_config__.db_path == test_location
    assert test_instance.__data_source__.database == test_location


def test_config_not_accepted_as_argument():
    test_instance = DataModel(
        __orm_config__ = ORMConfig(
            db_path=test_location
            )
        )
    assert test_instance.__orm_config__ == ORMConfig()
    assert test_instance.__data_source__.database == "database.db"

def test_data_source_immutable():
    test_instance = DataModel()
    with pytest.raises(AttributeError):
        test_instance.__data_source__ = SQLite3DataSource(database="test.db")