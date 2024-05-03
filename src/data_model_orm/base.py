from typing import ClassVar, Self

from pydantic import BaseModel, Field

from .data_sources import DataSource
from .data_sources.sqlite3 import SQLite3DataSource


class ORMConfig(BaseModel):
    db_path: str = Field(default="database.db", description="Database location")
    db_backend: type = Field(default=SQLite3DataSource, description="Database backend")
    ignore_if_exists: bool = Field(default=False, description="Ignore if the data source already exists")

class DataModelMeta(type):
    @property
    def __data_source__(self) -> DataSource:
        return self.__orm_config__.db_backend(database=self.__orm_config__.db_path)
    
    @__data_source__.setter
    def __data_source__(self, value):
        raise AttributeError("Can't set attribute")

class CombinedMeta(DataModelMeta, BaseModel.__class__):
    pass

class DataModel(BaseModel, metaclass=CombinedMeta):
    """
    A base class for data models that interact with a data source.

    Attributes:
        __orm_config__ (ORMConfig): The configuration for the data model. Default is ORMConfig().
        __data_source__ (DataSource): The data source to interact with. Default is SQLite3DataSource.

    Example:
        class User(DataModel):
            name: str
            age: int
    """
    
    __orm_config__: ClassVar[ORMConfig] = ORMConfig()
    
    def __getattribute__(self, name):
        if name == "__data_source__":
            return type(self).__data_source__
        return super().__getattribute__(name)
    
    def __setattr__(self, name, value):
        if name == "__data_source__":
            raise AttributeError("Can't set attribute")
        super().__setattr__(name, value)

    @classmethod
    def get_primary_key(cls) -> str:
        """
        Get the primary key field of the data model.

        Returns:
            str: The name of the primary key field.

        Raises:
            ValueError: If no primary key is found.

        Example:
            >>> User.get_primary_key()
            'id'
        """
        for field_name, field in cls.model_fields.items():
            if not field.json_schema_extra:
                field.json_schema_extra = {}
            if field.json_schema_extra.get("primary_key", False):
                return field_name
        raise ValueError(f"Missing primary key in {cls.__name__}")

    @classmethod
    def create_source(cls, ignore_if_exists: bool = __orm_config__.ignore_if_exists) -> None:
        """
        Create the data source for the data model.

        Args:
            ignore_if_exists (bool, optional): If True, ignore the operation if the data source already exists. Default is False.

        Example:
            >>> User.create_source(ignore_if_exists=True)
        """
        cls.__data_source__.create_source(cls, ignore_if_exists)

    @classmethod
    def get_one(cls, **where) -> Self | None:
        """
        Get a single record that matches the given conditions.

        Args:
            **where: Conditions to match.

        Returns:
            Self | None: The matching record, or None if no record matches.

        Example:
            >>> user = User.get_one(name='John Doe')
            >>> print(user)
            User(name='John Doe', age=30)
        """
        return cls.__data_source__.get_one(data_model=cls, where=where)

    @classmethod
    def get_all(cls, **where) -> list[Self]:
        """
        Get all records that match the given conditions.

        Args:
            **where: Conditions to match.

        Returns:
            list[Self]: A list of matching records.

        Example:
            >>> users = User.get_all(age=30)
            >>> print(users)
            [User(name='John Doe', age=30), User(name='Jane Doe', age=30)]
        """
        return cls.__data_source__.get_all(data_model=cls, where=where)

    def save(self) -> None:
        """
        Save the current record to the data source.

        Example:
            >>> user = User(name='John Doe', age=30)
            >>> user.save()
        """

        response = self.__data_source__.save(self)
        primary_key = self.get_primary_key()
        setattr(self, primary_key, response)

    def delete(self) -> None:
        """
        Delete the current record from the data source.

        Example:
            >>> user = User.get_one(name='John Doe')
            >>> user.delete()
        """
        self.__data_source__.delete(self)
