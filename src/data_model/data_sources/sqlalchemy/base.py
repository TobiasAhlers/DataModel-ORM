from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from types import UnionType
from typing import TYPE_CHECKING, Any, Union, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from sqlalchemy import (JSON, Boolean, Column, Date, DateTime, Float,
                        ForeignKey, Integer, LargeBinary, String, Table,
                        create_engine)
from sqlalchemy.orm import Session, registry

from ..base import DataSource

if TYPE_CHECKING:
    from ..base import DataModel


def extract_type(type_: type) -> type:
    """
    Extracts the type from a Union.

    Args:
        type_ (type): The type to extract.

    Returns:
        type: The extracted type.

    Raises:
        TypeError: If the Union has more than 2 types or if the Union does not include None.

    Example:
        >>> extract_type(Union[int, None])
        <class 'int'>
        >>> extract_type(int)
        <class 'int'>
        >>> extract_type(int | None)
        <class 'int'>
    """
    if get_origin(type_) is UnionType or get_origin(type_) is Union:
        if len(type_.__args__) > 2:
            raise TypeError(f"Union with more than 2 types is not supported: {type_}")
        if type(None) in type_.__args__:
            return next(t for t in type_.__args__ if t is not type(None))
        else:
            raise TypeError(f"Union without None is not supported: {type_}")
    try:
        if issubclass(get_origin(type_), Iterable):
            return Iterable
    except TypeError:
        pass
    return type_


def is_nullable(field: FieldInfo) -> bool:
    """
    Checks if a field is nullable.

    Args:
        field (FieldInfo): The field to check.

    Returns:
        bool: True if the field is nullable, False otherwise.

    Example:
        >>> is_nullable(FieldInfo(name="name", type=str))
        False
    """
    if (
        get_origin(field.annotation) is UnionType
        or get_origin(field.annotation) is Union
    ):
        if type(None) in field.annotation.__args__:
            return True
    return False


def get_sqlalchemy_type(type_: type) -> Any:
    """
    Converts a Python type to a SQLAlchemy type.

    Args:
        type_ (type): The Python type to convert.

    Returns:
        Any: The corresponding SQLAlchemy type.

    Raises:
        TypeError: If the type is not supported.

    Example:
        >>> get_sqlalchemy_type(int)
        <class 'sqlalchemy.sql.sqltypes.Integer'>
    """
    type_ = extract_type(type_)
    if issubclass(type_, BaseModel):
        try:
            primary_key = type_.get_primary_key()
            field = type_.model_fields[primary_key]
            if isinstance(field.annotation, BaseModel):
                raise TypeError(
                    f"Nested models are not supported as primary key: {type_}"
                )
            return get_sqlalchemy_type(field.annotation)
        except AttributeError:
            return JSON
    if issubclass(type_, str):
        return String
    if issubclass(type_, float):
        return Float
    if issubclass(type_, bool):
        return Boolean
    if issubclass(type_, int):
        return Integer
    if issubclass(type_, datetime):
        return DateTime
    if issubclass(type_, date):
        return Date
    if issubclass(type_, bytes):
        return LargeBinary
    if issubclass(type_, (Iterable, dict)):
        return JSON
    raise TypeError(f"Unsupported type: {type_}")


def get_foreign_key_from_field(field: FieldInfo) -> ForeignKey | None:
    """
    Get a foreign key from a field.

    Args:
        field (FieldInfo): The field to get the foreign key for.

    Returns:
        ForeignKey | None: The corresponding foreign key, or None if the field is not a foreign key.

    Example:
        >>> get_foreign_key_from_field(FieldInfo(name="name", type=str))
        None
    """
    if field.json_schema_extra == None:
        field.json_schema_extra = {}
    if "foreign_key" in field.json_schema_extra:
        return ForeignKey(field.json_schema_extra["foreign_key"])
    type_ = extract_type(field.annotation)

    if issubclass(type_, BaseModel):
        try:
            primary_key = type_.get_primary_key()

            return ForeignKey(f"{type_.__name__}.{primary_key}")
        except AttributeError:
            return None
    return None


def get_sqlalchemy_column(field_name: str, field: FieldInfo) -> Column:
    """
    Get a SQLAlchemy column for a given field.

    Args:
        field_name (str): The name of the field to get the column for.
        field (FieldInfo): The field to get the column for.

    Returns:
        Column: The corresponding SQLAlchemy column.

    Example:
        >>> get_sqlalchemy_column(FieldInfo(name="name", type=str, required=True))
        <class 'sqlalchemy.sql.schema.Column'>
    """
    if field.json_schema_extra == None:
        field.json_schema_extra = {}
    args = [get_sqlalchemy_type(field.annotation)]
    kwargs = {"name": field_name, "nullable": False}
    if "primary_key" in field.json_schema_extra:
        kwargs["primary_key"] = field.json_schema_extra["primary_key"]
    if "unique" in field.json_schema_extra:
        kwargs["unique"] = field.json_schema_extra["unique"]
    if get_foreign_key_from_field(field) is not None:
        args.append(get_foreign_key_from_field(field))
    if is_nullable(field):
        kwargs["nullable"] = True
    column = Column(
        *args,
        **kwargs,
    )
    return column


def get_sqlalchemy_table(
    data_model: type["DataModel"], data_source: SQLAlchemyDataSource
) -> Table:
    """
    Get a SQLAlchemy model for a given data model.

    Args:
        data_model (type["DataModel"]): The data model to get the model for.
        data_source (SQLAlchemyDataSource): The data source to get the model for.

    Returns:
        DeclarativeBase: The corresponding SQLAlchemy model.

    Example:
        >>> get_sqlalchemy_model(MyDataModel)
        <class 'sqlalchemy.ext.declarative.api.DeclarativeMeta'>
    """
    columns = []
    for field_name, field in data_model.model_fields.items():
        columns.append(get_sqlalchemy_column(field_name, field))
    return Table(data_model.__name__, data_source.registry.metadata, *columns)


class SQLAlchemyDataSource(DataSource):
    """
    SQLAlchemyDataSource is a subclass of DataSource that interacts with a SQLAlchemy database.
    """

    def __init__(self, database_url: str) -> None:
        """
        Initialize a new instance of SQLAlchemyDataSource.

        Args:
            database_url (str): The URL of the database to connect to.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
        """
        self.registry = registry()
        self.engine = create_engine(database_url)
        self.tables = {}

    def register_table(self, table: Table) -> None:
        """
        Register a table with the data source.

        Args:
            table (Table): The table to register.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> table = get_sqlalchemy_table(MyDataModel, ds)
            >>> ds.register_table(table)
        """

        def constructor(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        cls = type(
            table.name,
            (object,),
            {"__init__": constructor}
            | {column: None for column in table.columns.keys()},
        )
        self.registry.map_imperatively(cls, table)
        self.tables[table.name] = {
            "table": table,
            "class": cls,
        }

    def get_table(self, data_model: type["DataModel"]) -> Table:
        """
        Get a SQLAlchemy table for a given data model.

        Args:
            data_model (type["DataModel"]): The data model to get the table for.

        Returns:
            Table: The corresponding SQLAlchemy table.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> table = ds.get_table(MyDataModel)
        """
        if data_model.__name__ not in self.tables:
            self.register_table(get_sqlalchemy_table(data_model, self))
        return self.tables[data_model.__name__]["table"]

    def create_source(
        self, data_model: type["DataModel"], ignore_if_exists: bool = False
    ) -> None:
        """
        Create a new data source.

        Args:
            data_model (type["DataModel"]): The data model to create the source for.
            ignore_if_exists (bool, optional): If True, ignore the operation if the data source already exists. Default is False.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> ds.create_source(MyDataModel, ignore_if_exists=True)
        """
        raise NotImplementedError()

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
        """
        Get a single record from the data source.

        Args:
            data_model (type["DataModel"]): The data model to get the record for.
            where (dict): A dictionary of conditions to match the record against.

        Returns:
            Union["DataModel", None]: The matching record, or None if no record was found.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> record = ds.get_one(MyDataModel, {"id": 1})
        """
        raise NotImplementedError()

    def get_all(self, data_model: type["DataModel"], where: dict) -> list["DataModel"]:
        """
        Get all records from the data source that match the given conditions.

        Args:
            data_model (type["DataModel"]): The data model to get the records for.
            where (dict, optional): A dictionary of conditions to match the records against. Default is None.

        Returns:
            list["DataModel"]: The matching records.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> records = ds.get_all(MyDataModel, {"age": 21})
        """
        raise NotImplementedError()

    def save(self, data_model: DataModel) -> None:
        """
        Save a record to the data source.

        Args:
            data_model (type["DataModel"]): The data model to save.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> ds.save(MyDataModel)
        """
        raise NotImplementedError()

    def delete(self, data_model: DataModel) -> None:
        """
        Delete a record from the data source.

        Args:
            data_model (type["DataModel"]): The data model to delete the record from.
            where (dict): A dictionary of conditions to match the record against.

        Example:
            >>> ds = SQLAlchemyDataSource("sqlite:///database.db")
            >>> ds.delete(MyDataModel, {"id": 1})
        """
        raise NotImplementedError()
