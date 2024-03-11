from sqlite3 import connect, OperationalError
from collections.abc import Iterable
from datetime import date, datetime
from json import dumps, loads, JSONDecodeError
from typing import TYPE_CHECKING, Union, Any, Literal
from re import match

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ..base import DataSource
from ..errors import *

from .utils import extract_type, is_nullable

if TYPE_CHECKING:
    from ..base import DataModel


def is_data_model(type_: type | BaseModel) -> bool:
    """
    Check if a type is a data model.

    Args:
        type_ (type | BaseModel): The type to check.

    Returns:
        bool: True if the type is a data model, False otherwise.

    Example:
        >>> class User(DataModel):
        ...     name: str
        ...     age: int
        >>> is_data_model(User)
        True

        >>> is_data_model(str)
        False
    """
    try:
        return issubclass(type_, BaseModel) and hasattr(type_, "get_primary_key")
    except TypeError:
        return isinstance(type_, BaseModel) and hasattr(type_, "get_primary_key")


def adapt_value(value: Any, mode: Literal["get", "save"] = "get") -> Any:
    """
    Adapt a value to a format that can be stored in a SQLite3 database.

    Args:
        value (Any): The value to adapt.
        mode (Literal["get", "save"], optional): The mode to adapt the value for. Default is "get".

    Returns:
        Any: The adapted value.

    Example:
        >>> adapt_value({"key": "value"})
        '{"key": "value"}'
    """
    if isinstance(value, (dict, list)):
        return dumps(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, BaseModel):
        if is_data_model(value):
            if mode == "save":
                return value.__data_source__.save(data_model=value)
            elif mode == "get":
                primary_key = value.get_primary_key()
                primary_key_value = getattr(value, primary_key)
                if primary_key_value:
                    return primary_key_value
                else:
                    return getattr(
                        value.__data_source__.get_one(
                            data_model=value.__class__,
                            where={
                                field: getattr(value, field)
                                for field in value.model_fields.keys()
                            },
                        ),
                        primary_key,
                    )

            raise ValueError(f"Invalid mode: {mode}. Please use 'get' or 'save'.")
        return dumps(value.model_dump())
    return value


def convert_value(value: Any, type_: type) -> Any:
    """
    Convert a value to a specific type.

    Args:
        value (Any): The value to convert.
        type_ (type): The type to convert the value to.

    Returns:
        Any: The converted value.

    Example:
        >>> convert_value("2021-01-01", date)
        datetime.date(2021, 1, 1)
    """
    if issubclass(type_, (datetime, date)):
        return type_.fromisoformat(value)
    if issubclass(type_, BaseModel):
        if is_data_model(type_):
            return type_.get_one(**{type_.get_primary_key(): value})
        return type_.model_validate_json(value)
    if issubclass(type_, (str, int, float, bool, bytes)):
        return value
    if issubclass(type_, (dict, list, Iterable)):
        try:
            return loads(value)
        except JSONDecodeError:
            raise ConversionError(
                f"Unable to decode JSON: {value}. Please use a valid JSON string."
            )
    return value


def convert_query_result_to_data_model(
    record: tuple, data_model: type["DataModel"]
) -> "DataModel":
    """
    Convert a query result to a data model.

    Args:
        record (tuple): The query result to convert to a data model.
        data_model (DataModel): The data model to convert the query result to.

    Returns:
        DataModel: The converted data model.

    Example:
        >>> convert_query_result_to_data_model(("John", 30), User)
        User(name='John', age=30)
    """
    args = {}
    for i, field_name in enumerate(data_model.model_fields.keys()):
        type_ = extract_type(data_model.model_fields[field_name].annotation)
        args[field_name] = convert_value(record[i], type_)
    return data_model(**args)


def get_sqlite3_type(type_: type) -> str:
    """
    Get the SQLite3 type for a given type.

    Args:
        type_ (type): The type to get the SQLite3 type for.

    Returns:
        str: The SQLite3 type.

    Example:
        >>> get_sqlite3_type(int)
        'INTEGER'
        >>> get_sqlite3_type(str)
        'TEXT'
        >>> get_sqlite3_type(float)
        'REAL'
    """
    type_ = extract_type(type_)
    if issubclass(type_, BaseModel):
        if is_data_model(type_):
            primary_key = type_.get_primary_key()
            field = type_.model_fields[primary_key]
            if isinstance(field.annotation, BaseModel):
                raise TypeError(
                    f"Nested models are not supported as primary key: {type_}"
                )
            return get_sqlite3_type(field.annotation)
        return "JSON"
    if issubclass(type_, str):
        return "TEXT"
    if issubclass(type_, float):
        return "REAL"
    if issubclass(type_, (bool, int)):
        return "INTEGER"
    if issubclass(type_, datetime):
        return "TIMESTAMP"
    if issubclass(type_, date):
        return "DATE"
    if issubclass(type_, bytes):
        return "BLOB"
    if issubclass(type_, (Iterable, dict)):
        return "JSON"
    raise TypeError(
        f"Unsupported type for SQLite3: {type_}. Please use a supported type or a custom field."
    )


def get_foreign_key_from_field(field_name: str, field: FieldInfo) -> str | None:
    """
    Get the foreign key for a field.

    Args:
        field_name (str): The name of the field to get the foreign key for.
        field (FieldInfo): The field to get the foreign key for.

    Returns:
        str: The foreign key.

    Example:
        >>> get_foreign_key_from_field("other_id", FieldInfo(name="other_id", type=int, json_schema_extra={"foreign_key": "OtherModel.id"}))
        'FOREIGN KEY (other_id) REFERENCES OtherModel(id)'
    """
    if field.json_schema_extra == None:
        field.json_schema_extra = {}
    if "foreign_key" in field.json_schema_extra:
        if bool(match(r"^\w+\(id\)$", field.json_schema_extra["foreign_key"])) == False:
            raise ValueError(
                f"Invalid foreign key format: {field.json_schema_extra['foreign_key']}. The format should be 'ModelName(id)'"
            )
        return f"FOREIGN KEY ({field_name}) REFERENCES {field.json_schema_extra['foreign_key']}"
    type_ = extract_type(field.annotation)

    if issubclass(type_, BaseModel):
        if is_data_model(type_):
            primary_key = type_.get_primary_key()
            return (
                f"FOREIGN KEY ({field_name}) REFERENCES {type_.__name__}({primary_key})"
            )
        return None
    return None


def get_sqlite3_column(field_name: str, field: FieldInfo) -> str:
    """
    Get the SQLite3 column for a field.

    Args:
        field_name (str): The name of the field to get the SQLite3 column for.
        field (FieldInfo): The field to get the SQLite3 column for.

    Returns:
        str: The SQLite3 column.

    Example:
        >>> get_sqlite3_column("name", FieldInfo(name="name", type=str))
        'name TEXT'
    """
    if field.json_schema_extra == None:
        field.json_schema_extra = {}
    column = f"{field_name} {get_sqlite3_type(field.annotation)}"
    if not is_nullable(field):
        column += " NOT NULL"
    if "primary_key" in field.json_schema_extra:
        column += " PRIMARY KEY"
    if "autoincrement" in field.json_schema_extra:
        if get_sqlite3_type(field.annotation) != "INTEGER":
            raise ValueError(
                f"Autoincrement is only supported for INTEGER: {field_name} in {field}"
            )
        column += " AUTOINCREMENT"
    if "unique" in field.json_schema_extra:
        column += " UNIQUE"
    return column


def generate_create_table_query(
    model: "DataModel", ignore_if_exists: bool = False
) -> str:
    """
    Generate the CREATE TABLE query for a data model.

    Args:
        model (DataModel): The data model to generate the CREATE TABLE query for.
        ignore_if_exists (bool): If True, ignore the operation if the table already exists.

    Returns:
        str: The CREATE TABLE query.

    Example:
        >>> class User(DataModel):
        ...     name: str
        ...     age: int
        >>> generate_create_table_query(User, ignore_if_exists=True)
        'CREATE TABLE User (name TEXT NOT NULL, age INTEGER NOT NULL)'
    """
    fields = []
    primary_keys = []
    constraints = []
    for field_name, field in model.model_fields.items():
        if not field.json_schema_extra:
            field.json_schema_extra = {}
        if field.json_schema_extra.get("primary_key", False):
            primary_keys.append(field_name)
        fields.append(get_sqlite3_column(field_name, field))
        foreign_key = get_foreign_key_from_field(field_name, field)
        if foreign_key:
            constraints.append(foreign_key)

    if len(primary_keys) > 1:
        raise ValueError(
            f"Multiple primary keys are not supported: {primary_keys} in {model.__name__}"
        )
    if len(primary_keys) == 0:
        raise ValueError(f"Missing primary key in {model.__name__}")

    return f"CREATE TABLE {'IF NOT EXISTS ' if ignore_if_exists else ''}{model.__name__} ({', '.join(fields + constraints)})"


def generate_get_query(
    data_model: type["DataModel"], where_attributes: list[str], limit: int = None
) -> str:
    """
    Generate the SELECT query for a data model.

    Args:
        data_model (DataModel): The data model to generate the SELECT query for.
        where_attributes (list[str]): The attributes to filter the SELECT query by.
        limit (int): The LIMIT clause for the SELECT query.

    Returns:
        str: The SELECT query.

    Example:
        >>> class User(DataModel):
        ...     name: str = Field(json_schema_extra={"primary_key": True})
        ...     age: int
        >>> generate_get_query(User, {"age": 20}, 1)
        'SELECT * FROM User WHERE age = 20 LIMIT 1'
    """
    limit_clause = ""
    if limit:
        if limit < 0:
            raise ValueError(f"Limit should be a positive integer: {limit}")
        limit_clause = f" LIMIT {limit}"

    where_clause = ""
    if where_attributes != []:
        where_clause = " WHERE " + " AND ".join(
            [f"{attr} = ?" for attr in where_attributes]
        )
    return f"SELECT * FROM {data_model.__name__}{where_clause}{limit_clause}"


def generate_save_query(data_model: type["DataModel"]) -> str:
    """
    Generate the INSERT query for a data model.

    Args:
        data_model (DataModel): The data model to generate the INSERT query for.

    Returns:
        str: The INSERT query.

    Example:
        >>> class User(DataModel):
        ...     name: str = Field(json_schema_extra={"primary_key": True})
        ...     age: int
        >>> generate_save_query(User)
        'INSERT INTO User (name, age) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET age = excluded.age'
    """
    insert_fields = []
    update_fields = []
    for field_name, field in data_model.model_fields.items():
        if field.json_schema_extra == None:
            field.json_schema_extra = {}

        insert_fields.append(field_name)
        if not "primary_key" in field.json_schema_extra:
            update_fields.append(field_name)

    return f"INSERT INTO {data_model.__name__} ({', '.join(insert_fields)}) VALUES ({', '.join(['?' for _ in insert_fields])}) ON CONFLICT ({data_model.get_primary_key()}) DO UPDATE SET {', '.join([f'{field} = excluded.{field}' for field in update_fields])}"


def generate_delete_query(data_model: type["DataModel"]) -> str:
    """
    Generate the DELETE query for a data model.

    Args:
        data_model (DataModel): The data model to generate the DELETE query for.

    Returns:
        str: The DELETE query.

    Example:
        >>> class User(DataModel):
        ...     name: str = Field(json_schema_extra={"primary_key": True})
        ...     age: int
        >>> generate_delete_query(User)
        'DELETE FROM User WHERE name = ?'
    """
    return f"DELETE FROM {data_model.__name__} WHERE {data_model.get_primary_key()} = ?"


class SQLite3DataSource(DataSource):
    """
    A data source for SQLite3 databases.

    Example:
        >>> class User(DataModel):
        ...     __data_source__ = SQLite3DataSource(database="database.db")
        ...     name: str
        ...     age: int
    """

    def __init__(self, database: str) -> None:
        self.database = database

    def create_source(
        self, data_model: type["DataModel"], ignore_if_exists: bool = False
    ) -> None:
        """
        Create the data source for a data model.

        Args:
            data_model (DataModel): The data model to create the data source for.
            ignore_if_exists (bool): If True, ignore the operation if the data source already exists.

        Raises:
            ValueError: If the data model has no primary key.
            ValueError: If the data model has multiple primary keys.
            TypeError: If the data model has an unsupported type.
            DataSourceAlreadyExistsError: If the data source already exists and ignore_if_exists is False.

        Example:
            >>> User.__data_source__.create_source(User, ignore_if_exists=True)
        """
        with connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            cursor = connection.cursor()

            for field in data_model.model_fields.values():
                type_ = extract_type(field.annotation)
                if issubclass(extract_type(type_), BaseModel):
                    if is_data_model(type_):
                        self.create_source(type_, ignore_if_exists)
            try:
                cursor.execute(
                    generate_create_table_query(data_model, ignore_if_exists)
                )
                connection.commit()
            except OperationalError as e:
                if "already exists" in str(e):
                    if ignore_if_exists:
                        return
                    raise DataSourceAlreadyExistsError(
                        f"Data source already exists: {data_model.__name__}"
                    ) from e
                raise

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
        """
        Get a single record from the data source.

        Args:
            data_model (DataModel): The data model to get the record for.
            where (dict): A dictionary of conditions to match the record against.

        Returns:
            DataModel | None: The matching record, or None if no record was found.

        Example:
            >>> user = User.__data_source__.get_one(User, {"name": "John"})
            User(name='John', age=30)
        """
        with connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys = ON")

            cursor = connection.cursor()
            cursor.execute(
                generate_get_query(data_model, list(where.keys()), 1),
                [adapt_value(value) for value in where.values()],
            )

            record = cursor.fetchone()
            if record:
                return convert_query_result_to_data_model(record, data_model)
            return None

    def get_all(self, data_model: type["DataModel"], where: dict) -> list["DataModel"]:
        """
        Get all records from the data source.

        Args:
            data_model (DataModel): The data model to get the records for.
            where (dict): A dictionary of conditions to match the records against.

        Returns:
            list[DataModel]: A list of matching records.

        Example:
            >>> users = User.__data_source__.get_all(User, {"age": 30})
            [User(name='John', age=30), User(name='Jane', age=30)]
        """
        with connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys = ON")

            cursor = connection.cursor()
            cursor.execute(
                generate_get_query(data_model, list(where.keys())),
                [adapt_value(value) for value in where.values()],
            )

            return [
                convert_query_result_to_data_model(record, data_model)
                for record in cursor.fetchall()
            ]

    def save(self, data_model: "DataModel") -> Any:
        """
        Save a record to the data source.

        Args:
            data_model (DataModel): The data model to save.

        Returns:
            Any: The result of the save operation.

        Example:
            >>> User(name="John", age=30).__data_source__.save(User(name="John", age=30))
        """
        with connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys = ON")

            cursor = connection.cursor()
            cursor.execute(
                generate_save_query(data_model.__class__),
                [
                    adapt_value(getattr(data_model, field), mode="save")
                    for field in data_model.model_fields.keys()
                ],
            )
            connection.commit()
            if cursor.lastrowid > 0:
                return cursor.lastrowid
            return getattr(data_model, data_model.get_primary_key())

    def delete(self, data_model: "DataModel") -> None:
        """
        Delete a record from the data source.

        Args:
            data_model (DataModel): The data model to delete.

        Example:
            >>> User(name="John", age=30).__data_source__.delete(User(name="John", age=30))
        """
        with connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys = ON")

            cursor = connection.cursor()
            cursor.execute(
                generate_delete_query(data_model.__class__),
                [getattr(data_model, data_model.get_primary_key())],
            )
            connection.commit()
