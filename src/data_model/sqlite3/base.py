from typing import TYPE_CHECKING

from sqlalchemy import create_engine, Engine, delete, select
from sqlalchemy.dialects.sqlite import insert

from ..data_source import DataSource

from .utils import *

if TYPE_CHECKING:
    from ..base import DataModel


class SQLite3DataSource(DataSource):
    """
    SQLite3DataSource is a class that provides an interface to interact with a SQLite3 database.

    Attributes:
        database (str): The name of the SQLite3 database.
        __tables__ (dict): A dictionary to cache SQLAlchemy table objects.
    """

    def __init__(self, database: str) -> None:
        """
        The constructor for SQLite3DataSource class.

        Parameters:
            database (str): The name of the SQLite3 database.
        """
        self.database = database
        self.__tables__ = {}

    @property
    def engine(self) -> Engine:
        """
        The engine property returns a SQLAlchemy engine connected to the SQLite3 database.

        Returns:
            Engine: A SQLAlchemy engine.
        """
        return create_engine(f"sqlite:///{self.database}")

    def get_table(self, data_model: type["DataModel"]) -> Table:
        """
        The get_table method returns a SQLAlchemy table object for the given data model.

        Parameters:
            data_model (type["DataModel"]): The data model class.

        Returns:
            Table: A SQLAlchemy table object.
        """
        if data_model.__class__.__name__ not in self.__tables__:
            self.__tables__[data_model] = get_sqlalchemy_table(data_model)
        return self.__tables__[data_model]

    def create_source(
        self, data_model: type["DataModel"], ignore_if_exists: bool = False
    ) -> None:
        """
        The create_source method creates a table in the SQLite3 database for the given data model.

        Parameters:
            data_model (type["DataModel"]): The data model class.
            ignore_if_exists (bool): If True, the table will not be created if it already exists. Defaults to False.
        """
        table = self.get_table(data_model)
        table.create(bind=self.engine, checkfirst=ignore_if_exists)

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
        """
        The get_one method returns a single record from the table of the given data model that matches the where clause.

        Parameters:
            data_model (type["DataModel"]): The data model class.
            where (dict): A dictionary where the keys are the column names and the values are the values to match.

        Returns:
            DataModel: A data model object with the data of the record.
            None: If no record matches the where clause.

        Example:
            user = get_one(User, {"id": 1})
        """
        table = self.get_table(data_model)

        with self.engine.connect() as connection:
            query = select(table).where(
                *[table.c[key] == value for key, value in where.items()]
            )
            res = connection.execute(query)
            result = res.fetchone()
            if result:
                return data_model(
                    **{
                        column.name: value
                        for column, value in zip(table.columns, result)
                    }
                )
            return None

    def get_all(
        self, data_model: type["DataModel"], where: dict
    ) -> list["DataModel"]:
        """
        The get_all method returns all records from the table of the given data model that match the where clause.

        Parameters:
            data_model (type["DataModel"]): The data model class.
            where (dict): A dictionary where the keys are the column names and the values are the values to match.

        Returns:
            list[DataModel]: A list of data model objects with the data of the records.

        Example:
            users = get_all(User, {"country": "US"})
        """
        table = self.get_table(data_model)

        with self.engine.connect() as connection:
            query = select(table).where(
                *[table.c[key] == value for key, value in where.items()]
            )
            res = connection.execute(query)
            return [
                data_model(
                    **{
                        column.name: value
                        for column, value in zip(table.columns, result)
                    }
                )
                for result in res
            ]
        

    def save(self, data_model: "DataModel") -> Any:
        """
        The save method inserts or updates a record in the table of the given data model.

        Parameters:
            data_model (DataModel): The data model object with the data to insert or update.

        Returns:
            Any: The last row id of the inserted or updated record.

        Example:
            user = User(name="John Doe", email="john.doe@example.com")
            last_row_id = save(user)
        """
        table = self.get_table(data_model.__class__)

        with self.engine.connect() as connection:
            query = (
                insert(table)
                .values(**data_model.model_dump())
                .on_conflict_do_update(
                    index_elements=[data_model.get_primary_key()],
                    set_=data_model.model_dump(),
                )
            )
            res = connection.execute(query)
            connection.commit()
            return res.lastrowid

    def delete(self, data_model: "DataModel") -> None:
        """
        The delete method deletes a record in the table of the given data model.

        Parameters:
            data_model (DataModel): The data model object with the data of the record to delete.

        Example:
            user = get_one(User, {"id": 1})
            delete(user)
        """
        table = self.get_table(data_model.__class__)

        with self.engine.connect() as connection:
            query = delete(table).where(
                table.c[data_model.get_primary_key()]
                == getattr(data_model, data_model.get_primary_key())
            )
            connection.execute(query)
            connection.commit()
