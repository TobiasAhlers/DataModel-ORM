from typing import TYPE_CHECKING

from sqlalchemy import create_engine, Engine, delete, select
from sqlalchemy.dialects.sqlite import insert

from ..data_source import DataSource

from .utils import *

if TYPE_CHECKING:
    from ..base import DataModel


class SQLite3DataSource(DataSource):

    def __init__(self, database: str) -> None:
        self.database = database
        self.__tables__ = {}

    @property
    def engine(self) -> Engine:
        return create_engine(f"sqlite:///{self.database}")

    def get_table(self, data_model: type["DataModel"]) -> Table:
        print(data_model.__class__.__name__)
        if data_model.__class__.__name__ not in self.__tables__:
            self.__tables__[data_model] = get_sqlalchemy_table(data_model)
        return self.__tables__[data_model]

    def create_source(
        self, data_model: type["DataModel"], ignore_if_exists: bool = False
    ) -> None:
        table = self.get_table(data_model)
        table.create(bind=self.engine, checkfirst=ignore_if_exists)

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
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
        table = self.get_table(data_model.__class__)

        with self.engine.connect() as connection:
            query = delete(table).where(
                table.c[data_model.get_primary_key()]
                == getattr(data_model, data_model.get_primary_key())
            )
            connection.execute(query)
            connection.commit()
