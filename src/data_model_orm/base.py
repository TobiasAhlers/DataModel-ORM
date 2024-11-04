from typing import ClassVar, Self

from sqlmodel import SQLModel, Field, select, Session
from sqlalchemy import Engine


class DataModel(SQLModel):
    """
    Base class for data models using SQLModel and SQLAlchemy.
    Provides common methods for database operations.
    """

    __engine__: ClassVar[Engine]

    @classmethod
    def get_primary_key(cls) -> str:
        """
        Get the primary key field name of the model.

        Returns:
            str: The name of the primary key field.

        Raises:
            ValueError: If no primary key is found in the model.
        """
        for field_name, field in cls.model_fields.items():
            if field.primary_key:
                return field_name
        raise ValueError(f"Missing primary key in {cls.__name__}")

    @classmethod
    def create_source(cls, ignore_if_exists: bool = False) -> None:
        """
        Create the database table for the model.

        Args:
            ignore_if_exists (bool): If True, will not raise an error if the table already exists.
        """
        cls.metadata.create_all(
            bind=cls.__engine__, checkfirst=ignore_if_exists, tables=[cls.__table__]
        )

    @classmethod
    def get_one(cls, **where) -> Self | None:
        """
        Retrieve a single record from the database that matches the given criteria.

        Args:
            **where: Arbitrary keyword arguments representing the filter criteria.

        Returns:
            Self | None: An instance of the model if a match is found, otherwise None.
        """
        with Session(cls.__engine__) as session:
            statement = select(cls)
            for key, value in where.items():
                statement = statement.where(getattr(cls, key) == value)
            return session.exec(statement).first()

    @classmethod
    def get_all(cls, **where) -> list[Self]:
        """
        Retrieve all records from the database that match the given criteria.

        Args:
            **where: Arbitrary keyword arguments representing the filter criteria.

        Returns:
            list[Self]: A list of instances of the model that match the criteria.
        """
        with Session(cls.__engine__) as session:
            statement = select(cls)
            for key, value in where.items():
                statement = statement.where(getattr(cls, key) == value)
            return session.exec(statement).all()

    def save(self) -> None:
        """
        Save the current instance to the database.
        If the instance is new, it will be added. If it already exists, it will be updated.
        """
        with Session(self.__engine__) as session:
            if getattr(self, self.get_primary_key()) is not None:
                entry = session.get(self.__class__, getattr(self, self.get_primary_key()))
                if entry:
                    for key, value in self.model_dump().items():
                        setattr(entry, key, value)
                    self = entry
            session.add(self)
            session.commit()
            session.refresh(self)

    def delete(self) -> None:
        """
        Delete the current instance from the database.
        """
        with Session(self.__engine__) as session:
            session.delete(self)
            session.commit()