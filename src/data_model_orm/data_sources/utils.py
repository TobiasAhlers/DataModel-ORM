from collections.abc import Iterable
from typing import get_origin, Union
from types import UnionType
from pydantic.fields import FieldInfo


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
        if issubclass(get_origin(type_), dict):
            return dict
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
