# DataModel ORM

DataModel ORM is a Python library that provides a fast and efficient way to interact with databases using Pydantic models.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install DataModel.

    pip install datamodel_orm

## Usage

This example demonstrates how to use the User data model in Python. The User data model is defined using Pydantic and includes fields for id, name, and age.

### Setup
First, import the necessary modules and define the User data model.

    from typing import Optional
    from pydantic import Field
    from data_model_orm import DataModel

    class User(DataModel):
        id: Optional[int] = Field(json_schema_extra={"primary_key": True}, default=None)
        name: str
        age: int

### Creating the Data Source
Create the data source for the User model. If the data source already exists, this operation will be ignored.

    User.create_source(ignore_if_exists=True)

### Saving a New User
To save a user, instantiate the User class and call the save method.

    user = User(name="John", age=30)
    user.save()
    print(user.id)  # Prints the ID of the newly created user

### Retrieving a User
To retrieve a user, use the get_one method and provide the user's name.

    user = User.get_one(name="John")

### Retrieving All Users with a Specific Name
To retrieve all users with a specific name, use the get_all method and provide the name.

    users = User.get_all(name="John")

### Deleting a User
To delete a user, call the delete method on a User instance.

    user.delete()

Please note that the actual usage may vary depending on the implementation of the DataModel class and the data source.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.