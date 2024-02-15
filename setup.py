from setuptools import setup


setup(
    name="DataModel",
    version="0.1.0",
    description="A fast and efficient way to interact with databases using pydantic models",
    author="Tobias Ahlers",
    author_email="93325697+TobiasAhlers@users.noreply.github.com",
    install_requires=[
        "pydantic",
        "sqlalchemy",
    ],
)
