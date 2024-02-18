from setuptools import setup

from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


setup(
    name="data_model",
    version="0.1.0",
    description="A fast and efficient way to interact with databases using pydantic models",
    author="Tobias Ahlers",
    author_email="93325697+TobiasAhlers@users.noreply.github.com",
    install_requires=["pydantic"],
    long_description=long_description,
    long_description_content_type="text/markdown",
)
