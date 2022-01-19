import os
import pathlib


def default_database_path():
    return pathlib.Path(os.getcwd(), 'files.db')