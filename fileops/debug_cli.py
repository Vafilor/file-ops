import datetime
import os
import pathlib
import time
import sys
import json
from os.path import join

import click

from fileops.cli_common import default_database_path
from fileops.database.database import FileDatabase
from fileops.files.file import File

@click.group(name="debug", help="Debug commands to help time parts of the app and test behavior")
def debug_cli():
    pass


@debug_cli.command()
@click.argument('path', type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
@click.option('-o', '--output', 'output_path',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to a file to store output as a sqlite database.")
def walk_path(path, output_path):
    start = time.time()

    records = []
    # We include creating the File object as that is what the main index command does.
    # It helps gain an accurate estimate of the time it takes to run it.
    for root, dirs, files in os.walk(path):
        for directory in dirs:
            file = File(path=join(root, directory), is_directory=True)
            records.append(file)

        for name in files:
            file = File(path=join(root, name), is_directory=False, eager=True)
            records.append(file)

    end = time.time()

    db = FileDatabase(output_path)
    db.create_tables()

    connection = db.create_connection()
    cursor = connection.cursor()

    db.insert_files(cursor, records)

    cursor.close()
    connection.commit()
    connection.close()

    print('"walk-path {}" took {}'.format(path, end - start))


@debug_cli.command(
    help='Inserts files into a database. An input file path is required, must be output from walk_path command')
@click.argument('path', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path))
@click.option('-d', '--database', 'database_path',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to a database file. Must not exist. If not provided, one will be created named 'files.db'")
@click.option('-s', '--batch-size',
              type=click.INT,
              help="Commit changes to database when we've reached this many")
def insert(path, database_path, batch_size):
    if batch_size is None:
        batch_size = 500

    if batch_size < 0:
        batch_size = sys.maxsize

    if database_path is None:
        database_path = default_database_path()

    source_database = FileDatabase(path)
    source_connection = source_database.create_connection()
    source_cursor = source_connection.cursor()

    files = source_database.get_all_files(source_cursor)
    source_cursor.close()
    source_connection.close()

    start = time.time()

    db = FileDatabase(database_path)
    db.create_tables()
    connection = db.create_connection()
    cursor = connection.cursor()

    index = 0
    for i, file in enumerate(files):
        if index == batch_size:
            connection.commit()
            index = 0

        db.insert_file(cursor, file)

    connection.commit()
    cursor.close()
    connection.close()

    print('"insert {}" took {}'.format(path, time.time() - start))
