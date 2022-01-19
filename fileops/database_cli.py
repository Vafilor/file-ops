import os
import pathlib
from typing import Optional

import click

from fileops.cli_common import default_database_path
from fileops.database.database import FileDatabase
from fileops.files.file import humanize_file_size


@click.group(name="database", help="Interact with a SQLite database")
def database_cli():
    pass


@database_cli.command(help="Prints statistics about the provided database")
@click.option('-d', '--database', type=click.Path(exists=False, path_type=pathlib.Path))
def stats(database: Optional[pathlib.Path]):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    stats = db.statistics()

    stat = os.stat(database)

    click.echo(f"File size {humanize_file_size(stat.st_size)}")
    click.echo(f"Total records {stats.total_records}")
    click.echo(f"Files {stats.files}")
    click.echo(f"Files hashed {stats.files_hashed}")
    click.echo(f"Directories {stats.directories}")
    click.echo(f"Total size {humanize_file_size(stats.total_size)}")