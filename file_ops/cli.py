from pathlib import Path

import click

from file_ops.actions.hash import hash_files
from file_ops.actions.index import index_directory
from file_ops.actions.stats import calculate_stats
from file_ops.actions.unique_names import (
    execute_unqiue_name_change,
    generate_unique_name_changes,
)
from file_ops.actions.watch import watch
from file_ops.database.database import Database


@click.group()
@click.version_option("0.1.0", prog_name="file-ops")
def cli() -> None:
    pass

@cli.command("uqname")
@click.argument("path", type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True))
@click.option("--dry-run", type=bool, is_flag=True)
def unique_name_files_in_path(path: str, dry_run: bool) -> None:
    """Goes through all of the files in the path and gives them unique names that are uuids.
    Files that have the same name but different extension will be preserved in the new naming scheme.
    """
    for change in generate_unique_name_changes(Path(path)):
        print(f"{change.original_path} -> {change.new_path}")
        if not dry_run:
            execute_unqiue_name_change(change)
        

@cli.command("index")
@click.argument("path", type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True))
def index_path(path: str) -> None:
    database = Database()
    click.echo(f"Indexing {path}")
    index_directory(path=Path(path), database=database)


@cli.command("hash")
@click.option("--max-workers", type=int, default=5)
def hash_database_files(max_workers: int) -> None:
    database = Database()
    hash_files(database=database, max_workers=max_workers)

@cli.command("stats")
def calculate_stats_for_database_files() -> None:
    database = Database()
    calculate_stats(database=database)

@cli.command("watch")
@click.option("--path", type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True))
def watch_files(path: str|None) -> None:
    if path is None:
        path = str(Path.cwd())

    click.echo(f"Watching path {path}")

    database = Database()
    watch(path=path, database=database)

if __name__ == "__main__":
    cli()
