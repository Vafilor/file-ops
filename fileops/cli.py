import pathlib
import os
import os.path

import time
import datetime

import click

from typing import Union, Optional

from fileops.files.file import humanize_file_size
from fileops.files.producer import FileProducer
from fileops.database.database import FileDatabase
from fileops.database.producer import HashFileProducer, NotDeletedFileProducer as DatabaseNotDeletedFileProducer
from fileops.database.recorder import HashFileRecorder, DeleteFileAction, FileInserter
from fileops.database.filter import FileFilter
from fileops.database.recorder import FileStatsRecorder
from fileops.files.filter import DeletedFileFilter
from fileops.pipe.pipe import Pipe, ProcessPipe
from fileops.files.printer import Printer
from fileops.files.buffered_printer import BufferedPrinter
from fileops.files.hasher import Hasher, PrintHasherProgress
from fileops.files.counter import Counter

from cProfile import Profile
from pstats import Stats

class PrefixFormatter:
    # We place this here so it can be pickled in multiprocessing
    def __init__(self, prefix='', with_timestamp=True):
        self.prefix = prefix
        self.with_timestamp = with_timestamp

    def format(self, item):
        if self.with_timestamp:
            now = datetime.datetime.now()
            return f'{now} {self.prefix} {item}'
        else:
            return f'{self.prefix} {item}'


class FileFormatter:
    # We place this here so it can be pickled in multiprocessing
    def __init__(self, prefix=''):
        self.prefix = prefix

    def format(self, item):
        return f'{self.prefix} {item.path}'


def default_database_path():
    return pathlib.Path(os.getcwd(), 'files.db')


def create_file_indexer(path: pathlib.Path, database: FileDatabase) -> Pipe:
    if database.file_count() == 0:
        # If database doesn't exist, we don't need to filter any files.
        file_indexer = ProcessPipe() \
            .pipe(FileProducer(path)) \
            .pipe(FileInserter(database))
    else:
        file_indexer = ProcessPipe() \
            .pipe(FileProducer(path)) \
            .pipe(FileFilter(database)) \
            .pipe(FileStatsRecorder(database))

    return file_indexer


def create_file_hasher(database: FileDatabase) -> Pipe:
    file_hasher = ProcessPipe() \
            .pipe(HashFileProducer(database)) \
            .pipe(Hasher()) \
            .pipe(HashFileRecorder(database)) \

    return file_hasher


def create_file_deleter(output_file) -> Pipe:
    file_deleter = ProcessPipe() \
            .pipe(DatabaseNotDeletedFileProducer(output_file)) \
            .pipe(DeletedFileFilter()) \
            .pipe(DeleteFileAction(output_file)) \

    return file_deleter


@click.group()
def cli():
    pass


@cli.command(help="Index files under a path. This does not calculate the hash.")
@click.argument('path', type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
@click.option('-d', '--database',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to an existing database file. If not provided, one will be created named 'file.db' ")
@click.option('-t', '--time', 'time_command', is_flag=True, help="Time the command")
def index(path: pathlib.Path, database: Optional[pathlib.Path], time_command: bool):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    db.create_tables()

    file_indexer = create_file_indexer(path, db)

    start = time.time()

    file_indexer.run()

    if time_command:
        print('"index {}" took {}'.format(path, time.time() - start))

@cli.command('hash', help="Calculates a hash for all of the files in the provided database and updates the record. This is a slow process.")
@click.option('-d', '--database',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to an existing database file. If not provided, one will be created named 'file.db' ")
@click.option('-t', '--time', 'time_command', is_flag=True, help="Time the command")
def hash_files(database: Optional[pathlib.Path], time_command: bool):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    db.create_tables()

    hasher = create_file_hasher(db)

    start = time.time()

    hasher.run()

    if time_command:
        print('"hash {}" took {}'.format(database, time.time() - start))


@cli.command('folder-stats', help="Runs a SQL query to calculate the size of each folder in the database.")
@click.option('-d', '--database',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to an existing database file. If not provided, one will be created named 'file.db' ")
@click.option('-t', '--time', 'time_command', is_flag=True, help="Time the command")
def folder_stats_files(database: Optional[pathlib.Path], time_command: bool):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    db.create_tables()

    start = time.time()

    connection = db.create_connection()
    cursor = connection.cursor()

    try:
        db.update_directory_sizes(cursor)
        connection.commit()
    except Exception as e:
        cursor.close()
        connection.close()
        print(e)
        return

    cursor.close()
    connection.close()

    if time_command:
        print('"hash {}" took {}'.format(database, time.time() - start))

# TODO - test to make sure this works across bucket boundaries
@cli.command('map-duplicates', help="Finds duplicate files by hash and stores the record in the database. Use list-duplicates to output these.")
@click.option('-d', '--database',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to an existing database file. If not provided, one will be created named 'file.db' ")
@click.option('-t', '--time', 'time_command', is_flag=True, help="Time the command")
def map_duplicates(database: Optional[pathlib.Path], time_command: bool):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    db.create_tables()

    connection = db.create_connection()
    cursor = connection.cursor()

    start = time.time()

    index = 0
    buckets = []
    total_bucket_size = 0
    while True:
        files = db.get_files(cursor, 'hash ASC', 1000, index)
        if not files:
            break

        index += len(files)

        bucket = []
        for file in files:
            if file.is_directory:
                continue

            if not bucket:
                bucket.append(file)
            elif file.content_hash == bucket[-1].content_hash:
                bucket.append(file)
            else:
                buckets.append(bucket)
                bucket = [file]

            total_bucket_size += 1

        if total_bucket_size > 500:
            for bucket in buckets:
                db.insert_same_hash(cursor, bucket)

            connection.commit()
            buckets = []
            total_bucket_size = 0

    for bucket in buckets:
        db.insert_same_hash(cursor, bucket)

    connection.commit()

    cursor.close()
    connection.close()

    if time_command:
        click.echo('"hash {}" took {}'.format(database, time.time() - start))

@cli.command('list-duplicates', help="Outputs the duplicated files. Use -o to set an output file [recommended]. Make sure to run map-duplicates first.")
@click.option('-d', '--database',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to an existing database file. If not provided, one will be created named 'file.db' ")
@click.option('-o', '--output',
              type=click.Path(exists=False, dir_okay=False, path_type=pathlib.Path),
              help="Path to a file to store output. If not provided, stdout is used.")
@click.option('-t', '--time', 'time_command', is_flag=True, help="Time the command")
def list_duplicates(database: Optional[pathlib.Path], output: Optional[pathlib.Path], time_command: bool):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    db.create_tables()

    connection = db.create_connection()
    cursor = connection.cursor()

    printer = click.echo
    output_file = None
    if output is not None:
        output_file = open(output, 'w')
        printer = output_file.write

    start = time.time()

    total_duplicated = 0

    def print_link_information(links):
        nonlocal total_duplicated
        link = links[0]
        size = humanize_file_size(link.file1.size)
        duplicated_amount = link.file1.size * len(links)
        duplicated_size = humanize_file_size(duplicated_amount)
        total_duplicated += duplicated_amount

        printer(f"File Size: {size}\tDuplicated: {duplicated_size}")

        printer(f"  {link.file1.path}")
        for link in links:
            printer(f"  {link.file2.path}")

        printer()

    bucket = []
    previous_id = None
    chunk_size = 1000
    offset = 0
    while True:
        links = db.get_file_links(
            cursor,
            skip_identical=True,
            skip_empty=True,
            order="f1.size DESC, f1.id ASC, f2.id ASC",
            limit=chunk_size,
            offset=offset)

        if not links:
            break

        for link in links:
            new_id = link.file_1_id

            if previous_id is None:
                previous_id = new_id
                bucket.append(link)
            elif previous_id == new_id:
                bucket.append(link)
            else:
                previous_id = new_id
                print_link_information(bucket)
                bucket = [link]

        offset += chunk_size

    if bucket:
        print_link_information(bucket)

    printer(f'Total duplicated: {humanize_file_size(total_duplicated)}')

    cursor.close()
    connection.close()

    if output_file is not None:
        output_file.close()

    if time_command:
        click.echo('"hash {}" took {}'.format(database, time.time() - start))


@click.group(name="database")
def database_cli():
    pass

@database_cli.command()
@click.option('-d', '--database', type=click.Path(exists=False, path_type=pathlib.Path))
def stats(database: Optional[pathlib.Path]):
    if database is None:
        database = default_database_path()

    db = FileDatabase(database)
    stats = db.statistics()

    click.echo(f"Total records {stats.total_records}")
    click.echo(f"Files {stats.files}")
    click.echo(f"Files hashed {stats.files_hashed}")
    click.echo(f"Directories {stats.directories}")
    click.echo(f"Total size {humanize_file_size(stats.total_size)}")


if __name__ == '__main__':
    cli.add_command(database_cli)
    cli()