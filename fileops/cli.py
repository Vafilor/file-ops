import sys
import os
import os.path

import time
import datetime

from fileops.files.producer import FileProducer
from fileops.database.database import FileDatabase
from fileops.database.producer import HashFileProducer, NotDeletedFileProducer as DatabaseNotDeletedFileProducer
from fileops.database.recorder import HashFileRecorder, DeleteFileAction
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


def create_file_indexer(path, output_file) -> Pipe:
    file_indexer = ProcessPipe() \
        .pipe(FileProducer(path)) \
        .pipe(FileFilter(output_file)) \
        .pipe(FileStatsRecorder(output_file))

    return file_indexer


def create_file_hasher(output_file) -> Pipe:
    file_hasher = ProcessPipe() \
            .pipe(HashFileProducer(output_file)) \
            .pipe(Hasher()) \
            .pipe(HashFileRecorder(output_file)) \

    return file_hasher


def create_file_deleter(output_file) -> Pipe:
    file_deleter = ProcessPipe() \
            .pipe(DatabaseNotDeletedFileProducer(output_file)) \
            .pipe(Printer(FileFormatter('Checking'))) \
            .pipe(DeletedFileFilter()) \
            .pipe(Printer(FileFormatter('Deleting from database'))) \
            .pipe(DeleteFileAction(output_file)) \

    return file_deleter


def index_files(args, opts):
    if len(args) == 0 or 'help' in opts:
        print('Usage: index path [database_file]')
        return

    path = args[0]

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 1:
        output_path = os.path.abspath(os.path.expandvars(args[1]))

    db = FileDatabase(output_path)
    db.create_tables()
    db.close()

    print(f'path:{path}')
    print(f'database file:{output_path}')

    file_indexer = create_file_indexer(path, output_path)
    file_indexer.run()


def hash_files(args, opts):
    if 'help' in opts:
        print('Usage: hash [database_file]')
        return

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 0:
        output_path = os.path.abspath(os.path.expandvars(args[0]))

    if not os.path.exists(output_path):
        print(f'{output_path} does not exist. Nothing to hash.')
        return

    print(f'Hashing files in {output_path}')

    hasher = create_file_hasher(output_path)
    hasher.run()


def cleanup_files(args, opts):
    if 'help' in opts:
        print('Usage: cleanup [database_file]')
        return

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 0:
        output_path = os.path.abspath(os.path.expandvars(args[0]))

    if not os.path.exists(output_path):
        print(f'{output_path} does not exist. Nothing to cleanup.')
        return

    print(f'Cleaning files in {output_path}')

    deleter = create_file_deleter(output_path)
    deleter.run()


def folder_stats_files(args, opts):
    if 'help' in opts:
        print('Usage: folder-stats [database_file]')
        return

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 0:
        output_path = os.path.abspath(os.path.expandvars(args[0]))

    if not os.path.exists(output_path):
        print(f'{output_path} does not exist. No folders to calculate stats on.')
        return

    db = FileDatabase(output_path)
    db.create_tables()
    db.update_directory_sizes()
    db.close()

def map_duplicates(args, opts):
    if 'help' in opts:
        print('Usage: map-duplicates [database_file]')
        return

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 0:
        output_path = os.path.abspath(os.path.expandvars(args[0]))

    if not os.path.exists(output_path):
        print(f'{output_path} does not exist. No folders to find duplicates in.')
        return

    db = FileDatabase(output_path)
    db.create_tables()

    index = 0
    buckets = []
    total_bucket_size = 0
    while True:
        files = db.get_files('hash ASC', 1000, index)
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
                db.insert_same_hash(bucket)

            buckets = []
            total_bucket_size = 0


    db.close()

def list_duplicates(args, opts):
    if 'help' in opts:
        print('Usage: list-duplicates [database_file]')
        return


def main():
    if len(sys.argv) < 2:
        print('Usage: command [options]')
        print('commands: index, hash, cleanup, folder-stats, map-duplicates, list-duplicates')
        print('          use --time to time the program.')
        return

    command = sys.argv[1]

    if command not in ['index', 'hash', 'cleanup', 'folder-stats', 'map-duplicates', 'list-duplicates']:
        print(f'Unknown command: {command}')
        return

    args = []
    opts = []
    for arg in sys.argv[2:]:
        if str.startswith(arg, '--'):
            opts.append(arg[2:])  # remove the leading --
        else:
            args.append(arg)

    start_time = time.time()

    if command == 'index':
        index_files(args, opts)
    elif command == 'hash':
        hash_files(args, opts)
    elif command == 'cleanup':
        cleanup_files(args, opts)
    elif command == 'folder-stats':
        folder_stats_files(args, opts)
    elif command == 'map-duplicates':
        map_duplicates(args, opts)
    elif command == 'list-duplicates':
        list_duplicates(args, opts)

    if 'time' in opts:
        print('Total Time:{}'.format(time.time() - start_time))


if __name__ == '__main__':
    main()
