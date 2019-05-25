import sys
import os
import os.path

import time

from fileops.files.producer import FileProducer, PrintFileProgress
from fileops.database.database import FileDatabase
from fileops.database.producer import HashFileProducer, NotDeletedFileProducer as DatabaseNotDeletedFileProducer
from fileops.database.recorder import HashFileRecorder, DeleteFileAction
from fileops.database.filter import FileFilter
from fileops.database.recorder import FileStatsRecorder
from fileops.files.filter import DeletedFileFilter
from fileops.pipe.pipe import Pipe, ProcessPipe
from fileops.files.printer import Printer
from fileops.files.hasher import Hasher, PrintHasherProgress


class FileFormatter:
    # We place this here so it can be pickled in multiprocessing
    def __init__(self, prefix=''):
        self.prefix = prefix

    def format(self, item):
        return f'{self.prefix} {item.path}'


def create_file_indexer(path, output_file) -> Pipe:
    file_indexer = ProcessPipe() \
        .pipe(FileProducer(path, PrintFileProgress())) \
        .pipe(FileFilter(output_file)) \
        .pipe(FileStatsRecorder(output_file)) \

    return file_indexer


def create_file_hasher(output_file) -> Pipe:
    file_hasher = ProcessPipe() \
            .pipe(HashFileProducer(output_file)) \
            .pipe(Hasher(progress=PrintHasherProgress())) \
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


def index_files(*args):
    if len(args) == 0 or '--help' in args:
        print('Usage: index path [database_file]')
        return

    path = args[0]

    output_file_name = 'files.db'
    output_path = os.path.abspath(os.path.join(os.getcwd(), output_file_name))
    if len(args) > 1:
        output_path = os.path.abspath(os.path.expandvars(args[1]))

    db = FileDatabase(output_path)
    db.create_file_table()
    db.close()

    print(f'path:{path}')
    print(f'database file:{output_path}')

    file_indexer = create_file_indexer(path, output_path)
    file_indexer.run()


def hash_files(*args):
    if '--help' in args:
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


def cleanup_files(*args):
    if '--help' in args:
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


def folder_stats_files(*args):
    if '--help' in args:
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
    db.create_file_table()
    db.calculate_directory_sizes()
    db.close()


def main():
    if len(sys.argv) < 2:
        print('Usage: command [options]')
        print('commands: index, hash, cleanup, folder-stats')
        print('          use --time to time the program.')
        return

    command = sys.argv[1]

    if command not in ['index', 'hash', 'cleanup', 'folder-stats']:
        print(f'Unknown command: {command}')
        return

    args = sys.argv[2:]

    start_time = time.time()

    if command == 'index':
        index_files(*args)
    elif command == 'hash':
        hash_files(*args)
    elif command == 'cleanup':
        cleanup_files(*args)
    elif command == 'folder-stats':
        folder_stats_files(*args)

    if '--time' in sys.argv:
        print('Total Time:{}'.format(time.time() - start_time))


if __name__ == '__main__':
    main()
