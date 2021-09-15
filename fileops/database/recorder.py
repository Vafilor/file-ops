import sqlite3
from multiprocessing.queues import Queue

from ..pipe.operator import TerminateOperand, Operator
from typing import Union, Optional

from ..files.file import File
from .file import File as DatabaseFile
from .database import FileDatabase
import datetime


class BufferedFileRecorder(Operator):
    """
    Given a queue of hashed files, records them in a database a chunk at a time.

    We use a chunk to avoid doing the operation on a single file at a time, so it should be more efficient.
    """

    def __init__(self, database: FileDatabase,
                 chunk_size: int = 500,
                 report_file_count=100000):
        """
        database_path:
            path of the database.

        chunk_size:
            maximum number of records to update at a time.
        """
        Operator.__init__(self)
        self.database = database
        self.chunk_size = chunk_size
        self.report_file_count = report_file_count

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        connection = self.database.create_connection()
        cursor = connection.cursor()

        chunk = []
        count = 0
        total_count = 0

        for file in iter(input_queue.get, TerminateOperand()):
            count += 1
            correct_input, message = self.is_correct_input(file)
            if not correct_input:
                print(message)
                continue

            if self.process_item(file):
                chunk.append(file)

            if len(chunk) >= self.chunk_size:
                self.process_chunk(self.database, cursor, chunk)
                for chunklet in chunk:
                    output_queue.put(chunklet)

                chunk = []

                if count >= self.report_file_count:
                    total_count += count
                    print(f'{datetime.datetime.now()} processed {total_count} files for recorder')
                    count = 0

        self.process_chunk(self.database, cursor, chunk)
        for chunklet in chunk:
            output_queue.put(chunklet)

        cursor.close()
        connection.close()

    def is_correct_input(self, item):
        """
        Returns true, '' if the item is a File.
        If it is not correct, return False, along with a message to display.
        """
        if isinstance(item, File):
            return True, ''

        return False, f'{item} is not a File'

    def process_item(self, item):
        """
        Does any logic needed to the incoming item from the queue once it is obtained.

        Return True if successful, False otherwise.

        If False is returned, it won't be added to the chunk to process.
        """
        return True

    def process_chunk(self, database: FileDatabase, cursor: sqlite3.Cursor, chunk):
        """Override this method to process the chunk of data"""
        raise NotImplementedError()


class FileStatsRecorder(BufferedFileRecorder):
    def is_correct_input(self, item):
        if isinstance(item, DatabaseFile):
            return True, ''

        return False, f'{item} is not a DatabaseFile'

    def process_item(self, item):
        try:
            item.load()
            return True
        except OSError as ex:
            print(f'Exception getting file stats for {item.path}. Exception {ex}')

    def process_chunk(self, database: FileDatabase, cursor: sqlite3.Cursor, chunk):
        database.delete_by_ids(cursor, [file for file in chunk if file.key is not None])
        database.insert_files(cursor, chunk)

        cursor.connection.commit()


class FileInserter(BufferedFileRecorder):
    def is_correct_input(self, item):
        if isinstance(item, File):
            return True, ''

        return False, f'{item} is not a File'

    def process_item(self, item):
        try:
            item.load()
            return True
        except OSError as ex:
            print(f'Exception getting file stats for {item.path}. Exception {ex}')

    def process_chunk(self, database: FileDatabase, cursor: sqlite3.Cursor, chunk):
        database.insert_files(cursor, chunk)
        cursor.connection.commit()

class HashFileRecorder(BufferedFileRecorder):
    """
    Updates the file hashes into the database.
    """
    def process_chunk(self, database: FileDatabase, cursor: sqlite3.Cursor, chunk):
        database.update_file_hashes(cursor, chunk)
        cursor.connection.commit()


# TODO rename this and one below?
class DeleteFileRecorder(BufferedFileRecorder):
    """
    Marks files deleted in a database.
    """

    def process_chunk(self, database, chunk):
        database.mark_deleted(chunk)


class DeleteFileAction(BufferedFileRecorder):
    """
    Deletes files from database.
    """

    def process_chunk(self, database, chunk):
        database.delete(chunk)
