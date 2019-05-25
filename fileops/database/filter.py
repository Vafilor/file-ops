from multiprocessing import Queue
from typing import AnyStr, Sequence

from ..files.file import File
from ..pipe.operator import Operator, TerminateOperand

from .database import FileDatabase
from .file import File as DatabaseFile


class FileFilter(Operator):
    """
    Filters out files that are already in the the database.
    Files are considered equal if they have the same path and modified_at timestamp.
    """
    def __init__(self, database_path: AnyStr = None, chunk_size: int = 999):
        """
        chunk_size:
            how many files to check from the database at a time. This should be <= 999
            as SQLite does not like more parameters in the query than that.
            See: https://sqlite.org/limits.html Maximum Number Of Host Parameters In A Single SQL Statement

        """
        Operator.__init__(self)
        self.database_path = database_path
        self.chunk_size = chunk_size

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        database = FileDatabase(self.database_path)

        pending_files = []
        file = input_queue.get()
        while not isinstance(file, TerminateOperand):
            pending_files.append(file)

            if len(pending_files) >= self.chunk_size:
                filtered_files = self.filter_files(database, pending_files)
                pending_files = []
                for filtered_file in filtered_files:
                    output_queue.put(filtered_file)

            file = input_queue.get()

        filtered_files = self.filter_files(database, pending_files)
        for filtered_file in filtered_files:
            output_queue.put(filtered_file)

        input_queue.put(file)

        database.close()

    def filter_files(self, database: FileDatabase, files: Sequence[File]) -> Sequence[DatabaseFile]:
        if len(files) == 0:
            return []

        existing = database.get_files_by_paths([file.path for file in files], ['id', 'path', 'modified_at'])
        file_map = {file['path']: file for file in existing}

        filtered = []

        for file in files:
            new_file = True
            if file.path in file_map:
                new_file = False
                modifications_equal = file.modified_at == file_map[file.path]['modified_at']

                if modifications_equal:
                    continue

            key = None
            if not new_file:
                key = file_map[file.path]['id']

            db_file = DatabaseFile.from_file(file, key)
            filtered.append(db_file)

        return filtered
