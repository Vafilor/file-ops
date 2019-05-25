import os
import os.path
import sqlite3
import datetime
from typing import Union, Sequence, Optional

from ..files.file import File as File
from .file import File as DatabaseFile


class FileDatabase:
    def __init__(self, path: Optional[Union[str, bytes]] = None):
        """
        path:
            if path is None, it is set to the current working directory / files.db
        """
        if path is None:
            path = os.path.join(os.getcwd(), 'files.db')

        self.connection = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.row_factory = sqlite3.Row

    def close(self):
        self.connection.close()

    @property
    def now(self):
        """Gets the current time in utc."""
        return datetime.datetime.utcnow()

    def create_file_table(self):
        """
        Creates the files table.
        """
        c = self.connection.cursor()

        sql = """
            CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                record_created_at timestamp,
                updated_at timestamp,
                path TEXT,
                modified_at timestamp,
                deleted_at timestamp,
                size INTEGER,
                hash TEXT,
                hashed_at timestamp,
                is_directory INTEGER
            );
        """

        c.execute(sql)

        sql = """
            CREATE INDEX path_index
            ON files(path);
        """

        try:
            c.execute(sql)
        except sqlite3.OperationalError:
            pass  # It's okay, we already have the index.

        self.connection.commit()

        c.close()

    def insert_files(self, files: Union[Sequence[File], File]):
        """
        Inserts files into the databse with the following fields:
            * path
            * record_created_at
            * updated_at
            * modified_at
            * deleted_at
            * size
            * is_directory

        hash is not inserted.
        """
        if files is None or len(files) == 0:
            return

        if not isinstance(files, list):
            files = [files]

        c = self.connection.cursor()

        now = self.now

        for file in files:
            c.execute("INSERT INTO files"
                      "(path, record_created_at, updated_at, modified_at, deleted_at, size, is_directory) "
                      "VALUES(?, ?, ?, ?, ?, ?, ?);",
                      (file.path, now, now, file.modified_at, file.deleted_at,
                       file.size, file.is_directory))

        self.connection.commit()
        c.close()

    def update_files_basic(self, files: Union[Sequence[File], File]):
        """
        Updates files'
         * modified_at
         * size
         * is_directory
         * updated_at
        """
        if files is None or len(files) == 0:
            return

        if not isinstance(files, list):
            files = [files]

        now = self.now

        c = self.connection.cursor()

        # Skip updated at, because they are all set to the same value
        columns = ['modified_at', 'size', 'is_directory']
        column_cases = {column: '' for column in columns}

        for file in files:
            formatted_file_path = file.path.replace("'", "''")

            if file.modified_at is None:
                column_cases['modified_at'] += " WHEN '{}' THEN null".format(formatted_file_path)
            else:
                column_cases['modified_at'] += " WHEN '{}' THEN '{}'".format(formatted_file_path, file.modified_at)

            if file.size is None:
                column_cases['size'] += " WHEN '{}' THEN null".format(formatted_file_path)
            else:
                column_cases['size'] += " WHEN '{}' THEN {}".format(formatted_file_path, file.size)

            column_cases['is_directory'] += " WHEN '{}' THEN {}".format(formatted_file_path, int(file.is_directory))

        query = "UPDATE files SET "
        for column in columns:
            query += " {} = CASE {} {} ELSE {} END,".format(column, 'path', column_cases[column], column)

        query += " updated_at = '{}'".format(now)
        query += " WHERE path IN ({0})".format(','.join('?' for _ in files))

        c.execute(query, [file.path for file in files])

        self.connection.commit()
        c.close()

    def update_file_hashes(self, files: Union[Sequence[File], File]):
        """
        Updates the files' hashes.
        """
        if files is None or len(files) == 0:
            return

        if not isinstance(files, list):
            files = [files]

        c = self.connection.cursor()

        query = "UPDATE files SET hashed_at = '{0}', updated_at = '{0}', hash = CASE path".format(self.now)

        for file in files:
            query += " WHEN '{}' THEN '{}'".format(file.path.replace("'", "''"), file.content_hash)

        query += " ELSE NULL END"
        query += " WHERE path IN ({0})".format(','.join('?' for _ in files))

        c.execute(query, [file.path for file in files])

        self.connection.commit()
        c.close()

    def mark_deleted(self, files: Union[Sequence[File], File]):
        """
        Sets the files' deleted_at to now.
        """
        if files is None or len(files) == 0:
            return

        if not isinstance(files, list):
            files = [files]

        c = self.connection.cursor()

        query = "UPDATE files SET deleted_at = '{}'".format(self.now)
        query += " WHERE path IN ({0})".format(','.join('?' for _ in files))

        c.execute(query, [file.path for file in files])

        self.connection.commit()
        c.close()

    def delete(self, files: Union[Sequence[File], File]):
        """
        Deletes the files.
        """
        if files is None or len(files) == 0:
            return

        if not isinstance(files, list):
            files = [files]

        c = self.connection.cursor()

        query = "DELETE FROM files WHERE path IN ({0})".format(','.join('?' for _ in files))

        c.execute(query, [file.path for file in files])

        self.connection.commit()
        c.close()

    def get_files_by_paths(self, paths: Sequence[Union[str, bytes]], columns: Optional[Sequence[str]] = None):
        """
        Loads files from the database for the given paths.

        columns:
            If not specified, all columns are loaded.
            Otherwise just the columns input.

        """
        if not isinstance(paths, list):
            paths = [paths]

        if columns is not None and not isinstance(columns, list):
            columns = [columns]

        c = self.connection.cursor()
        c.arraysize = len(paths)

        if columns is None:
            c.execute("SELECT * FROM files WHERE path IN ({0})".format(','.join('?' for _ in paths)), paths)
        else:
            select = "SELECT {} FROM files".format(','.join(columns))
            c.execute("{0} WHERE path IN ({1})".format(select, ','.join('?' for _ in paths)), paths)

        results = c.fetchall()

        c.close()

        return results

    def update_or_insert(self, files: Union[Sequence[DatabaseFile], DatabaseFile]):
        """
        Updates the files if they have a key value set, otherwise inserts them into the database.
        """

        if not isinstance(files, list):
            files = [files]

        if len(files) == 0:
            return

        updates = []
        inserts = []

        for file in files:
            if file.key is not None:
                updates.append(file)
            else:
                inserts.append(file)

        self.insert_files(inserts)
        self.update_files_basic(updates)

    def get_files_to_hash(self, start_id: int = 0, limit: int = 500):
        """
        Returns a list of filepaths for files that need their hash calculated.

        Files need their hash calculated if
         * the hash is null
         * the hash is the empty string
         * it was modified since it was hashed.
        """
        c = self.connection.cursor()

        c.execute("""
            SELECT id, path 
            FROM files 
            WHERE is_directory = 0 AND
                  (
                    hash IS NULL OR 
                    hash = '' OR 
                    hashed_at < modified_at
                  ) AND
                  id > {}
                  ORDER BY id
                  LIMIT {}""".format(start_id, limit))
        results = c.fetchall()

        c.close()

        return [DatabaseFile(key=result['id'], path=result['path']) for result in results]

    def get_files_not_deleted(self, start_id: int = 0, limit: int = 500):
        """
        Returns a list of file/directory paths for files that do not have deleted_at set.
        """
        c = self.connection.cursor()

        c.execute("""
            SELECT id, path 
            FROM files 
            WHERE id > {} AND 
                  deleted_at IS NULL
                  ORDER BY id
                  LIMIT {}""".format(start_id, limit))
        results = c.fetchall()

        c.close()

        return [DatabaseFile(key=result['id'], path=result['path']) for result in results]

    def get_directories(self, start_id: int = 0, columns: Optional[Sequence[str]] = None, limit: int = 500):
        """
        Returns a list of directories that need to have their size updated.


        Directories need to have their size updated if
         * size is not set (null)
         * it has been modified since it was last updated
        """
        if columns is not None and not isinstance(columns, list):
            columns = [columns]

        c = self.connection.cursor()

        select = "SELECT * FROM files"
        if columns is not None:
            select = "SELECT {} FROM files".format(','.join(columns))

        query = select + """ 
            WHERE id > {} AND 
                  is_directory = 1 AND
                  (size is NULL OR updated_at < modified_at) 
            ORDER BY id LIMIT {}""".format(start_id, limit)
        c.execute(query)

        results = c.fetchall()

        c.close()

        return results

    def update_directory_sizes(self):
        """
        Calculates and updates the directory sizes for all directories in the database.

        Not the fastest thing in the world, be ready for a wait.
        """
        c = self.connection.cursor()

        c.execute("""
            UPDATE files
            SET size = COALESCE(
                (SELECT sum(f2.size)
                 FROM files AS f2
                 WHERE f2.path LIKE files.path || '%' AND f2.is_directory = 0), 0)
            WHERE files.is_directory = 1 and size is null;
            """)

        self.connection.commit()
        c.close()

