from multiprocessing import Queue
from ..pipe.operator import Operator
from typing import Union, Optional, Sequence

from .database import FileDatabase


class BufferedFileProducer(Operator):
    def __init__(self, database_path: Union[bytes, str] = None, chunk_size: int = 500, limit: Optional[int] = None):
        """
       Gets files from the database, override to determine which files.
       Each record is assumed to have an id. The id is used as a tracker so we don't get the same items.

       database_path:
           path of the database

       chunk_size:
           how many file records should be taken from the database at a time.

       limit:
           maximum number of files to process.
       """
        Operator.__init__(self)
        self.last_id = 0  # keeps track of the last id in the query
        self.database_path = database_path
        self.chunk_size = chunk_size
        self.limit = limit

    def process(self, input_queue: Queue, output_queue: Queue) -> None:
        database = FileDatabase(self.database_path)

        total_provided = 0

        while True:
            if self.limit is not None and total_provided >= self.limit:
                break

            items = self.get_items(database, self.last_id, self.chunk_size)
            if len(items) == 0:
                break

            self.last_id = items[-1].key

            for item in items:
                output_queue.put(item)

            total_provided += len(items)

        database.close()

    def get_items(self, database, last_id, chunk_size) -> Sequence:
        """
        Get items from the database.

        database:
            database object.

        last_id
            The last id of the record retrieved.

        chunk_size:
            Maximum number of items that should be retrieved.
        """
        raise NotImplementedError()


class HashFileProducer(BufferedFileProducer):
    """
    Gets files that need to be hashed.
    """
    def get_items(self, database, last_id, chunk_size) -> Sequence:
        return database.get_files_to_hash(last_id, chunk_size)


class NotDeletedFileProducer(BufferedFileProducer):
    """
    Gets files that have not been deleted from a database.
    Not deleted means they do not have deleted_at set.
    """
    def get_items(self, database, last_id, chunk_size) -> Sequence:
        return database.get_files_not_deleted(last_id, chunk_size)




