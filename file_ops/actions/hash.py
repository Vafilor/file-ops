import hashlib
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Generator, Sequence

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.orm import Session

from file_ops.database.database import Database
from file_ops.database.models import File, FileStatus
from file_ops.database.utils import change_files_status, update_files_to_status

logger = logging.getLogger(__name__)

class HashStats:
    def __init__(self) -> None:
        self.success = 0
        self.failed = 0
        self.expected_total = 0



class ThreadPoolExecutorWithQueueSizeLimit(ThreadPoolExecutor):
    def __init__(self, maxsize: int = 50, *args: Any, **kwargs: Any) -> None:
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=maxsize)  # type: ignore


class FileUpdater(threading.Thread):
    def __init__(self, database: Database, hashed_file_queue: queue.Queue[dict], stats: HashStats, max_size: int = 100):
        super().__init__()
        self.max_size = max_size
        self._updated_file_data: list[dict[str, Any]] = []
        self.hashed_file_queue = hashed_file_queue
        self.database = database
        self.max_tries = 3
        self.stats = stats

    def run(self) -> None:
        try:
            while True:
                item = self.hashed_file_queue.get()
                self.hashed_file_queue.task_done()

                if "error_message" in item:
                    self.stats.failed += 1
                else:
                    self.stats.success += 1

                self._updated_file_data.append(item)

                if len(self._updated_file_data) >= self.max_size:
                    self.flush()
        except queue.ShutDown:
            return

    def flush(self) -> None:
        if not len(self._updated_file_data):
            return
        
        tries = 0

        while tries < self.max_tries:
            tries += 1
            try:
                with self.database.get_session() as session:
                    session.execute(update(File), self._updated_file_data)
                    session.commit()
                    logger.info(f"Flushed changes for {len(self._updated_file_data)} files")
                    logger.info(f"{self.stats.success} / {self.stats.expected_total} processed with {self.stats.failed} errors")
                    self._updated_file_data = []
                    break
            except BaseException:
                logger.error(f"Unable to flush hash updates. Try {tries}/{self.max_size}", exc_info=True)
                if tries == self.max_tries:
                    raise


def _get_file_to_hash_conditions() -> list[ColumnElement]:
    return [
        File.status == FileStatus.BASIC,
        File.content_hash.is_(None),
        File.is_directory.is_(False),
    ]

def get_files_to_hash(session: Session, limit: int) -> Sequence[File]:
    conditions = _get_file_to_hash_conditions()

    try:
        query = (
            select(File)
            .where(*conditions)
            .limit(limit)
        )

        return session.scalars(query).all()
    except BaseException as be:
        print(str(be))
        logging.error("error", exc_info=True)
        return []
    
def count_files_to_hash(session: Session) -> int:
    conditions = _get_file_to_hash_conditions()

    query = (
        select(func.count(File.id))
        .where(*conditions)
    )

    count = session.scalar(query)

    return count if count else 0


def generate_files_to_hash(
    database: Database, batch_size: int
) -> Generator[Sequence[File], None, None]:
    while True:
        with database.get_session() as session:
            files = get_files_to_hash(session=session, limit=batch_size)
            if not len(files):
                break

        yield files


def hash_file_path(path: Path) -> str:
    with open(path, "rb") as f:
        digest = hashlib.file_digest(f, "sha256")

    return digest.hexdigest()


def hash_file(file: File, output: queue.Queue[dict]) -> None:
    try:
        digest = hash_file_path(Path(file.path))
        output.put({
            "id": file.id,
            "content_hash": digest,
            "status": FileStatus.BASIC,
        })
    except BaseException as be:
        logging.error(f"Unable to get hash of file {file.path}.", exc_info=True)
        output.put({
            "id": file.id,
            "status": FileStatus.BASIC,
            "error_message": str(be),
        })


def hash_files(
    database: Database, max_workers: int = 5, batch_size_get: int = 500, batch_size_update: int = 100, log_level: str = "info"
) -> None:
    logger.disabled = False
    logger.setLevel(logging.getLevelNamesMapping()[log_level.upper()])

    with database.get_session() as session:
        change_files_status(
            session=session,
            from_status=FileStatus.HASHING,
            to_status=FileStatus.BASIC,
        )
        session.commit()

    stats = HashStats()
    with database.get_session() as session:
        stats.expected_total = count_files_to_hash(session=session)

    hashed_file_queue = queue.Queue[dict](maxsize=100)

    updater = FileUpdater(database=database, hashed_file_queue=hashed_file_queue, stats=stats, max_size=batch_size_update)
    updater.start()

    with ThreadPoolExecutorWithQueueSizeLimit(maxsize=100, max_workers=max_workers) as executor:
        for files in generate_files_to_hash(
            database=database, batch_size=batch_size_get
        ):
            file_ids = [file.id for file in files]
            with database.get_session() as session:
                update_files_to_status(
                    session=session, ids=file_ids, status=FileStatus.HASHING
                )
                session.commit()

            for file in files:
                executor.submit(hash_file, file, hashed_file_queue)


    hashed_file_queue.join()
    hashed_file_queue.shutdown()
    updater.join()
    updater.flush()
