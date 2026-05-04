import hashlib
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import update

from file_ops.database.database import Database
from file_ops.database.file_service import FileError, FileService
from file_ops.database.models import File, FileStatus
from file_ops.logging_config import verbosity_extra

logger = logging.getLogger(__name__)


handlers = logger.handlers


class HashStats:
    def __init__(self) -> None:
        self.success = 0
        self.failed = 0
        self.expected_total = 0


class FileUpdater(threading.Thread):
    def __init__(
        self,
        database: Database,
        hashed_file_queue: queue.Queue[dict],
        stats: HashStats,
        max_size: int = 100,
    ):
        super().__init__()
        self.max_size = max_size
        self._updated_file_data: list[dict[str, Any]] = []
        self.hashed_file_queue = hashed_file_queue
        self.database = database
        self.max_tries = 5
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
                    self._updated_file_data = []
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
                    logger.info(
                        f"{self.stats.success:,} / {self.stats.expected_total:,} processed with {self.stats.failed:,} errors"
                    )
                    break
            except Exception:
                logger.error(
                    f"Unable to flush hash updates. Try {tries}/{self.max_tries}",
                    exc_info=True,
                )
                if tries == self.max_tries:
                    raise
                else:
                    # Sleep a little before retrying flush
                    time.sleep(0.25)


def hash_file_path(path: Path) -> str:
    logger.info(f"Hashing: {path}", extra=verbosity_extra(3))
    with open(path, "rb") as f:
        digest = hashlib.file_digest(f, "sha256")

    logger.info(f"Done Hashing {path}", extra=verbosity_extra(3))

    return digest.hexdigest()


def hash_file(
    file: File,
    output: queue.Queue[dict],
) -> None:
    """Hashes a file and puts the results into the output queue."""

    try:
        digest = hash_file_path(Path(file.path))
        output.put(
            {
                "id": file.id,
                "content_hash": digest,
                "status": FileStatus.BASIC,
            }
        )
        time.sleep(0.090)
    except Exception as e:
        logging.error(f"Unable to get hash of file {file.path}.", exc_info=True)
        output.put(
            {
                "id": file.id,
                "status": FileStatus.FAILED_TO_HASH,
                "error_message": str(e),
            }
        )


@dataclass
class SplitFiles:
    not_existing: list[File]  # Moved or maybe deleted
    existing: list[File]
    error: list[FileError]  # Error trying to read


def split_files_by_existing(files: Sequence[File]) -> SplitFiles:
    result = SplitFiles(not_existing=[], existing=[], error=[])

    for file in files:
        try:
            if Path(file.path).exists():
                result.existing.append(file)
            else:
                result.not_existing.append(file)
        except OSError as e:
            result.error.append(FileError(file=file, error=e))

    return result


def hash_files(
    database: Database,
    prefix: str | None = None,
    max_workers: int = 2,
    batch_size_get: int = 500,
    batch_size_update: int = 100,
) -> None:
    file_service = FileService(session_maker=lambda: database.get_session())

    logger.info("Clearing statuses")

    file_service.change_files_status_batched(
        from_status=[FileStatus.CHECKING, FileStatus.HASHING],
        to_status=FileStatus.BASIC,
        batch_size=batch_size_get,
    )

    logger.info("Counting files to hash")
    stats = HashStats()
    stats.expected_total = file_service.count_files_to_hash(prefix=prefix)

    logger.info(f"Found {stats.expected_total:,} files to hash")

    logger.info("Starting to calculate file hashes")
    hashed_file_queue = queue.Queue[dict](maxsize=100)

    updater = FileUpdater(
        database=database,
        hashed_file_queue=hashed_file_queue,
        stats=stats,
        max_size=batch_size_update,
    )
    updater.start()

    workers_semaphor = threading.Semaphore(value=max_workers * 10)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for files in file_service.generate_files_to_hash(
            prefix=prefix, batch_size=batch_size_get
        ):
            file_status = split_files_by_existing(files=files)

            file_service.delete_files(file_status.not_existing)

            error_updates = [
                {
                    "id": file_error.file.id,
                    "error": str(file_error.error),
                    "status": FileStatus.FAILED_TO_HASH,
                }
                for file_error in file_status.error
            ]
            file_service.update_files(changes=error_updates)
            file_service.change_file_status_for_ids(
                file_ids=[file.id for file in file_status.existing],
                to_status=FileStatus.HASHING,
            )

            for file in file_status.existing:
                workers_semaphor.acquire()

                time.sleep(0.020)

                hash_file_future = executor.submit(hash_file, file, hashed_file_queue)

                hash_file_future.add_done_callback(lambda _: workers_semaphor.release())

    hashed_file_queue.join()
    hashed_file_queue.shutdown()
    updater.join()
    updater.flush()
