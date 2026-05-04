import datetime
import logging
import os
import re
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from watchdog.events import (
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    RegexMatchingEventHandler,
)
from watchdog.observers import Observer

from file_ops.database.database import Database
from file_ops.database.file_service import FileService
from file_ops.database.models import File
from file_ops.filesystem.filesystem import FileInfo, generate_file_insert_data

logger = logging.getLogger(__name__)


class FileEventHandler(RegexMatchingEventHandler):
    def __init__(
        self,
        *,
        regexes: list[str] | None = None,
        ignore_regexes: list[str] | None = None,
        ignore_directories: bool = False,
        case_sensitive: bool = False,
        file_service: FileService,
    ):
        super().__init__(
            regexes=regexes,
            ignore_regexes=ignore_regexes,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )
        self.file_service = file_service

    def _update_existing_file_on_change(self, file: File, data: dict[str, Any]) -> None:
        file.db_updated_at = data["db_updated_at"]
        file.created_at = data["created_at"]
        file.modified_at = data["modified_at"]
        file.is_directory = data["is_directory"]
        file.status = data["status"]
        file.size = data["size"]
        file.error_message = None
        file.content_hash = None

    def _on_file_changed(self, file_path: str, is_directory: bool) -> None:
        info = FileInfo(path=file_path, is_directory=is_directory)
        data = generate_file_insert_data(info)

        with self.file_service.session_maker() as session:
            result = session.execute(select(File).where(File.path == file_path))
            existing_file: File | None = result.scalar_one_or_none()
            if not existing_file:
                session.add(File(**data))
            else:
                self._update_existing_file_on_change(existing_file, data=data)

            session.commit()

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        logger.info("on_created", extra={"event": event})

        file_path: str = (
            event.src_path
            if isinstance(event.src_path, str)
            else event.src_path.decode("utf-8")
        )

        self._on_file_changed(file_path=file_path, is_directory=event.is_directory)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        logger.info("on_modified", extra={"event": event})

        file_path: str = (
            event.src_path
            if isinstance(event.src_path, str)
            else event.src_path.decode("utf-8")
        )
        self._on_file_changed(file_path=file_path, is_directory=event.is_directory)

    def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
        logger.info("on_deleted", extra={"event": event})

        file_path: str = (
            event.src_path
            if isinstance(event.src_path, str)
            else event.src_path.decode("utf-8")
        )

        self.file_service.delete_file_for_path(path=file_path)

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        logger.info("on_file_moved", extra={"event": event})

        src_file_path: str = (
            event.src_path
            if isinstance(event.src_path, str)
            else event.src_path.decode("utf-8")
        )
        dst_file_path: str = (
            event.dest_path
            if isinstance(event.dest_path, str)
            else event.dest_path.decode("utf-8")
        )

        now = datetime.datetime.now()
        with self.file_service.session_maker() as session:
            query = select(File).where(
                or_(File.path == src_file_path, File.path == dst_file_path)
            )
            results = session.scalars(query).all()
            existing_file: File | None = None
            existing_destination: File | None = None
            for result in results:
                if result.path == src_file_path:
                    existing_file = result
                else:
                    existing_destination = result

            if existing_destination:
                session.delete(existing_destination)

            if not existing_file:
                info = FileInfo(path=dst_file_path, is_directory=event.is_directory)
                data = generate_file_insert_data(info)
                session.add(File(**data))
            else:
                existing_file.path = dst_file_path
                existing_file.db_updated_at = now

                try:
                    stats = os.stat(dst_file_path)
                    existing_file.modified_at = datetime.datetime.fromtimestamp(
                        stats.st_mtime
                    )
                except Exception as e:
                    logger.error(
                        f"Unable to get stats for file {dst_file_path}.", exc_info=True
                    )
                    existing_file.error_message = str(e)

            session.commit()


def watch(path: Path, database: Database) -> None:
    database_path = str(database.get_database_directory().resolve())
    escaped = re.escape(database_path + os.sep) + ".*"

    file_service = FileService(session_maker=lambda: database.get_session())
    event_handler = FileEventHandler(
        file_service=file_service,
        ignore_regexes=[escaped],
    )
    observer = Observer()
    observer.schedule(event_handler, str(path), recursive=True)
    observer.start()
    try:
        while observer.is_alive():
            observer.join()
    finally:
        observer.stop()
        observer.join()
