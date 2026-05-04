import datetime
import logging
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import ColumnElement, delete, func, select, update
from sqlalchemy.orm import Session

from file_ops.database import models

logger = logging.getLogger(__name__)


@dataclass
class FileError:
    file: models.File
    error: Exception


def _get_file_to_hash_conditions() -> list[ColumnElement]:
    return [
        models.File.content_hash.is_(None),
        models.File.is_directory.is_(False),
        models.File.status == models.FileStatus.BASIC,
    ]


class FileService:
    @staticmethod
    def get_for_hashes(
        session: Session, hashes: Sequence[str]
    ) -> Sequence[models.File]:
        query = select(models.File).where(models.File.content_hash.in_(hashes))

        return session.scalars(query).all()

    @staticmethod
    def get_for_paths(session: Session, paths: Sequence[str]) -> Sequence[models.File]:
        query = select(models.File).where(models.File.path.in_(paths))

        return session.scalars(query).all()

    def __init__(self, session_maker: Callable[[], Session]):
        self.session_maker = session_maker

    def count_files_for_hash(self, hash: str) -> int:
        with self.session_maker() as session:
            query = select(func.count(models.File.id)).where(
                models.File.content_hash == hash
            )

            result = session.scalar(query)

            return result if result else 0

    def get_files_for_hash(
        self, hash: str, limit: int | None = None
    ) -> Sequence[models.File]:
        with self.session_maker() as session:
            query = select(models.File).where(models.File.content_hash == hash)
            if limit:
                query = query.limit(limit)

            return session.scalars(query).all()

    def _get_file_stats_for_hashes(
        self, session: Session, hashes: list[str]
    ) -> Sequence[models.FileStats]:
        query = select(models.FileStats).where(
            models.FileStats.content_hash.in_(hashes)
        )
        return session.scalars(query).all()

    def get_file_for_path(self, path: str) -> models.File | None:
        with self.session_maker() as session:
            result = session.execute(
                select(models.File).where(models.File.path == path)
            )
            return result.scalar_one_or_none()

    def get_file_stats_for_hashes(
        self, hashes: list[str]
    ) -> Sequence[models.FileStats]:
        with self.session_maker() as session:
            return self._get_file_stats_for_hashes(session=session, hashes=hashes)

    def check_and_update_file_stat(
        self, file_stat: models.FileStats
    ) -> models.FileStats | None:
        """Recalculates a file_stats so its file count and file size are up to date."""
        count = self.count_files_for_hash(hash=file_stat.content_hash)

        if count == 0:
            logger.info(f"FileStat has no files. Hash {file_stat.content_hash}")
            self.delete_file_stat(file_stat=file_stat)
            return None

        sample_files = self.get_files_for_hash(hash=file_stat.content_hash, limit=1)
        sample_file = sample_files[0]

        # It matches, no need to update anything.
        if file_stat.file_count == count and file_stat.file_size == sample_file.size:
            return file_stat

        file_stat.file_count = count
        file_stat.file_size = sample_file.size or 0
        file_stat.updated_at = datetime.datetime.now()

        with self.session_maker() as session:
            session.add(file_stat)
            session.commit()
            session.refresh(file_stat)

        return file_stat

    def delete_file_for_path(self, path: str) -> None:
        with self.session_maker() as session:
            result = session.execute(
                select(models.File).where(models.File.path == path)
            )
            existing_file: models.File | None = result.scalar_one_or_none()
            if existing_file:
                self._delete_file(session, existing_file)

    def _delete_file(self, session: Session, file: models.File) -> None:
        """Deletes file and updates associated FileStats from the database"""

        if file.content_hash:
            file_stats = self._get_file_stats_for_hashes(
                session=session, hashes=[file.content_hash]
            )
            for file_stat in file_stats:
                file_stat.file_count -= 1
                if file_stat.file_count == 0:
                    session.delete(file_stat)

        session.delete(file)
        session.commit()

    def delete_file(self, file: models.File) -> None:
        """Deletes file and updates associated FileStats from the database"""

        with self.session_maker() as session:
            self._delete_file(session=session, file=file)

    def delete_files(self, files: list[models.File]) -> None:
        """Deletes files and updates associated FileStats from the database"""
        if not len(files):
            return

        file_ids = [file.id for file in files]
        file_hash_counts: dict[str, int] = {}
        for file in files:
            if not file.content_hash:
                continue

            if file.content_hash not in file_hash_counts:
                file_hash_counts[file.content_hash] = 1
                continue

            file_hash_counts[file.content_hash] += 1

        with self.session_maker() as session:
            file_stats = self._get_file_stats_for_hashes(
                session, list(file_hash_counts.keys())
            )

            for file_stat in file_stats:
                file_stat.file_count -= file_hash_counts[file_stat.content_hash]
                if file_stat.file_count == 0:
                    session.delete(file_stat)

            delete_files = delete(models.File).where(models.File.id.in_(file_ids))
            session.execute(delete_files)
            session.commit()

    def set_file_errors(self, file_errors: list[FileError]) -> None:
        if not len(file_errors):
            return

        changes = [
            {"id": file_error.file.id, "error": str(str(file_error.error))}
            for file_error in file_errors
        ]

        with self.session_maker() as session:
            session.execute(update(models.File), changes)
            session.commit()

    def update_files(self, changes: list[dict]) -> None:
        if not len(changes):
            return

        with self.session_maker() as session:
            session.execute(update(models.File), changes)
            session.commit()

    def count_files_to_hash(self, prefix: str | None = None) -> int:
        conditions = _get_file_to_hash_conditions()
        if prefix:
            conditions.append(models.File.path.startswith(prefix))

        query = select(func.count(models.File.id)).where(*conditions)

        with self.session_maker() as session:
            count = session.scalar(query)

        return count if count else 0

    def generate_files_to_hash(
        self, prefix: str | None = None, batch_size: int = 500
    ) -> Generator[Sequence[models.File], None, None]:
        while True:
            conditions = _get_file_to_hash_conditions()

            if prefix:
                conditions.append(models.File.path.startswith(prefix))

            query = (
                select(models.File)
                .where(*conditions)
                .order_by(models.File.size)
                .limit(batch_size)
            )

            with self.session_maker() as session:
                results = session.scalars(query).all()

            if not len(results):
                break

            yield results

    def change_files_status(
        self,
        from_status: models.FileStatus,
        to_status: models.FileStatus,
        prefix: str | None = None,
    ) -> None:
        conditions = [models.File.status == from_status]
        if prefix:
            conditions.append(models.File.path.startswith(prefix))

        now = datetime.datetime.now()

        with self.session_maker() as session:
            session.execute(
                update(models.File)
                .where(*conditions)
                .values(status=to_status, db_updated_at=now)
            )

    def change_file_status_for_ids(
        self, file_ids: Sequence[uuid.UUID], to_status: models.FileStatus
    ) -> None:
        changes = [{"id": file_id, "status": to_status} for file_id in file_ids]

        self.update_files(changes)

    def change_files_status_batched(
        self,
        from_status: list[models.FileStatus],
        to_status: models.FileStatus,
        prefix: str | None = None,
        batch_size: int = 500,
    ) -> None:
        conditions: list[ColumnElement] = [models.File.status.in_(from_status)]
        if prefix:
            conditions.append(models.File.path.startswith(prefix))

        now = datetime.datetime.now()

        while True:
            query = select(models.File.id).where(*conditions).limit(batch_size)

            with self.session_maker() as session:
                file_ids = session.scalars(query).all()
                if not len(file_ids):
                    break

                session.execute(
                    update(models.File)
                    .where(models.File.id.in_(file_ids))
                    .values(status=to_status, db_updated_at=now)
                )

                session.commit()

    def generate_like(
        self, like: str, is_directory: bool | None = None, batch_size: int = 50
    ) -> Generator[Sequence[models.File], None, None]:
        last_id: uuid.UUID | None = None

        while True:
            conditions: list[ColumnElement] = [models.File.path.like(like)]

            if is_directory is not None:
                conditions.append(models.File.is_directory == is_directory)

            if last_id:
                conditions.append(models.File.id > last_id)

            query = (
                select(models.File)
                .where(*conditions)
                .order_by(models.File.id)
                .limit(batch_size)
            )

            with self.session_maker() as session:
                results = session.scalars(query).all()

            if not len(results):
                break

            last_id = results[-1].id

            yield results

    def delete_file_stat(self, file_stat: models.FileStats) -> None:
        with self.session_maker() as session:
            session.delete(file_stat)
            session.commit()
