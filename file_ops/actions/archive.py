import logging
import os.path
import shutil
import uuid
from pathlib import Path
from typing import Generator, Sequence

from sqlalchemy import ColumnElement, select

from file_ops.database import models
from file_ops.database.database import Database

logger = logging.getLogger(__name__)


def remove_directory(path: Path) -> None:
    logger.info(f"Deleting {path}")
    shutil.rmtree(str(path))


def generate_top_like_directories(
    database: Database, like: str, skip_recycle_bin: bool = True, batch_size: int = 50
) -> Generator[Sequence[models.File], None, None]:
    last_id: uuid.UUID | None = None

    not_like = like + os.path.sep + "%"

    while True:
        conditions: list[ColumnElement] = [
            models.File.path.like(like),
            models.File.path.not_like(not_like),
            models.File.is_directory.is_(True),
        ]

        if skip_recycle_bin:
            # TODO recycle bin on other operating systems
            conditions.append(
                models.File.path.not_like("%" + os.path.sep + "$RECYCLE.BIN" + "%")
            )

        if last_id:
            conditions.append(models.File.id > last_id)

        query = (
            select(models.File)
            .where(*conditions)
            .order_by(models.File.id)
            .limit(batch_size)
        )

        with database.get_session() as session:
            results = session.scalars(query).all()

        if not len(results):
            break

        last_id = results[-1].id

        yield results


def zip_path(path: Path) -> None:
    archive_path = path.with_suffix("")

    shutil.make_archive(str(archive_path), "zip", str(path))


def zip_path_like(
    database: Database,
    like: str,
    delete_original: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> None:
    count = 0
    for files in generate_top_like_directories(
        database=database, like=like, skip_recycle_bin=True, batch_size=5
    ):
        for file in files:
            if limit and count > limit:
                return

            path = Path(file.path)
            if not path.exists():
                logger.info(f"File not found {file.path}")
                continue

            if path.with_suffix(".zip").exists():
                logger.info(f"Zip found, skipping: {file.path}")

                if delete_original:
                    remove_directory(path)

                continue

            count += 1
            logger.info(f"Zipping {file.path}")
            if dry_run:
                continue

            try:
                zip_path(path)

                if delete_original:
                    remove_directory(path)

            except Exception as e:
                logger.error(f"Unable to zip path {path}", exc_info=e)

                zipped_file_path = path.with_suffix(".zip")
                try:
                    zipped_file_path.unlink(missing_ok=True)
                except Exception:
                    logger.error(f"Unable to delete failed zip: {zipped_file_path}")
