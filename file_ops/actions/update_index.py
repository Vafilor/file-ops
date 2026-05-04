import logging
import os
from dataclasses import dataclass
from typing import Iterator, cast

from sqlalchemy import ColumnElement, func, select

from file_ops.database import models
from file_ops.database.database import Database
from file_ops.database.file_service import FileService
from file_ops.database.utils import (
    generate_files,
)
from file_ops.filesystem.filesystem import (
    FileUpdate,
    FileUpdateError,
    generate_file_update_data,
    generate_file_update_error,
    get_file_change,
)
from file_ops.logging_config import verbosity_extra

logger = logging.getLogger(__name__)


@dataclass
class FileUpdateChange:
    update: FileUpdate | FileUpdateError
    path: str
    reason: str


@dataclass
class FileChanges:
    deletes: list[models.File]
    updates: list[FileUpdateChange]
    files_checked: int


def count_index_changes(database: Database, prefix: str | None = None) -> int:
    conditions: list[ColumnElement[bool]] = []

    if prefix:
        conditions.append(models.File.path.startswith(prefix))

    with database.get_session() as session:
        count_query = select(func.count(models.File.id)).where(*conditions)

        return session.execute(count_query).scalar_one()


def get_file_path_stat(path: str) -> os.stat_result | None:
    """Returns the stat_result is found, or None if there was an error meaning file not found"""
    try:
        return os.stat(path=path)
    except OSError:
        return None
    except ValueError:
        # Non-encodable path
        return None


def generate_index_changes(
    database: Database, prefix: str | None = None, batch_size: int = 500
) -> Iterator[FileChanges]:
    with database.get_session() as session:
        # all_query = select(models.File.id)

        # all_results = session.execute(all_query)

        # ids_set = set()
        # for item in all_results:
        #     ids_set.add(item[0])

        # for files in generate_files(
        #     session=session, prefix=prefix, batch_size=batch_size
        # ):
        #     for file in files:
        #         ids_set.remove(file.id)

        # remaining = len(ids_set)

        for files in generate_files(
            session=session,
            prefix=prefix,
            batch_size=batch_size,
        ):
            changes: FileChanges = FileChanges(
                deletes=[], updates=[], files_checked=len(files)
            )

            for file in files:
                logger.info(f"Checking {file.path}", extra=verbosity_extra(3))
                try:
                    stat_result = get_file_path_stat(file.path)
                    if not stat_result:
                        changes.deletes.append(file)
                    else:
                        update = generate_file_update_data(file, stats=stat_result)
                        if file_change := get_file_change(file, update):
                            changes.updates.append(
                                FileUpdateChange(
                                    update=update,
                                    path=file.path,
                                    reason=file_change,
                                )
                            )
                except Exception as e:
                    update_error = generate_file_update_error(file, e)
                    changes.updates.append(
                        FileUpdateChange(
                            update=update_error,
                            path=file.path,
                            reason="error checking file",
                        )
                    )

            yield changes


def execute_file_changes(file_service: FileService, changes: FileChanges) -> None:
    file_service.delete_files(changes.deletes)

    updates = cast(list[dict], [item.update for item in changes.updates])
    file_service.update_files(updates)
