import logging
from pathlib import Path
from typing import Sequence

from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session

from file_ops.batch.batch import batch_items
from file_ops.database import models
from file_ops.database.database import Database
from file_ops.database.models import File
from file_ops.filesystem.filesystem import (
    FileInfo,
    FileUpdate,
    generate_file_insert_data,
    generate_file_paths,
    generate_file_update_data,
    get_file_change,
)

logger = logging.getLogger(__name__)


def get_files_for_paths(session: Session, paths: list[str]) -> Sequence[models.File]:
    query = select(models.File).where(models.File.path.in_(paths))

    return session.scalars(query).all()


def _get_file_actions(
    session: Session, file_info_items: list[FileInfo]
) -> tuple[list[FileInfo], list[File]]:
    "Returns [inserts, updates]"
    path_to_file = dict[str, File]()

    for file in get_files_for_paths(
        session=session, paths=[file_info.path for file_info in file_info_items]
    ):
        path_to_file[file.path] = file

    inserts: list[FileInfo] = []
    updates: list[File] = []

    for file_info in file_info_items:
        if file_info.path in path_to_file:
            updates.append(path_to_file[file_info.path])
        else:
            inserts.append(file_info)

    return inserts, updates


def _filter_update_changes(files: list[File]) -> list[FileUpdate]:
    changes: list[FileUpdate] = []
    for file in files:
        update_data = generate_file_update_data(file)
        if get_file_change(file, update_data):
            changes.append(update_data)

    return changes


def index_directory(
    path: Path, database: Database, batch_size: int = 500, skip: list[str] | None = None
) -> None:
    gen = batch_items(generate_file_paths(path, skip=skip), batch_size=batch_size)

    total_files_inserted = 0
    total_files_updated = 0

    with Session(database.engine) as session:
        for file_info_items in gen:
            inserts, updates = _get_file_actions(
                session=session, file_info_items=file_info_items
            )

            if len(inserts):
                total_files_inserted += len(inserts)
                session.execute(
                    insert(File),
                    [generate_file_insert_data(file_info) for file_info in inserts],
                )

            update_changes = 0
            if len(updates):
                changes = _filter_update_changes(updates)
                if len(changes):
                    update_changes = len(changes)
                    total_files_updated += update_changes

                    session.execute(
                        update(File),
                        changes,
                    )

            session.commit()

            if len(inserts):
                logger.info(
                    f"Inserted {len(inserts)} files. {total_files_inserted} total"
                )

            if update_changes:
                logger.info(
                    f"Updated {update_changes} files. {total_files_updated} total"
                )

            if not len(inserts) and not update_changes:
                logger.info("No updates or inserts")
