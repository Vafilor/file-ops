import shutil
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Iterator

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from file_ops.database import models
from file_ops.database.database import Database
from file_ops.database.file_service import FileService


def generate_file_stats_by_biggest_file_count(
    session: Session,
    min_file_size: int | None = None,
    batch_size: int = 500,
) -> Iterator[Sequence[models.FileStats]]:
    last_id: uuid.UUID | None = None
    conditions = []

    while True:
        conditions = [models.FileStats.file_count != 1]

        if last_id:
            conditions.append(models.FileStats.id > last_id)

        if min_file_size:
            conditions.append(models.FileStats.file_size >= min_file_size)

        query = (
            select(models.FileStats)
            .where(*conditions)
            .order_by(desc(models.FileStats.file_count))
            .order_by(models.FileStats.id)
            .limit(batch_size)
        )

        results = session.scalars(query).all()
        if not len(results):
            break

        last_id = results[-1].id

        yield results


def move_files_to_directory(
    database: Database, files: list[models.File], path: Path
) -> None:
    if not path.exists():
        path.mkdir(parents=True)

    file_service = FileService(session_maker=lambda: database.get_session())

    names_taken = set[str]()

    files_to_delete_from_database: list[models.File] = []
    files_to_delete_from_filesystem: list[Path] = []
    for file in files:
        file_path = Path(file.path)
        files_to_delete_from_database.append(file)
        if not file_path.exists():
            continue

        destination_path = path / file_path.name

        counter = 0
        while destination_path.name in names_taken:
            names_taken.add(destination_path.name)
            counter += 1
            destination_path = destination_path.with_stem(
                file_path.stem + f"-{counter}"
            )

        while destination_path.exists():
            names_taken.add(destination_path.name)
            counter += 1
            destination_path = destination_path.with_stem(
                file_path.stem + f"-{counter}"
            )

        shutil.copy2(file_path, destination_path)

        names_taken.add(destination_path.name)

        files_to_delete_from_filesystem.append(file_path)

    for path in files_to_delete_from_filesystem:
        path.unlink(missing_ok=True)

    file_service.delete_files(files_to_delete_from_database)
