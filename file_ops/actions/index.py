import logging
from pathlib import Path
from typing import Sequence

from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from file_ops.batch.batch import batch_items
from file_ops.database import models
from file_ops.database.database import Database
from file_ops.database.models import File
from file_ops.filesystem.filesystem import (
    FileInfo,
    generate_file_data,
    generate_file_paths,
)

logger = logging.getLogger(__name__)

def get_files_for_paths(session: Session, paths: list[str]) -> Sequence[models.File]:
    query = select(models.File).where(models.File.path.in_(paths))

    return session.scalars(query).all()


def filter_out_existing_paths(
    session: Session, file_info_items: list[FileInfo]
) -> list[FileInfo]:
    existing_paths = set(
        file_info.path
        for file_info in get_files_for_paths(
            session=session, paths=[file_info.path for file_info in file_info_items]
        )
    )

    return [
        file_info
        for file_info in file_info_items
        if file_info.path not in existing_paths
    ]

def index_directory(path: Path, database: Database, batch_size: int = 500) -> None:
    gen = batch_items(generate_file_paths(path), batch_size=batch_size)

    total_files = 0

    with Session(database.engine) as session:
        for file_info_items in gen:
            new_file_infos = filter_out_existing_paths(session=session, file_info_items=file_info_items)
            if not len(new_file_infos):
                continue

            # TODO if the file already exists, compare its attributes and update its status as according
            # this method should generate inserts and updates

            session.execute(
                insert(File),
                [generate_file_data(file_info) for file_info in new_file_infos],
            )

            session.commit()

            total_files += len(new_file_infos)
            print(f"Inserted {len(new_file_infos)} files. {total_files} total")
