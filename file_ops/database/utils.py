import uuid
from typing import Iterator, Sequence

from sqlalchemy import ColumnElement, select
from sqlalchemy.orm import Session

from file_ops.database import models


def generate_files_for_status(
    session: Session,
    statuses: list[models.FileStatus],
    prefix: str | None = None,
    batch_size: int = 500,
) -> Iterator[Sequence[models.File]]:
    last_id: uuid.UUID | None = None

    while True:
        conditions: list[ColumnElement[bool]] = [models.File.status.in_(statuses)]
        if prefix:
            conditions.append(models.File.path.startswith(prefix))
        if last_id:
            conditions.append(models.File.id > last_id)

        query = (
            select(models.File)
            .where(*conditions)
            .order_by(models.File.id)
            .limit(batch_size)
        )

        files = session.scalars(query).all()

        if not len(files):
            break

        last_id = files[-1].id

        yield files


def generate_files(
    session: Session,
    prefix: str | None = None,
    batch_size: int = 500,
) -> Iterator[Sequence[models.File]]:
    last_id: uuid.UUID | None = None

    while True:
        conditions: list[ColumnElement[bool]] = []
        if prefix:
            conditions.append(models.File.path.startswith(prefix))
        if last_id:
            conditions.append(models.File.id > last_id)

        query = (
            select(models.File)
            .where(*conditions)
            .order_by(models.File.id)
            .limit(batch_size)
        )

        files = session.scalars(query).all()

        if not len(files):
            break

        last_id = files[-1].id

        yield files
