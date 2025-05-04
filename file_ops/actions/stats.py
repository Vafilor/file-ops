import datetime
import uuid
from typing import Iterator, Sequence

from sqlalchemy import ColumnElement, distinct, func, insert, select, update
from sqlalchemy.orm import Session

from file_ops.database import models
from file_ops.database.database import Database
from file_ops.database.file_stats_service import FileStatsService


def _generate_file_stat_hashes(session: Session) -> Iterator[Sequence[str]]:
    last_hash: str | None = None

    while True:
        query = select(models.FileStats.content_hash).order_by(
            models.FileStats.content_hash
        )
        if last_hash:
            query = query.where(models.FileStats.content_hash > last_hash)

        results = session.scalars(query).all()
        if not len(results):
            break

        last_hash = results[-1]

        yield results


def _get_unused_hashes(session: Session, hashes: Sequence[str]) -> list[str]:
    existing_hashes = set(hashes)

    query = select(distinct(models.File.content_hash)).where(
        models.File.content_hash.in_(hashes)
    )

    file_hashes = session.scalars(query).all()

    for file_hash in file_hashes:
        if file_hash in existing_hashes:
            existing_hashes.remove(file_hash)

    return list(existing_hashes)


def _delete_unused_file_stats(session: Session) -> None:
    for hashes in _generate_file_stat_hashes(session):
        unused_hashes = _get_unused_hashes(session, hashes)
        FileStatsService.delete_for_content_hashes(session, unused_hashes)


def _generate_file_hashes(
    session: Session, batch_size: int = 500
) -> Iterator[Sequence[str]]:
    last_hash: str | None = None

    while True:
        conditions: list[ColumnElement[bool]] = [models.File.content_hash.is_not(None)]
        if last_hash:
            conditions.append(models.FileStats.content_hash > last_hash)

        query = (
            select(distinct(models.File.content_hash))
            .where(*conditions)
            .limit(batch_size)
            .order_by(models.File.content_hash)
        )

        results = session.scalars(query).all()
        if not len(results):
            break

        last_hash = results[-1]

        final = [result for result in results if result is not None]

        yield final


def _get_unique_files_for_hash(
    session: Session, content_hashes: list[str], batch_size: int = 500
) -> list[models.File]:
    last_id: uuid.UUID | None = None

    hashes_used = set[str]()
    results: list[models.File] = []

    while True:
        conditions: list[ColumnElement[bool]] = [
            models.File.content_hash.in_(content_hashes)
        ]
        if last_id:
            conditions.append(models.File.id > last_id)

        query = (
            select(models.File)
            .where(*conditions)
            .limit(batch_size)
            .order_by(models.File.id)
        )

        scalars = session.scalars(query).all()
        if not len(scalars):
            break

        last_id = scalars[-1].id

        for scalar in scalars:
            if scalar.content_hash is None or scalar.content_hash in hashes_used:
                continue

            hashes_used.add(scalar.content_hash)

            results.append(scalar)

    return results


def _get_file_stats_for_hash(
    session: Session, content_hashes: list[str]
) -> Sequence[models.FileStats]:
    query = select(models.FileStats).where(
        models.FileStats.content_hash.in_(content_hashes)
    )

    return session.scalars(query).all()


def _count_files_for_hashes(
    session: Session, content_hashes: list[str]
) -> dict[str, int]:
    "Returns a dictionary where the key is content_hash and the value is the number of files with that hash"
    query = (
        select(models.File.content_hash, func.count(models.File.content_hash))
        .where(models.File.content_hash.in_(content_hashes))
        .group_by(models.File.content_hash)
    )

    rows = session.execute(query).all()

    hash_to_count = dict[str, int]()

    for row in rows:
        content_hash = row[0]
        if content_hash is None:
            continue

        hash_to_count[content_hash] = row[1]

    return hash_to_count


def _get_file_stat_updates(
    file_stats: Sequence[models.FileStats],
    hash_to_count: dict[str, int],
    now: datetime.datetime,
) -> list[dict]:
    updates = []
    for file_stat in file_stats:
        file_count = hash_to_count.get(file_stat.content_hash, 0)
        if file_count == file_stat.file_count:
            continue

        file_stat.file_count = file_count
        file_stat.updated_at = now
        updates.append({"id": file_stat.id, "file_count": file_count})

    return updates


def _get_file_stat_inserts(
    files: Sequence[models.File], hash_to_count: dict[str, int], now: datetime.datetime
) -> list[dict]:
    inserts = []

    for file in files:
        if file.content_hash is None or file.size is None:
            continue

        file_count = hash_to_count.get(file.content_hash, 0)
        inserts.append(
            {
                "id": uuid.uuid4(),
                "created_at": now,
                "updated_at": now,
                "content_hash": file.content_hash,
                "file_size": file.size,
                "file_count": file_count,
            }
        )

    return inserts


def calculate_stats(database: Database, batch_size: int = 500) -> None:
    with database.get_session() as session:
        _delete_unused_file_stats(session)

    for hashes in _generate_file_hashes(session=session, batch_size=batch_size):
        now = datetime.datetime.now()
        list_hashes = list(hashes)

        file_stats = _get_file_stats_for_hash(
            session=session, content_hashes=list_hashes
        )
        hash_to_count = _count_files_for_hashes(
            session=session, content_hashes=list_hashes
        )

        updates = _get_file_stat_updates(
            file_stats=file_stats, hash_to_count=hash_to_count, now=now
        )
        session.execute(update(models.FileStats), updates)

        for file_stat in file_stats:
            list_hashes.remove(file_stat.content_hash)

        files = _get_unique_files_for_hash(session=session, content_hashes=list_hashes)
        inserts = _get_file_stat_inserts(
            files=files, hash_to_count=hash_to_count, now=now
        )

        session.execute(insert(models.FileStats), inserts)

        session.commit()
