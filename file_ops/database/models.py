import datetime
import uuid
from enum import StrEnum

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class FileStatus(StrEnum):
    BASIC = "basic"
    "Default status for the file"

    HASHING = "hashing"
    "The file is currently hashing"

    FAILED_TO_HASH = "failed_to_hash"
    "The file failed to hash"

    FAILED_TO_INDEX = "failed_to_index"
    "The file failed to index"


class File(Base):
    __tablename__ = "files"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    "A unique id for the file"

    db_created_at: Mapped[datetime.datetime]
    "When the file was added to the database"

    db_updated_at: Mapped[datetime.datetime]
    "Last time the file was updated in the database"

    path: Mapped[str] = mapped_column(index=True, unique=True)
    "Full path of the file"

    size: Mapped[int | None]
    "Size of the file in bytes. None if information is not available."

    content_hash: Mapped[str | None] = mapped_column(index=True)
    "Hash of the file content, if available."

    created_at: Mapped[datetime.datetime | None]
    "When the file was created if available, according to the filesystem"

    modified_at: Mapped[datetime.datetime | None]
    "When the file was last modified if available, according to the filesystem"

    is_directory: Mapped[bool]
    "true if the file is a directory, false otherwise"

    status: Mapped[FileStatus] = mapped_column(index=True)
    "Internal status of the database file, varies from the default 'basic' to 'hashing', etc. See FileStatus"

    error_message: Mapped[str | None]
    "Message indicating any error that happened while interacting with this file"


class FileStats(Base):
    __tablename__ = "file_stats"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    "A unique id for the file stat"

    created_at: Mapped[datetime.datetime]
    "When the record was created"

    updated_at: Mapped[datetime.datetime | None]
    "When the record was last updated"

    content_hash: Mapped[str] = mapped_column(index=True, unique=True)
    "Hash of the file contents this stat is for"

    file_size: Mapped[int]
    "Size of the file, in bytes, that this stat is for"

    file_count: Mapped[int]
    "Number of files in the files table that have the content hash of this record"
