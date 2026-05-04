import datetime
import logging
import os
import re
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from stat import S_ISDIR
from typing import Any, Generator, TypedDict

from file_ops.database import models
from file_ops.logging_config import verbosity_extra

logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    path: str
    is_directory: bool

    @classmethod
    def from_path(cls, path: Path) -> "FileInfo":
        return cls(path=str(path.absolute()), is_directory=path.is_dir())


def generate_file_paths(
    path: Path, skip: list[str] | None = None
) -> Generator[FileInfo, None, None]:
    skip_patterns: list[re.Pattern] = []

    if skip:
        for item in skip:
            pattern = re.compile(item)
            skip_patterns.append(pattern)

    def should_skip(path: str) -> bool:
        if not len(skip_patterns):
            return False

        for pattern in skip_patterns:
            if pattern.fullmatch(path):
                return True

        return False

    for dirpath, dirs, files in os.walk(path):
        for dir in dirs:
            final_path = os.path.join(dirpath, dir)
            if should_skip(final_path):
                logger.info(f"Skipping {final_path}", extra=verbosity_extra(3))
                continue

            yield FileInfo(path=final_path, is_directory=True)

        for file in files:
            final_path = os.path.join(dirpath, file)
            if should_skip(final_path):
                logger.info(f"Skipping {final_path}", extra=verbosity_extra(3))
                continue

            yield FileInfo(path=final_path, is_directory=False)


def generate_file_insert_data(file_info: FileInfo) -> dict:
    now = datetime.datetime.now()

    result: dict[str, Any] = {
        "id": uuid.uuid4(),
        "path": file_info.path,
        "db_created_at": now,
        "db_updated_at": now,
        "size": None,
        "created_at": None,
        "modified_at": None,
        "is_directory": file_info.is_directory,
        "status": "basic",
    }

    try:
        stats = os.stat(file_info.path)
    except Exception as e:
        logger.error(f"Unable to get stats for file {file_info.path}.", exc_info=True)
        result["error_message"] = str(e)

        return result

    try:
        result["size"] = None if file_info.is_directory else stats.st_size
        result["modified_at"] = datetime.datetime.fromtimestamp(stats.st_mtime)
        result["created_at"] = datetime.datetime.fromtimestamp(stats.st_birthtime)
    except Exception as e:
        logger.error(
            f"Unable to get timestamps for file {file_info.path}", exc_info=True
        )
        result["error_message"] = str(e)

    return result


class FileUpdate(TypedDict):
    id: uuid.UUID
    db_updated_at: datetime.datetime
    modified_at: datetime.datetime | None
    size: int | None
    status: models.FileStatus
    error_message: str | None
    content_hash: str | None
    is_directory: bool | None


class FileUpdateError(TypedDict):
    id: uuid.UUID
    db_updated_at: datetime.datetime
    status: models.FileStatus
    error_message: str


def generate_file_update_data(
    file: models.File, stats: os.stat_result | None = None
) -> FileUpdate:
    now = datetime.datetime.now()

    result: FileUpdate = {
        "id": file.id,
        "db_updated_at": now,
        "modified_at": file.modified_at,
        "size": file.size,
        "status": models.FileStatus.BASIC,
        "error_message": file.error_message,
        "content_hash": file.content_hash,
        "is_directory": file.is_directory,
    }

    if not stats:
        try:
            stats = os.stat(file.path)
        except Exception as e:
            logger.error(f"Unable to get stats for file {file.path}.", exc_info=True)
            result["status"] = models.FileStatus.FAILED_TO_INDEX
            result["error_message"] = str(e)

            return result

    try:
        is_directory = S_ISDIR(stats.st_mode)
        result["is_directory"] = is_directory
        result["size"] = None if is_directory else stats.st_size

        if file.size != result["size"]:
            result["content_hash"] = None

    except Exception as e:
        logger.error(f"Unable check is_directory for file {file.path}", exc_info=True)
        result["status"] = models.FileStatus.FAILED_TO_INDEX
        result["error_message"] = f"Error checking is_directory {str(e)}"

        return result

    try:
        result["modified_at"] = datetime.datetime.fromtimestamp(stats.st_mtime)
    except Exception as e:
        logger.error(f"Unable to get modified_at for file {file.path}", exc_info=True)
        result["status"] = models.FileStatus.FAILED_TO_INDEX
        result["error_message"] = f"Error getting modified time {str(e)}"

    return result


def generate_file_update_error(file: models.File, ex: Exception) -> FileUpdateError:
    return {
        "id": file.id,
        "db_updated_at": datetime.datetime.now(),
        "status": models.FileStatus.FAILED_TO_INDEX,
        "error_message": str(ex),
    }


class FileChangeReason(StrEnum):
    ERROR = "error"
    STATUS_CHANGED = "status changed"
    MODIFIED = "file modified"


def get_file_change(file: models.File, update: FileUpdate) -> FileChangeReason | None:
    if update.get("error_message", None):
        return FileChangeReason.ERROR

    if (update["modified_at"] != file.modified_at) or (update["size"] != file.size):
        return FileChangeReason.MODIFIED

    if update["status"] != file.status:
        return FileChangeReason.STATUS_CHANGED

    return None
