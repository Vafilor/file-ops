import datetime
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

from file_ops.database import models

logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    path: str
    is_directory: bool


def generate_file_paths(path: Path) -> Generator[FileInfo, None, None]:
    for dirpath, dirs, files in os.walk(path):
        for dir in dirs:
            yield FileInfo(path=os.path.join(dirpath, dir), is_directory=True)

        for file in files:
            yield FileInfo(path=os.path.join(dirpath, file), is_directory=False)


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
    except BaseException as be:
        logger.error(f"Unable to get stats for file {file_info.path}.", exc_info=True)
        result["error_message"] = str(be)

        return result

    try:
        result["size"] = None if file_info.is_directory else stats.st_size
        result["modified_at"] = datetime.datetime.fromtimestamp(stats.st_mtime)
        result["created_at"] = datetime.datetime.fromtimestamp(stats.st_birthtime)
    except BaseException as be:
        logger.error(
            f"Unable to get timestamps for file {file_info.path}", exc_info=True
        )
        result["error_message"] = str(be)

    return result


def generate_file_update_data(file: models.File) -> dict:
    now = datetime.datetime.now()

    result: dict[str, Any] = {
        "id": file.id,
        "db_updated_at": now,
        "size": None,
        "status": "basic",
    }

    try:
        stats = os.stat(file.path)
    except BaseException as be:
        logger.error(f"Unable to get stats for file {file.path}.", exc_info=True)
        result["status"] = "failed_to_index"
        result["error_message"] = str(be)

        return result

    try:
        result["size"] = None if file.is_directory else stats.st_size
        result["modified_at"] = datetime.datetime.fromtimestamp(stats.st_mtime)
    except BaseException as be:
        logger.error(f"Unable to get timestamps for file {file.path}", exc_info=True)
        result["status"] = "failed_to_index"
        result["error_message"] = str(be)

    return result
