import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

from sqlalchemy import insert, update

from file_ops.database.database import Database
from file_ops.database.file_service import FileService
from file_ops.database.models import File
from file_ops.filesystem.filesystem import FileInfo, generate_file_insert_data


def get_path_total_stem(path: Path) -> str:
    """Returns the stem of the path, but before any periods, '.' as opposed to just the last one"""
    parts = path.stem.split(".")
    return parts[0]


def path_with_total_stem(path: Path, stem: str) -> Path:
    """Replaces the path's total stem, which is the stem of the path, but before any periods, '.', as opposed to just the last one"""
    name = path.name

    dot_index = name.find(".")
    if dot_index == -1:
        return path.with_stem(stem)

    return path.parent / Path(stem + name[dot_index:])


@dataclass
class UniqueNameChange:
    original_path: Path
    new_path: Path


def get_unique_name_changes_for_paths(
    paths: list[Path],
) -> list[UniqueNameChange]:
    if not len(paths):
        return

    # Sort the paths by name so if we have two or more with the same extension
    # they will be right next to each other.
    paths.sort(key=lambda path: path.name)

    last_file_path = paths[0]
    last_new_path = path_with_total_stem(last_file_path, str(uuid.uuid4()))

    changes = list[UniqueNameChange]()

    changes.append(
        UniqueNameChange(original_path=last_file_path, new_path=last_new_path)
    )

    for file_path in paths[1:]:
        current_total_stem = get_path_total_stem(file_path)
        last_total_stem = get_path_total_stem(last_file_path)
        # The file names should only be the same if they are in the same directory.
        if (
            last_file_path.parent == file_path.parent
            and current_total_stem == last_total_stem
        ):
            new_stem = get_path_total_stem(last_new_path)
            last_new_path = path_with_total_stem(file_path, new_stem)
        else:
            last_new_path = path_with_total_stem(file_path, str(uuid.uuid4()))

        changes.append(
            UniqueNameChange(original_path=file_path, new_path=last_new_path)
        )

        last_file_path = file_path

    return changes


def generate_unique_name_changes(
    path: Path, recursive: bool
) -> Iterator[list[UniqueNameChange]]:
    """generate_unique_name_changes goes through a directory and yields UniqueNameChange objects.
    It does not do any name changes itself. If recursive is set to true, files in subdirectories are also looked at."""
    directories: deque[Path] = deque([path])

    while len(directories):
        directory = directories.popleft()

        file_paths: list[Path] = []
        for child_path in directory.iterdir():
            if child_path.is_dir():
                directories.append(child_path)
                continue

            file_paths.append(child_path)

        if len(file_paths):
            yield get_unique_name_changes_for_paths(file_paths)

        if not recursive:
            break


def execute_unqiue_name_changes(
    database: Database, changes: Sequence[UniqueNameChange]
) -> None:
    file_paths = [str(change.original_path.absolute()) for change in changes]

    inserts = list[dict]()
    updates = list[dict]()

    with database.get_session() as session:
        files = FileService.get_for_paths(session, file_paths)
        path_to_file = {file.path: file for file in files}

        for change in changes:
            source = change.original_path.absolute()
            source.rename(change.new_path)

            if existing_file := path_to_file.get(str(source), None):
                updates.append({"id": existing_file.id, "path": str(change.new_path)})
            else:
                file_info = FileInfo.from_path(change.new_path)
                insert_data = generate_file_insert_data(file_info)
                inserts.append(insert_data)

        if len(inserts):
            session.execute(
                insert(File),
                inserts,
            )
            session.commit()

        if len(updates):
            session.execute(
                update(File),
                updates,
            )
            session.commit()
