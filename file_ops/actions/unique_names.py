import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


def get_path_total_stem(path: Path) -> str:
    """Returns the stem of the path, but before any periods, '.' as opposed to just the last one"""
    parts = path.stem.split('.')
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

def generate_unique_name_changes_for_paths(paths: list[Path]) -> Iterator[UniqueNameChange]:
    if not len(paths):
        return

    # Sort the paths by name so if we have two or more with the same extension
    # they will be right next to each other.
    paths.sort(key=lambda path: path.name)

    last_file_path = paths[0]
    last_new_path = path_with_total_stem(last_file_path, str(uuid.uuid4()))

    yield UniqueNameChange(
        original_path=last_file_path,
        new_path=last_new_path
    )

    for file_path in paths[1:]:
        current_total_stem = get_path_total_stem(file_path)
        last_total_stem = get_path_total_stem(last_file_path)
        # The file names should only be the same if they are in the same directory.
        if last_file_path.parent == file_path.parent and current_total_stem == last_total_stem:
            new_stem = get_path_total_stem(last_new_path)
            last_new_path = path_with_total_stem(file_path, new_stem) 
        else:
            last_new_path = path_with_total_stem(file_path, str(uuid.uuid4()))

        yield UniqueNameChange(
            original_path=file_path,
            new_path=last_new_path
        )

        last_file_path = file_path

def generate_unique_name_changes(path: Path) -> Iterator[UniqueNameChange]:
    directories: deque[Path] = deque([path])

    while len(directories):
        directory = directories.popleft()

        file_paths: list[Path] = []
        for child_path in directory.iterdir():
            if child_path.is_dir():
                directories.append(child_path)
                continue

            file_paths.append(child_path)

        yield from generate_unique_name_changes_for_paths(file_paths)
        
def execute_unqiue_name_change(change: UniqueNameChange) -> None:
    source = change.original_path.absolute()
    source.rename(change.new_path)