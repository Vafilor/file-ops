from pathlib import Path

from file_ops.actions.unique_names import (
    generate_unique_name_changes_for_paths,
    get_path_total_stem,
    path_with_total_stem,
)


def test_get_path_total_stem() -> None:
    assert get_path_total_stem(Path("path/hello.txt.wow")) == "hello"
    assert get_path_total_stem(Path("path/hello.txt")) == "hello"
    assert get_path_total_stem(Path("hello.txt.wow")) == "hello"
    assert get_path_total_stem(Path("hello")) == "hello"

def test_path_with_total_stem() -> None:
    assert path_with_total_stem(Path("path/hello.txt.wow"), "world") == Path("path/world.txt.wow")
    assert path_with_total_stem(Path("path/hello.txt"), "world") == Path("path/world.txt")
    assert path_with_total_stem(Path("hello.txt.wow"), "world") == Path("world.txt.wow")
    assert path_with_total_stem(Path("hello"), "world") == Path("world")

def test_generate_unique_name_changes_for_paths_single() -> None:
    result = list(generate_unique_name_changes_for_paths([Path("hello")]))

    assert str(result[0].original_path) == "hello"
    assert str(result[0].new_path) != "hello"

def test_generate_unique_name_changes_for_paths_multiple() -> None:
    result = list(generate_unique_name_changes_for_paths([Path("hello"), Path("world.txt"), Path("cake.png")]))

    assert not any([str(change.new_path) == "hello" for change in result])
    assert not any([str(change.new_path) == "world.txt" for change in result])
    assert not any([str(change.new_path) == "cake.png" for change in result])

def test_generate_unique_name_changes_for_paths_subdirectory() -> None:
    result = list(generate_unique_name_changes_for_paths([Path("hello.txt"), Path("sub/hello.txt")]))

    first = result[0]
    second = result[1]

    assert first.new_path.name != second.new_path.name

def test_generate_unique_name_changes_for_paths_preserve_for_extension() -> None:
    result = list(generate_unique_name_changes_for_paths([Path("hello"), Path("hello.txt"), Path("hello.txt.zip")]))

    first = result[0]
    second = result[1]
    third = result[2]

    assert get_path_total_stem(first.new_path) == get_path_total_stem(second.new_path)  == get_path_total_stem(third.new_path)