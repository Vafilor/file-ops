import pytest

from file_ops.file_size.file_size import parse_file_size


def test_parse_file_size_no_suffix() -> None:
    assert parse_file_size("0") == 0
    assert parse_file_size("5") == 5
    assert parse_file_size("235") == 235


def test_parse_file_size_spaces_suffix() -> None:
    assert parse_file_size("1 KB") == 1024
    assert parse_file_size("2   KB") == 2048


def test_parse_file_size_suffixes() -> None:
    assert parse_file_size("7 MB") == 7_340_032  # 7 * 1024**2
    assert parse_file_size("3 GB") == 3_221_225_472  # 3 * 1024**3
    assert parse_file_size("13 TB") == 14_293_651_161_088  # 13 * 1024**3


def test_parse_file_exceptions() -> None:
    with pytest.raises(ValueError):
        parse_file_size("")

    with pytest.raises(ValueError):
        parse_file_size("13 ZB")
