suffixes: dict[str, int] = {
    "": 1,  # Empty suffix is assumed to be bytes
    "b": 1,
    "kb": 1_024,
    "mb": 1_048_576,  # 1024 ** 2
    "gb": 1_073_741_824,  # 1024 ** 3
    "tb": 1_099_511_627_776,  # 1024 ** 4
}


def parse_file_size(value: str) -> int:
    """Attempts to parse file size and returns it as the number of bytes. If parsing fails, a ValueError is raised"""

    if not len(value):
        raise ValueError("Value was empty")

    digits: str = ""
    suffix: str = ""

    for sub_str in value:
        if sub_str.isdigit():
            digits += sub_str
        elif sub_str.isspace():
            continue
        else:
            suffix += sub_str

    suffix_value = suffixes.get(suffix.lower(), None)
    if not suffix_value:
        raise ValueError(f"Unknown suffix {suffix}")

    return int(digits) * suffix_value


humanize_suffixes = ["b", "kb", "mb", "gb", "tb"]


def humanize_bytes_str(size: float | int) -> str:
    index = 0

    if isinstance(size, int):
        size = float(size)

    while size > 1024.0:
        index += 1
        size = size / 1024.0

    if index < len(humanize_suffixes):
        suffix = humanize_suffixes[index]
        return f"{size:.4}{suffix}"

    return f"{size:.4} 10^({index})"
