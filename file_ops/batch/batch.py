from collections.abc import Iterable
from typing import Generator, TypeVar

T = TypeVar('T')

def batch_items(iter: Iterable[T], batch_size: int) -> Generator[list[T], None, None]:
    items: list[T] = [None] * batch_size # type: ignore
    count = 0
    for item in iter:
        if count == batch_size:
            yield items
            count = 0

        items[count] = item
        count += 1

    if count != 0:
        yield items[:count]