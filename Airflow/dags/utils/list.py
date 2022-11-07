from typing import Any, Iterable, List


def list_chunk(
    arr: List[Any],
    n: int,
) -> List[List[Any]]:
    return [arr[i : i + n] for i in range(0, len(arr), n)]


def list_chunk_generator(
    arr: List[Any],
    chunk_size: int,
) -> Iterable[List[Any]]:
    for i in range(0, len(arr), chunk_size):
        yield arr[i : i + chunk_size]
