import tempfile
from contextlib import contextmanager
from io import BytesIO
from typing import Iterable, Optional


def named_temporary_file(suffix: Optional[str] = None) -> Iterable[BytesIO]:
    """Context that introduces a temporary file.

    Creates a temporary file, yields its name, and upon context exit, deletes it.
    (In contrast, tempfile.NamedTemporaryFile() provides a 'file' object and
    deletes the file as soon as that file object is closed, so the temporary file
    cannot be safely re-opened by another library or process.)

    Args:
        suffix: desired filename extension (e.g. '.mp4').

    Yields:
        The temporary file.
    """
    f = tempfile.NamedTemporaryFile(suffix=suffix)
    try:
        yield f
    finally:
        f.close()


@contextmanager
def temporary_file(suffix: Optional[str] = None) -> Iterable[BytesIO]:
    f = tempfile.TemporaryFile(suffix=suffix)
    try:
        yield f
    finally:
        f.close()
