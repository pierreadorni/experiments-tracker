"""Shared validation for declared and registered research result paths."""

from pathlib import Path, PureWindowsPath

MAX_RESULT_PATH_CHARS = 1024


def validate_result_path(value):
    if not isinstance(value, str) or len(value) > MAX_RESULT_PATH_CHARS:
        raise ValueError("result path must be a bounded relative path without traversal")
    path = Path(value)
    windows_path = PureWindowsPath(value)
    if (
        not value.strip()
        or "\0" in value
        or path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or ".." in path.parts
        or ".." in windows_path.parts
    ):
        raise ValueError("result path must be a bounded relative path without traversal")
    return value
