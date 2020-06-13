from datetime import datetime
from pathlib import Path
from typing import Callable, Union

FieldType = Union[str, int, float, datetime, None]


def apply_func(value: FieldType, func: Callable) -> FieldType:
    return func(value)


def gen_file_name(file_path: Path, suffix: str) -> Path:
    clean_file_name = f'{file_path.stem}{suffix}.{file_path.suffix}'
    clean_file_path = file_path.parent/clean_file_name
    return clean_file_path
