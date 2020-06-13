import csv
import json
from pathlib import Path
from typing import List


def load_csv_data(
    file_path: Path,
    field_names: List,
    skip_header: bool = True
) -> List:
    with open(file_path) as f:
        if skip_header:
            next(f)

        reader = csv.DictReader(f, fieldnames=field_names)
        data = list(reader)

    return data


def load_json_data(file_path: Path, id_name: str) -> List:
    with open(file_path) as f:
        json_data = json.loads(f.read())

    data = []

    for row_id, row_data in json_data.items():
        row = {}
        row[id_name] = row_id

        for key, value in row_data.items():
            row[key] = value

        data.append(row)

    return data


def load_data(
    file_path: Path,
    file_type: str,
    field_names: List
) -> List:
    if file_type == 'csv':
        data = load_csv_data(file_path, field_names)
    elif file_type == 'json':
        id_name = field_names[0]
        data = load_json_data(file_path, id_name)

    return data
