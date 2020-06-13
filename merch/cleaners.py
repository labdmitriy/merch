
import csv
from collections import OrderedDict
from functools import reduce
from pathlib import Path
from typing import Dict, List

from .calculators import gen_fields
from .helpers import apply_func
from .loaders import load_data


def clean_line(line: Dict, clean_map: Dict) -> Dict:
    clean_fields = {
        field_name: reduce(apply_func, clean_funcs, line[field_name])
        for field_name, clean_funcs in clean_map.items()
    }
    return {**line, **clean_fields}


def clean_data(
    file_path: Path,
    file_type: str,
    field_names: List,
    clean_map: OrderedDict,
    gen_map: OrderedDict
) -> Path:
    clean_file_path = file_path.parent/f'{file_path.stem}_clean.csv'
    selected_field_names = list(clean_map.keys()) + list(gen_map.keys())

    data = load_data(file_path, file_type, field_names)

    with open(clean_file_path, 'w') as f_clean:
        writer = csv.DictWriter(f_clean,
                                fieldnames=selected_field_names,
                                extrasaction='ignore')
        writer.writeheader()

        for line in data:
            line = clean_line(line, clean_map)
            line = gen_fields(line, gen_map)
            writer.writerow(line)

    return clean_file_path


def strip(field: str) -> str:
    return field.strip()


def lower(field: str) -> str:
    return field.lower()
