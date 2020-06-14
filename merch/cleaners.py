import csv
from functools import reduce
from pathlib import Path
from typing import Dict, List

from .calculators import gen_fields
from .checkers import BadDataError, check_fields
from .loaders import load_data
from .utils import apply_func, gen_file_path


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
    clean_map: Dict,
    check_map: Dict,
    gen_map: Dict,
    error_file_path: Path,
    **context
) -> Path:
    clean_file_path = gen_file_path(file_path, '_clean')
    selected_field_names = list(clean_map.keys()) + list(gen_map.keys())

    data = load_data(file_path, file_type, field_names)

    with open(clean_file_path, 'w') as f_clean:
        writer = csv.DictWriter(f_clean,
                                fieldnames=selected_field_names,
                                extrasaction='ignore')
        writer.writeheader()

        for line in data:
            line = clean_line(line, clean_map)

            try:
                check_fields(line, check_map)
            except BadDataError:
                dag_id = context['task'].dag_id
                task_id = context['task'].task_id

                error_message = f'Bad data. DAG: {dag_id}, Task: {task_id}'

                with open(error_file_path, 'w') as f:
                    f.write(error_message)

                raise

            line = gen_fields(line, gen_map)
            writer.writerow(line)

    return clean_file_path


def strip(field: str) -> str:
    return field.strip()


def lower(field: str) -> str:
    return field.lower()
