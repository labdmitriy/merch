import json
from pathlib import Path
from typing import Dict, List

from .cleaners import clean_data
from .db import PostgresDB
from .downloaders import download_data


def process_data(
    conn_id: str,
    object_name: str,
    object_type: str,
    dir_path: Path,
    file_type: str,
    field_names: List,
    clean_map: Dict,
    check_map: Dict,
    gen_map: Dict,
    **context
) -> Path:
    file_path = download_data(
        conn_id,
        object_name,
        object_type,
        dir_path
    )
    clean_file_path = clean_data(
        file_path,
        file_type,
        field_names,
        clean_map,
        check_map,
        gen_map,
        **context
    )
    return clean_file_path


def create_dataset(
    conn_id: str,
    **context
) -> None:
    pg_db = PostgresDB(conn_id)

    target_sql = context['templates_dict']['target_sql']
    temp_tables = json.loads(context['templates_dict']['temp_tables'])
    target_table = json.loads(context['templates_dict']['target_table'])

    pg_db.create_schema('public')

    for table_info in temp_tables:
        table_name = table_info['table_name']
        table_file_path = table_info['table_file_path']

        pg_db.drop_table(table_name)
        pg_db.create_table(table_info)
        pg_db.load_table_from_file(table_name, table_file_path)

    pg_db.create_table(target_table)
    pg_db.execute(target_sql)
