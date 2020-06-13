from pathlib import Path

from airflow.hooks.http_hook import HttpHook

from .db import PostgresDB


def download_file(
    conn_id: str,
    endpoint: str,
    dir_path: Path
) -> Path:
    file_name = endpoint.split('/')[-1]
    file_path = dir_path/file_name

    hook = HttpHook(http_conn_id=conn_id, method='GET')
    resp = hook.run(str(endpoint))

    with open(file_path, 'w') as f:
        f.write(resp.content.decode('utf-8'))

    return file_path


def download_table_data(
    conn_id: str,
    table_name: str,
    dir_path: Path
) -> Path:
    pg_db = PostgresDB(conn_id)
    file_path = pg_db.save_table_to_file(table_name, dir_path)

    return file_path


def download_data(
    conn_id: str,
    object_name: str,
    object_type: str,
    dir_path: Path
) -> Path:
    if object_type == 'file':
        file_path = download_file(conn_id, object_name, dir_path)
    elif object_type == 'table':
        file_path = download_table_data(conn_id, object_name, dir_path)

    return file_path
