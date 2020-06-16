from datetime import datetime
from functools import reduce
from typing import Dict

from .db import PostgresDB
from .utils import apply_func

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class BadDataError(Exception):
    pass


def check_db(
    conn_id: str,
    success_task_name: str,
    failed_task_name: str
) -> str:
    pg_db = PostgresDB(conn_id)

    check_result = pg_db.is_db_alive()

    if check_result['is_alive']:
        return success_task_name
    else:
        return failed_task_name


def check_fields(line: Dict, check_map: Dict) -> None:
    [reduce(apply_func, check_funcs, line[field_name])
     for field_name, check_funcs in check_map.items()]


def check_num_field(field: str) -> None:
    try:
        float(field)
    except ValueError as e:
        raise BadDataError(f'Numeric field contains bad symbols: {str(e)}')


def check_datetime_field(field):
    try:
        datetime.strptime(field, DATETIME_FORMAT)
    except ValueError as e:
        raise BadDataError(f'Bad datetime field format: {str(e)}')
