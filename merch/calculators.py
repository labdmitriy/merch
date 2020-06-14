from datetime import datetime, timedelta
from typing import Dict

DAYS_IN_YEAR = 365.25
DATE_TIME_FORMAT = '%Y-%m-%d'


def gen_fields(line: Dict, gen_map: Dict) -> Dict:
    new_fields = {key: func(line) for key, func in gen_map.items()}
    return {**line, **new_fields}


def calculate_payment_status(line: Dict) -> str:
    return 'success' if line['success'] is True else 'failure'


def calculate_age(line: Dict) -> int:
    now = datetime.now()
    birth_date = datetime.strptime(line['birth_date'], DATE_TIME_FORMAT)
    age = (now - birth_date) // timedelta(days=DAYS_IN_YEAR)
    return age
