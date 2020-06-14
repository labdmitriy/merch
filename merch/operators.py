from airflow.operators.python_operator import PythonOperator


class TemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql', '.json')