from pathlib import Path
from typing import Dict

from psycopg2 import sql

from airflow.hooks.postgres_hook import PostgresHook


class PostgresDB:
    def __init__(self, conn_id: str) -> None:
        self.hook = PostgresHook(postgres_conn_id=conn_id)

    def save_table_to_file(
        self,
        table_name: str,
        dir_path: Path
    ) -> Path:
        file_path = dir_path/f'{table_name}.csv'

        table_id = sql.Identifier(table_name)
        sql_text = 'COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER'
        sql_query = sql.SQL(sql_text).format(table_id)

        self.hook.copy_expert(sql_query, file_path)

        return file_path

    def load_table_from_file(
        self,
        table_name: str,
        file_path: Path
    ) -> Path:
        table_id = sql.Identifier(table_name)
        sql_text = "COPY {} FROM STDIN DELIMITER ',' CSV HEADER"
        sql_query = sql.SQL(sql_text).format(table_id)

        self.hook.copy_expert(sql_query, file_path)

        return file_path

    def create_schema(self, schema_name: str) -> None:
        schema_id = sql.Identifier(schema_name)
        sql_query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(schema_id)

        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)

    def create_table(self, table_info: Dict) -> None:
        table_name = table_info['table_name']
        table_cols = table_info['table_cols']
        constraint_name = table_info.get('constraint_name')
        constraint_cols = table_info.get('constraint_cols')

        table_id = sql.Identifier(table_name)

        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                cols = [f'{sql.Identifier(col_name).as_string(cur)} {col_type}'
                        for (col_name, col_type) in table_cols.items()]
                cols_list = sql.SQL(','.join(cols))

                if constraint_name is None or constraint_cols is None:
                    sql_text = 'CREATE TABLE IF NOT EXISTS {} ({})'
                    sql_query = sql.SQL(sql_text).format(table_id, cols_list)
                else:
                    constraint_id = sql.Identifier(constraint_name)
                    constraints = [sql.Identifier(field)
                                   for field in constraint_cols]
                    constraints_list = sql.SQL(',').join(constraints)

                    sql_text = 'CREATE TABLE IF NOT EXISTS {} ({}, CONSTRAINT {} UNIQUE ({}))'
                    sql_query = sql.SQL(sql_text).format(
                        table_id,
                        cols_list,
                        constraint_id,
                        constraints_list
                    )

                cur.execute(sql_query)

    def drop_table(self, table_name: str) -> None:
        table_id = sql.Identifier(table_name)
        sql_query = sql.SQL('DROP TABLE IF EXISTS {}').format(table_id)

        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)

    def execute(self, sql_query: str) -> None:
        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)

            conn.commit()
