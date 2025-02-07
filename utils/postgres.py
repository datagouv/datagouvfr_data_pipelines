import os
from typing import TypedDict, Optional

import psycopg2
from airflow.hooks.base import BaseHook


class File(TypedDict):
    source_name: str
    source_path: str
    column_order: Optional[str]


class PostgresTool:
    def __init__(self, conn_name: str, schema: Optional[str] = None):
        airflow_conn = BaseHook.get_connection(conn_name)
        self.schema = schema or "public"
        self.conn = psycopg2.connect(
            host=airflow_conn.host,
            database=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password,
            port=airflow_conn.port,
            options=f"-c search_path={self.schema}",
        )

    def raise_if_not_file(self, file: File):
        is_file = os.path.isfile(os.path.join(file["source_path"], file["source_name"]))
        if not is_file:
            raise Exception(
                f"file {file['source_path']}{file['source_name']} does not exists"
            )

    def execute_query(self, query: str) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(query)
            data = self._return_sql_results(cur)
            self.conn.commit()
        return data

    def execute_sql_file(self, file: File) -> list[dict]:
        self.raise_if_not_file(file)
        with self.conn.cursor() as cur:
            cur.execute(
                open(os.path.join(file["source_path"], file["source_name"]), "r").read()
            )
            data = self._return_sql_results(cur)
            self.conn.commit()
            return data

    def copy_file(self, file: File, table: str, has_header: bool) -> list[dict]:
        self.raise_if_not_file(file)
        with self.conn.cursor() as cur:
            cur.copy_expert(
                sql=(
                    f"COPY {table} {file.get('column_order') or ''} FROM STDIN "
                    f"WITH CSV {'HEADER' * has_header} DELIMITER AS ','"
                ),
                file=open(os.path.join(file["source_path"], file["source_name"]), "r"),
            )
            data = self._return_sql_results(cur)
            self.conn.commit()
        return data

    @staticmethod
    def _return_sql_results(cur) -> list[dict]:
        """Return data from a sql query

        Args:
            cur (Cursor): cursor from postgres connection

        Returns:
                  list: result of sql query, or an empty list if no result
        """
        try:
            data = cur.fetchall()
        except psycopg2.ProgrammingError as e:
            print(e)
            data = None
        if data:
            columns = [desc[0] for desc in cur.description]
            return [{k: v for k, v in zip(columns, d)} for d in data]
        return []
