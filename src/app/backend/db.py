"""Databricks SQL connector wrapper for the Meridian Portal app.

Reads warehouse configuration from environment variables set by the
Databricks App runtime. Returns query results as lists of dicts.
"""

import os
from contextlib import contextmanager
from typing import Any

from databricks import sql as dbsql


def _connection_params() -> dict:
    return {
        "server_hostname": os.environ["DATABRICKS_SERVER_HOSTNAME"],
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "access_token": os.environ.get("DATABRICKS_TOKEN", ""),
    }


@contextmanager
def get_connection():
    conn = dbsql.connect(**_connection_params())
    try:
        yield conn
    finally:
        conn.close()


def execute_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a SQL query and return results as a list of dicts."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]


def execute_query_single(query: str, params: dict[str, Any] | None = None) -> dict | None:
    """Execute a query and return the first row as a dict, or None."""
    results = execute_query(query, params)
    return results[0] if results else None
