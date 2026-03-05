"""Databricks SQL connector wrapper for the Meridian Portal app.

Uses the service principal OAuth credentials (DATABRICKS_HOST,
DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET) injected automatically
by the Databricks App runtime, plus DATABRICKS_HTTP_PATH from app.yaml.
"""

import os
from contextlib import contextmanager
from typing import Any

from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient


def _get_token() -> str:
    w = WorkspaceClient(
        host=f"https://{os.environ['DATABRICKS_HOST']}",
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    return w.config.authenticate()["Authorization"].removeprefix("Bearer ")


@contextmanager
def get_connection():
    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=_get_token(),
    )
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
