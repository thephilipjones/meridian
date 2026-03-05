"""Genie Conversation API proxy for the Meridian Portal.

Uses the databricks-sdk's typed Genie client (start_conversation_and_wait,
create_message_and_wait) per the official embedding best practices.
Supports multi-turn follow-up conversations.
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()
log = logging.getLogger(__name__)

_ws: WorkspaceClient | None = None


def _get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient(
            host=f"https://{os.environ['DATABRICKS_HOST']}",
            client_id=os.environ["DATABRICKS_CLIENT_ID"],
            client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
        )
    return _ws


class AskRequest(BaseModel):
    space_id: str
    question: str
    conversation_id: str | None = None


class AskResponse(BaseModel):
    conversation_id: str
    message_id: str
    status: str
    sql_query: str | None = None
    result_columns: list[str] = []
    result_rows: list[list] = []
    description: str | None = None


@router.post("/ask", response_model=AskResponse)
def ask_genie(req: AskRequest):
    """Send a question to a Genie space and return the answer.

    If conversation_id is provided, sends a follow-up in the same
    conversation (retaining context). Otherwise starts a new conversation.
    """
    ws = _get_ws()

    try:
        if req.conversation_id:
            msg = ws.genie.create_message_and_wait(
                space_id=req.space_id,
                conversation_id=req.conversation_id,
                content=req.question,
            )
        else:
            msg = ws.genie.start_conversation_and_wait(
                space_id=req.space_id,
                content=req.question,
            )
    except Exception as e:
        log.exception("Genie API call failed")
        raise HTTPException(502, f"Genie error: {e}") from e

    return _build_response(ws, req.space_id, msg)


def _build_response(ws: WorkspaceClient, space_id: str, msg: GenieMessage) -> AskResponse:
    """Extract text, SQL, and tabular results from a GenieMessage."""
    conversation_id = msg.conversation_id or ""
    message_id = msg.id or ""

    status_str = msg.status.value if hasattr(msg.status, "value") else str(msg.status or "")
    if status_str in ("FAILED", "CANCELLED", "ERROR"):
        error_text = msg.error.message if msg.error else str(msg.status)
        return AskResponse(
            conversation_id=conversation_id,
            message_id=message_id,
            status="error",
            description=f"Genie error: {error_text}",
        )

    sql_query = None
    description = None
    columns: list[str] = []
    rows: list[list] = []

    for att in msg.attachments or []:
        if att.text and att.text.content:
            description = att.text.content

        if att.query and att.query.query:
            sql_query = att.query.query
            if att.query.description:
                description = description or att.query.description

            att_id = getattr(att, "attachment_id", None) or getattr(att, "id", None)
            if att_id:
                columns, rows = _fetch_query_result(
                    ws, space_id, conversation_id, message_id, att_id
                )

    if not description:
        description = msg.content or ""

    return AskResponse(
        conversation_id=conversation_id,
        message_id=message_id,
        status="completed",
        sql_query=sql_query,
        result_columns=columns,
        result_rows=rows[:100],
        description=description,
    )


def _fetch_query_result(
    ws: WorkspaceClient,
    space_id: str,
    conversation_id: str,
    message_id: str,
    attachment_id: str,
) -> tuple[list[str], list[list]]:
    """Fetch tabular results for a query attachment."""
    try:
        result = ws.genie.get_message_attachment_query_result(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id,
        )

        columns = []
        rows = []

        stmt = result.statement_response
        if stmt and stmt.manifest and stmt.manifest.schema:
            columns = [c.name for c in (stmt.manifest.schema.columns or []) if c.name]

        if stmt and stmt.result and stmt.result.data_array:
            rows = [list(row) for row in stmt.result.data_array]

        return columns, rows
    except Exception:
        log.warning("Failed to fetch query result for attachment %s", attachment_id, exc_info=True)
        return [], []
