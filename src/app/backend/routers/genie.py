"""Genie Conversation API proxy for the Meridian Portal.

Provides a simple ask/poll interface so the React frontend can interact
with Databricks Genie spaces without needing direct workspace access.
Uses the databricks-sdk WorkspaceClient for auth.
"""

import os
import time

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

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


def _api(method: str, path: str, body: dict | None = None) -> dict:
    ws = _get_ws()
    resp = ws.api_client.do(method.upper(), path, body=body)
    return resp if isinstance(resp, dict) else {}


class AskRequest(BaseModel):
    space_id: str
    question: str


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
    """Send a question to a Genie space, poll for the answer, and return it."""
    conv = _api("post", f"/api/2.0/genie/spaces/{req.space_id}/start-conversation",
                body={"content": req.question})

    conversation_id = conv.get("conversation_id") or conv.get("id", "")
    message_id = conv.get("message_id", "")

    if not message_id:
        messages = conv.get("messages", [])
        if messages:
            message_id = messages[-1].get("id", "")

    if not conversation_id or not message_id:
        raise HTTPException(502, f"Unexpected Genie response: {list(conv.keys())}")

    for _ in range(30):
        time.sleep(2)
        msg = _api("get",
                    f"/api/2.0/genie/spaces/{req.space_id}/conversations/{conversation_id}/messages/{message_id}")

        status = msg.get("status", "")
        if status in ("COMPLETED", "COMPLETE"):
            return _extract_result(conversation_id, message_id, msg, req.space_id)
        if status in ("FAILED", "CANCELLED", "ERROR"):
            error_msg = msg.get("error", {}).get("message", status)
            return AskResponse(
                conversation_id=conversation_id,
                message_id=message_id,
                status="error",
                description=f"Genie error: {error_msg}",
            )

    return AskResponse(
        conversation_id=conversation_id,
        message_id=message_id,
        status="timeout",
        description="Genie took too long to respond. Try a simpler question.",
    )


def _extract_result(conversation_id: str, message_id: str, msg: dict, space_id: str) -> AskResponse:
    """Parse the Genie message response into a structured result."""
    attachments = msg.get("attachments", [])

    sql_query = None
    description = None
    columns: list[str] = []
    rows: list[list] = []
    query_attachment_id = None

    for att in attachments:
        att_type = att.get("type", "")
        if att_type == "QUERY" or "query" in att:
            query_obj = att.get("query", att)
            sql_query = query_obj.get("query", query_obj.get("sql", ""))
            description = query_obj.get("description", "")
            query_attachment_id = att.get("id") or att.get("attachment_id")

        if att_type == "TEXT" or "text" in att:
            text_obj = att.get("text", att)
            if isinstance(text_obj, dict):
                description = text_obj.get("content", str(text_obj))
            elif isinstance(text_obj, str):
                description = text_obj

    if query_attachment_id:
        try:
            result = _api(
                "get",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}"
                f"/messages/{message_id}/query-result/{query_attachment_id}",
            )
            stmt = result.get("statement_response", result)
            manifest = stmt.get("manifest", {})
            columns = [c.get("name", "") for c in manifest.get("schema", {}).get("columns", [])]
            chunks = stmt.get("result", {})
            rows = chunks.get("data_array", [])
        except Exception:
            pass

    if not description:
        description = msg.get("content", "")

    return AskResponse(
        conversation_id=conversation_id,
        message_id=message_id,
        status="completed",
        sql_query=sql_query,
        result_columns=columns,
        result_rows=rows[:100],
        description=description,
    )
