"""Pytest configuration — mock Databricks notebook builtins for local testing."""

import builtins
from types import SimpleNamespace
from unittest.mock import MagicMock


def _make_dbutils_mock():
    """Create a mock dbutils that handles widgets.text() and widgets.get()."""
    store = {}

    def text(key, default=""):
        store[key] = default

    def get(key):
        return store.get(key, "")

    widgets = SimpleNamespace(text=text, get=get)
    return SimpleNamespace(widgets=widgets)


dbutils_mock = _make_dbutils_mock()
builtins.dbutils = dbutils_mock  # type: ignore[attr-defined]
builtins.spark = MagicMock()  # type: ignore[attr-defined]
builtins.display = lambda *a, **kw: None  # type: ignore[attr-defined]
