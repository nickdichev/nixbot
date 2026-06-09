"""Helpers around sqlc-generated query functions."""

from __future__ import annotations


def expect[T](value: T | None) -> T:
    """Unwrap an Optional result from a generated :one query that is
    structurally guaranteed to return a row (e.g. INSERT ... RETURNING
    or an aggregate without GROUP BY)."""
    if value is None:
        msg = "query unexpectedly returned no row"
        raise RuntimeError(msg)
    return value
