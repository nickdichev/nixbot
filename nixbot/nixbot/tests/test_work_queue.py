"""Work-queue claim semantics: dedup, per-key serialization, requeue."""

from __future__ import annotations

import asyncpg
import pytest

from nixbot.work_queue import WorkQueue

pytestmark = pytest.mark.usefixtures("fresh_work_queue")


async def test_enqueue_dedupes_pending(pool: asyncpg.Pool) -> None:
    queue = WorkQueue(pool)
    assert await queue.enqueue("rerun", "build-1", {"build_id": 1})
    # Double click: identical pending intent collapses.
    assert not await queue.enqueue("rerun", "build-1", {"build_id": 1})
    # A different payload is a distinct intent.
    assert await queue.enqueue("report", "build-1", {"attempt": 2})
    item = await queue.claim_next()
    assert item is not None
    assert item.kind == "rerun"
    assert item.payload == {"build_id": 1}
    # Claimed (running) no longer blocks new intent.
    assert await queue.enqueue("rerun", "build-1", {"build_id": 1})
    await queue.finish(item.id)


async def test_claim_serializes_per_key(pool: asyncpg.Pool) -> None:
    queue = WorkQueue(pool)
    await queue.enqueue("rerun", "build-2")
    first = await queue.claim_next()
    assert first is not None
    await queue.enqueue("effects", "build-2")
    # Same key still running: must not run concurrently.
    assert await queue.claim_next() is None
    # work_queue_running_uniq enforces this in the database.
    with pytest.raises(asyncpg.UniqueViolationError):
        await pool.execute(
            "UPDATE work_queue SET status = 'running' "
            "WHERE kind = 'effects' AND dedup_key = 'build-2'"
        )
    # A different key is unaffected.
    await queue.enqueue("rerun", "build-3")
    other = await queue.claim_next()
    assert other is not None
    assert other.dedup_key == "build-3"
    # An exception with an empty message must still fail the item.
    await queue.finish(first.id, error=str(Exception()))
    row = await pool.fetchrow(
        "SELECT status, error FROM work_queue WHERE id = $1", first.id
    )
    assert (row["status"], row["error"]) == ("failed", "")
    blocked = await queue.claim_next()
    assert blocked is not None
    assert blocked.kind == "effects"


async def test_settle_interrupted_requeues(pool: asyncpg.Pool) -> None:
    queue = WorkQueue(pool)
    await queue.enqueue("change", "proj-1")
    assert await queue.claim_next() is not None
    # Same intent re-enqueued before the crash.
    await queue.enqueue("change", "proj-1")
    await queue.settle_interrupted()
    item = await queue.claim_next()
    assert item is not None
    assert item.dedup_key == "proj-1"
    await queue.finish(item.id)
    assert await queue.claim_next() is None
    superseded = await pool.fetchval(
        "SELECT count(*) FROM work_queue WHERE status = 'failed'"
    )
    assert superseded == 1
