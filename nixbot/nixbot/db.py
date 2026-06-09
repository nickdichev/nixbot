"""Data access layer (sqlc-generated queries over asyncpg).

The SQL lives in queries/builds.sql; `sqlc generate` produces
db_gen/builds.py. Key invariants:

- build identity is the post-merge tree hash: a second change event
  producing the same tree for the same project reuses the existing
  build instead of creating a new one,
- attribute completion is one transactional write (status + log
  metadata together),
- re-aggregation of a build's result is serialized per build via a row
  lock (SELECT ... FOR UPDATE) and bumps a monotonic status generation
  so stale forge status posts can be dropped.
"""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING

from .db_gen import builds as q
from .models import CacheStatus, NixEvalJobSuccess
from .sql_util import expect

if TYPE_CHECKING:
    from collections.abc import Sequence

    import asyncpg

    from .db_gen.models import Build as BuildRecord
    from .models import NixEvalJob
    from .scheduler import AttributeResult


class BuildStatus:
    PENDING = "pending"
    EVALUATING = "evaluating"
    BUILDING = "building"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    TERMINAL = frozenset({SUCCEEDED, FAILED, CANCELLED})


# Attribute statuses that count as failures when aggregating.
FAILED_ATTRIBUTE_STATUSES = frozenset(
    {"failed", "failed_eval", "dependency_failed", "cached_failure"}
)
TERMINAL_ATTRIBUTE_STATUSES = FAILED_ATTRIBUTE_STATUSES | frozenset(
    {"succeeded", "cancelled", "skipped_local"}
)


class BuildDB:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    # -- builds ---------------------------------------------------------

    async def get_or_create_build(  # noqa: PLR0913
        self,
        project_id: int,
        tree_hash: str,
        commit_sha: str,
        branch: str,
        pr_number: int | None = None,
        pr_author: str | None = None,
    ) -> tuple[BuildRecord, bool]:
        """Reuse keyed on post-merge tree hash across contexts."""
        async with self.pool.acquire() as conn, conn.transaction():
            await q.lock_build_identity(conn, key=f"{project_id}:{tree_hash}")
            row = await q.find_reusable_build(
                conn, project_id=project_id, tree_hash=tree_hash
            )
            if row is not None:
                if pr_number != row.pr_number:
                    if row.pr_number is not None or row.pr_author is not None:
                        row = await q.detach_build_from_pr(
                            conn, id_=row.id, pr_number=pr_number, branch=branch
                        )
                    elif pr_number is not None and branch == row.branch:
                        # Backfill PR identity for the pr_author authz
                        # rule when a push to the PR's own head branch
                        # created the build first. A PR must not capture
                        # authz over a build for another branch (e.g. a
                        # default-branch push sharing the tree hash).
                        row = await q.attach_build_to_pr(
                            conn,
                            id_=row.id,
                            pr_number=pr_number,
                            pr_author=pr_author,
                        )
                elif pr_author is not None and row.pr_author is None:
                    # Same PR: fill in the author when a previous event
                    # for this PR lacked it.
                    row = await q.backfill_pr_author(
                        conn, id_=row.id, pr_author=pr_author
                    )
                return expect(row), False
            row = await q.create_build(
                conn,
                project_id=project_id,
                tree_hash=tree_hash,
                commit_sha=commit_sha,
                branch=branch,
                pr_number=pr_number,
                pr_author=pr_author,
            )
            return expect(row), True

    async def create_failed_build(  # noqa: PLR0913
        self,
        project_id: int,
        commit_sha: str,
        branch: str,
        error: str,
        pr_number: int | None = None,
        pr_author: str | None = None,
    ) -> BuildRecord:
        """A build that failed before evaluation (e.g. merge conflict);
        no tree hash exists, the status is reported on the head SHA."""
        row = await q.create_failed_build(
            self.pool,
            project_id=project_id,
            commit_sha=commit_sha,
            branch=branch,
            pr_number=pr_number,
            pr_author=pr_author,
            error=error,
        )
        return expect(row)

    async def set_eval_warnings(self, build_id: int, warnings_json: str) -> None:
        """Streamed, deduplicated eval warnings; updated while the eval
        is still running (the trigger pushes a build_events notify)."""
        await q.set_eval_warnings(self.pool, id_=build_id, warnings=warnings_json)

    async def set_build_status(
        self,
        build_id: int,
        status: str,
        *,
        error: str | None = None,
    ) -> None:
        await q.set_build_status(
            self.pool,
            id_=build_id,
            status=status,
            error=error,
            terminal=list(BuildStatus.TERMINAL),
        )

    async def mark_eval_completed(self, build_id: int) -> None:
        """The full eval result is recorded in build_attributes; a later
        build of the same tree may reuse it instead of re-evaluating."""
        await q.mark_eval_completed(self.pool, id_=build_id)

    async def find_completed_eval(
        self, project_id: int, tree_hash: str, exclude_build_id: int
    ) -> int | None:
        """Most recent other build of the same tree whose eval result is
        fully recorded (e.g. a build cancelled after its eval)."""
        return await q.find_completed_eval(
            self.pool,
            project_id=project_id,
            tree_hash=tree_hash,
            exclude_build_id=exclude_build_id,
        )

    async def get_eval_jobs(self, build_id: int) -> list[NixEvalJobSuccess] | None:
        """Reconstruct the eval job set from the build's attribute rows;
        None when any row lacks a drv_path (eval failures must be
        reproduced by a fresh evaluation). Reconstructed jobs carry no
        dependency closures, like the crash-recovery rerun path."""
        rows = await q.eval_job_rows(self.pool, build_id=build_id)
        jobs = []
        for row in rows:
            if not row.drv_path:
                return None
            outputs = json.loads(row.outputs) if row.outputs else {}
            jobs.append(
                NixEvalJobSuccess(
                    attr=row.attr,
                    attr_path=row.attr.split("."),
                    cache_status=CacheStatus.not_built,
                    needed_builds=[],
                    needed_substitutes=[],
                    drv_path=row.drv_path,
                    name=row.attr,
                    outputs=outputs or {"out": None},
                    system=row.system or "",
                )
            )
        return jobs

    async def get_build(self, build_id: int) -> BuildRecord | None:
        return await q.get_build(self.pool, id_=build_id)

    async def mark_effects_started(self, build_id: int) -> bool:
        """Set the started-flag; returns False when it was already set
        (effects must never auto-re-run)."""
        return await q.mark_effects_started(self.pool, id_=build_id) is not None

    # -- attributes -----------------------------------------------------

    async def record_attributes(
        self, build_id: int, jobs: Sequence[NixEvalJob]
    ) -> None:
        """Persist eval results as pending rows (with statically-known
        outputs) so crash recovery can resume without a re-eval; eval
        failures are settled by the scheduler."""
        successes = [job for job in jobs if isinstance(job, NixEvalJobSuccess)]
        if not successes:
            return
        await q.record_attributes(
            self.pool,
            build_id=build_id,
            attrs=[job.attr for job in successes],
            systems=[job.system for job in successes],
            drv_paths=[job.drv_path for job in successes],
            outputs=[json.dumps(job.outputs) for job in successes],
        )

    async def settle_unfinished_attributes(self, build_id: int) -> None:
        """Mark pending/building rows cancelled. Builds that end without
        a normal aggregation (eval failure, supersedure) must not leave
        attributes that look like they are still running."""
        await q.settle_unfinished_attributes(self.pool, build_id=build_id)

    async def mark_attribute_building(
        self, build_id: int, attr: str, system: str | None, drv_path: str | None
    ) -> bool:
        """Flip an attribute to 'building' and stamp started_at so the
        web UI can distinguish running attributes from queued ones.
        Returns False when the row is already terminal (e.g. cancelled
        externally while queued): the caller must not build it."""
        return (
            await q.mark_attribute_building(
                self.pool,
                build_id=build_id,
                attr=attr,
                system=system,
                drv_path=drv_path,
            )
            is not None
        )

    async def complete_attribute(  # noqa: PLR0913
        self,
        build_id: int,
        result: AttributeResult,
        *,
        log_path: str | None = None,
        log_size: int = 0,
        log_truncated: bool = False,
        if_unfinished: bool = False,
    ) -> None:
        """Single atomic write: status, outputs, error and log
        metadata together (crash-recovery invariant). With
        if_unfinished, already-terminal rows are left untouched (early
        results must not overwrite settled attributes)."""
        await q.complete_attribute(
            self.pool,
            build_id=build_id,
            attr=result.attr,
            system=result.system,
            drv_path=result.drv_path,
            outputs=json.dumps({"out": result.out_path}) if result.out_path else None,
            status=result.status.value,
            error=result.error,
            # "Came from cache": already in the local store, or
            # successfully substituted from a binary cache.
            cached=result.status.value == "skipped_local"
            or (
                result.status.value == "succeeded"
                and isinstance(result.job, NixEvalJobSuccess)
                and result.job.cache_status == CacheStatus.cached
            ),
            log_path=log_path,
            log_size=log_size,
            log_truncated=log_truncated,
            if_unfinished=if_unfinished,
        )

    async def start_effect(
        self, build_id: int, name: str, status: str = "running"
    ) -> None:
        """Record an effect run; a rerun resets the existing row."""
        await q.start_effect(self.pool, build_id=build_id, name=name, status=status)

    async def finish_effect(  # noqa: PLR0913
        self,
        build_id: int,
        name: str,
        *,
        success: bool,
        error: str | None = None,
        log_path: str | None = None,
        log_size: int = 0,
        log_truncated: bool = False,
    ) -> None:
        await q.finish_effect(
            self.pool,
            build_id=build_id,
            name=name,
            status="succeeded" if success else "failed",
            error=error,
            log_path=log_path,
            log_size=log_size,
            log_truncated=log_truncated,
        )

    async def effects_for_build(self, build_id: int) -> list[dict]:
        rows = await q.effects_for_build(self.pool, build_id=build_id)
        return [dataclasses.asdict(row) for row in rows]

    async def get_attribute_statuses(self, build_id: int) -> dict[str, str]:
        rows = await q.attribute_statuses(self.pool, build_id=build_id)
        return {row.attr: row.status for row in rows}

    # -- aggregation ------------------------------------------------------

    async def aggregate_build(self, build_id: int) -> tuple[str, int]:
        """Recompute the build's aggregate result from its attributes.

        Serialized per build via a row lock taken before the statuses
        are read (see LockBuildRow); bumps the monotonic status
        generation. Returns (status, generation).
        """
        async with self.pool.acquire() as conn, conn.transaction():
            row = await q.lock_build_row(conn, id_=build_id)
            if row is None:
                msg = f"build {build_id} not found"
                raise LookupError(msg)
            statuses = list(await q.attribute_status_list(conn, build_id=build_id))
            if any(s not in TERMINAL_ATTRIBUTE_STATUSES for s in statuses):
                # Not all attributes terminal yet: keep current status.
                return row.status, row.status_generation
            if any(s in FAILED_ATTRIBUTE_STATUSES for s in statuses):
                status = BuildStatus.FAILED
            elif any(s == "cancelled" for s in statuses):
                status = BuildStatus.CANCELLED
            else:
                status = BuildStatus.SUCCEEDED
            generation = expect(
                await q.bump_build_status(conn, id_=build_id, status=status)
            )
            return status, generation
