"""Build restart and rerun paths driven from the work queue: attribute
resets, resuming pending attributes from stored eval results, falling
back to re-evaluation, and effects-only restarts.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .db import BuildStatus
from .events import ChangeEvent
from .recovery import check_store_paths, find_unfinished_builds
from .repos import repo_info

if TYPE_CHECKING:
    from .db import BuildRecord
    from .events import RepoInfo
    from .gitrepo import FetchCredentials
    from .recovery import ResumableBuild
    from .service import CIService

logger = logging.getLogger(__name__)


async def restart_effects(s: CIService, build_id: int) -> None:
    if build_id in s.orchestrator.cancel_events:
        return  # build (or an effects rerun) still running
    build = await s.orchestrator.db.get_build(build_id)
    if build is None or build.status != "succeeded":
        return  # effects only ever run after a successful build
    project = await s.repo_store.by_id(build.project_id)
    if project is None:
        return
    info = repo_info(project)
    credentials = await s.credentials_provider(info.forge).get(info.clone_url)
    await s.orchestrator.rerun_effects(info, build, credentials)


async def restart(s: CIService, build_id: int, attr: str | None) -> None:
    """Reset attributes (one or all) and re-run only the pending jobs
    from the stored eval results — no re-eval."""
    if build_id in s.orchestrator.cancel_events:
        return  # still running; a restart would double-build
    if await s.orchestrator.db.get_build(build_id) is None:
        return
    if attr is not None:
        # A stale attr (e.g. after a re-eval renamed it) must not
        # reset the build row and spawn an empty rerun.
        known = await s.pool.fetchval(
            "SELECT 1 FROM build_attributes WHERE build_id = $1 AND attr = $2",
            build_id,
            attr,
        )
        if known is None:
            logger.warning(
                "restart of unknown attribute ignored",
                extra={"build_id": build_id, "attr": attr},
            )
            return
    # An explicit rebuild clears cached failures so the attributes
    # actually build again instead of re-skipping.
    await s.pool.execute(
        "DELETE FROM failed_builds WHERE project_id = "
        "(SELECT project_id FROM builds WHERE id = $1) "
        "AND derivation IN "
        "(SELECT drv_path FROM build_attributes "
        "WHERE build_id = $1 AND ($2::text IS NULL OR attr = $2))",
        build_id,
        attr,
    )
    await s.pool.execute(
        "UPDATE build_attributes SET status = 'pending', error = NULL, "
        "started_at = NULL, finished_at = NULL "
        "WHERE build_id = $1 AND ($2::text IS NULL OR attr = $2)",
        build_id,
        attr,
    )
    if attr is None:
        # A full restart re-runs effects; a partial rebuild must
        # not re-deploy.
        await s.pool.execute(
            "UPDATE builds SET effects_started = FALSE WHERE id = $1", build_id
        )
        await s.pool.execute(
            "UPDATE build_effects SET status = 'pending', error = NULL, "
            "finished_at = NULL, log_path = NULL, log_size = 0, "
            "log_truncated = FALSE WHERE build_id = $1",
            build_id,
        )
    # Queued, not started: the rerun decides whether this becomes
    # a re-eval (evaluating) or an attribute rerun (building).
    # Clearing finished_at keeps retention cleanup off a build
    # that is about to rerun; clearing error/eval_warnings keeps
    # a stale failure banner off a restart that succeeds.
    await s.pool.execute(
        "UPDATE builds SET status = 'pending', error = NULL, "
        "eval_warnings = NULL, started_at = NULL, finished_at = NULL "
        "WHERE id = $1",
        build_id,
    )
    await rerun(s, build_id)


async def rerun(s: CIService, build_id: int) -> None:
    # Serialized by the work queue's per-build dedup key.
    build = await s.orchestrator.db.get_build(build_id)
    if build is None:
        return
    project = await s.repo_store.by_id(build.project_id)
    if project is None:
        return
    info = repo_info(project)
    credentials = await s.credentials_provider(info.forge).get(info.clone_url)
    results = await find_unfinished_builds(s.pool, build_id=build_id)
    resumable = results[0] if results else None
    if resumable is None:
        return
    unfinished_count = await s.pool.fetchval(
        "SELECT count(*) FROM build_attributes "
        "WHERE build_id = $1 AND status IN ('pending', 'building')",
        build_id,
    )
    if (
        # A crash mid-eval leaves a partial attribute set; resuming
        # it would report success for an incomplete build.
        build.status != "evaluating"
        and resumable.has_attributes
        and len(resumable.pending_jobs) == unfinished_count
    ):
        # Stored drv paths may have been garbage-collected since
        # the eval; rerunning them would fail with "path does not
        # exist" instead of rebuilding.
        drvs = [job.drv_path for job in resumable.pending_jobs]
        valid = await check_store_paths(drvs)
        if all(drv in valid for drv in drvs):
            await s.orchestrator.rerun_pending_attributes(
                info, build, resumable.pending_jobs, credentials
            )
            return
        logger.info(
            "stored derivations missing from the store; re-evaluating",
            extra={"build_id": build_id},
        )
    # No resumable eval results (no attribute rows, or unfinished
    # rows without drv_path): an empty rerun would aggregate to
    # "succeeded" without building anything; re-evaluate instead.
    try:
        await _reeval(s, info, build, credentials)
    except Exception:
        logger.exception("re-evaluation failed", extra={"build_id": build_id})
        await s.orchestrator.db.set_build_status(
            build_id,
            BuildStatus.FAILED,
            error="re-evaluation failed; see service logs",
        )
        await _report_interrupted(s, resumable)


async def _reeval(
    s: CIService,
    info: RepoInfo,
    build: BuildRecord,
    credentials: FetchCredentials | None,
) -> None:
    try:
        async with s.orchestrator.rerun_worktree(info, build, "rerun", credentials) as (
            event,
            worktree_path,
        ):
            # Stale rows (e.g. failed_eval with NULL drv_path) would
            # wedge the aggregate; the re-eval rewrites them. Finished
            # rows with a drv_path are kept: their results are valid
            # and the re-eval skips already-built attributes.
            # The flag must drop before the rows: a concurrent build
            # of the same tree must not reuse the partial set
            # (run_build only clears it after this window).
            await s.pool.execute(
                "UPDATE builds SET eval_completed = FALSE WHERE id = $1",
                build.id,
            )
            await s.pool.execute(
                "DELETE FROM build_attributes WHERE build_id = $1 "
                "AND (status IN ('pending', 'building') OR drv_path IS NULL)",
                build.id,
            )
            await s.orchestrator.run_build(event, build, worktree_path)
    finally:
        s.orchestrator.cancel_events.pop(build.id, None)


async def change_event_for(
    s: CIService, resumable: ResumableBuild
) -> ChangeEvent | None:
    project = await s.repo_store.by_id(resumable.project_id)
    if project is None:
        return None
    return ChangeEvent(
        repo=repo_info(project),
        branch=resumable.branch,
        commit_sha=resumable.commit_sha,
        pr_number=resumable.pr_number,
    )


async def _report_interrupted(s: CIService, resumable: ResumableBuild) -> None:
    """Post the failure to the forge; otherwise the commit status
    stays pending forever after an interrupted evaluation."""
    build = await s.orchestrator.db.get_build(resumable.build_id)
    event = await change_event_for(s, resumable)
    if build is None or event is None:
        return
    await s.orchestrator.reporter.eval_finished(
        event, build, success=False, warnings=[]
    )
    await s.orchestrator.reporter.build_finished(
        event, build, BuildStatus.FAILED, build.status_generation, []
    )
