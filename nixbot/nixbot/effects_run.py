"""Effects execution: gated discovery after a successful build,
per-effect queue items, and running one queued effect with its own
row and log.

Calls back into other concerns only via Orchestrator methods, which
keeps the module dependency graph acyclic.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING
from urllib.parse import quote

from .db_gen import builds as builds_q
from .db_gen import maintenance as q
from .db_gen import web as web_q
from .db_gen import work_queue as wq
from .effects import (
    EffectsContext,
    EffectsError,
    effects_context,
    list_effects,
    run_effect,
    should_run_effects,
)
from .executor import failure_excerpt
from .repo_config import CONFIG_FILENAMES, BranchConfig

if TYPE_CHECKING:
    from pathlib import Path

    from .db import BuildRecord
    from .db_gen.models import BuildEffectRun
    from .events import ChangeEvent, RepoInfo
    from .gitrepo import FetchCredentials
    from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)


async def maybe_run_effects(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    worktree_path: Path,
    credentials: FetchCredentials | None = None,
) -> None:
    repo = event.repo
    # Gating config comes from the default branch of the central
    # clone: the worktree is PR-controlled, so its nixbot.toml
    # could grant the PR effects (and deploy secrets).
    # refs/heads/ prefix: a bare branch name would resolve a tag
    # of the same name first (tags auto-follow into the clone).
    config_text = None
    for filename in CONFIG_FILENAMES:
        config_text = await o.repos.show_file(
            repo.key, f"refs/heads/{repo.default_branch}", filename
        )
        if config_text is not None:
            break
    default_branch_config = BranchConfig.loads(config_text)
    if not should_run_effects(
        default_branch_config,
        repo.default_branch,
        event.branch,
        is_pull_request=event.pr_number is not None,
    ):
        return
    # The run row guards against auto-re-running effects for the same
    # triggering ref. A reused build may still need another run for a
    # different branch, e.g. production after main.
    run_id = await builds_q.start_effect_run(
        o.pool,
        build_id=build.id,
        commit_sha=event.commit_sha,
        branch=event.branch,
        pr_number=event.pr_number,
    )
    if run_id is None:
        await _replay_effect_statuses(o, event, build)
        return
    task_token = o.task_tokens.issue(build.project_id)
    ctx = effects_context(
        o.config,
        repo,
        worktree_path=worktree_path,
        rev=event.commit_sha,
        branch=event.branch,
        git_token=credentials.token if credentials is not None else None,
        task_token=task_token,
    )
    try:
        names = await list_effects(ctx)
    except (EffectsError, OSError):
        # OSError: nixbot-effects not installed; effects are
        # best-effort and must not fail the (already reported) build.
        logger.exception("effects discovery failed", extra={"build_id": build.id})
        return
    finally:
        o.task_tokens.revoke(task_token)
    # Effects removed from the flake since the last run would
    # otherwise linger as stale pending rows.
    await q.drop_removed_effects(o.pool, run_id=run_id, names=names)
    await _enqueue_effects(o, event, build, run_id, names)


async def _enqueue_effects(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    run_id: int,
    names: list[str],
) -> None:
    """One queue item per effect, on the effect run's dedup key."""
    # Effect names are repo-controlled; a duplicate inside one batch
    # would make ON CONFLICT DO UPDATE fail with "cannot affect row a
    # second time".
    names = list(dict.fromkeys(names))
    if not names:
        return
    await builds_q.start_pending_effects(
        o.pool, build_id=build.id, run_id=run_id, names=names
    )
    await o.reporter.effects_started(event, build, len(names))
    await wq.enqueue_effect_items(
        o.pool,
        dedup_key=f"effect-run-{run_id}",
        build_id=build.id,
        run_id=run_id,
        names=names,
    )


async def run_effect_item(
    o: Orchestrator,
    info: RepoInfo,
    build: BuildRecord,
    name: str,
    run_id: int | None = None,
    credentials: FetchCredentials | None = None,
) -> None:
    """Dispatcher entry for one queued effect."""
    row = await q.effect_status(o.pool, build_id=build.id, run_id=run_id, name=name)
    if row != "pending":
        # Swept after a crash mid-run, or already terminal; started
        # effects never auto-re-run (deploys are not idempotent).
        return
    async with o.rerun_worktree(info, build, "effect", credentials) as (
        worktree_event,
        worktree_path,
    ):
        effect_run = (
            await builds_q.get_effect_run(o.pool, id_=run_id)
            if run_id is not None
            else None
        )
        if run_id is not None and effect_run is None:
            return
        event = _effects_event(effect_run, build, worktree_event)
        task_token = o.task_tokens.issue(build.project_id)
        try:
            ctx = effects_context(
                o.config,
                info,
                worktree_path=worktree_path,
                rev=event.commit_sha,
                branch=event.branch,
                git_token=credentials.token if credentials is not None else None,
                task_token=task_token,
            )
            await _run_one_effect(o, event, ctx, build, run_id, name)
            await _maybe_post_effects_summary(o, event, build, run_id)
        finally:
            o.task_tokens.revoke(task_token)


def _effects_event(
    effect_run: BuildEffectRun | None, build: BuildRecord, fallback: ChangeEvent
) -> ChangeEvent:
    """The ref that triggered the effects run, recorded on the build;
    pre-0018 builds have no record and fall back to the build commit."""
    if effect_run is not None:
        return replace(
            fallback,
            commit_sha=effect_run.commit_sha,
            branch=effect_run.branch,
            pr_number=effect_run.pr_number or None,
        )
    if build.effects_commit_sha is None:
        return fallback
    return replace(
        fallback,
        commit_sha=build.effects_commit_sha,
        branch=build.effects_branch or fallback.branch,
        pr_number=build.effects_pr_number,
    )


async def _maybe_post_effects_summary(
    o: Orchestrator, event: ChangeEvent, build: BuildRecord, run_id: int | None
) -> None:
    """Post the aggregate status once all effects settle; the items run
    independently, so the last to finish reports it."""
    rows = (
        await builds_q.effects_for_run(o.pool, run_id=run_id)
        if run_id is not None
        else await web_q.web_effects(o.pool, build_id=build.id)
    )
    statuses = [e.status for e in rows]
    if any(s in ("pending", "running") for s in statuses):
        return
    await o.reporter.effects_finished(
        event,
        build,
        failed=sum(1 for s in statuses if s != "succeeded"),
        succeeded=sum(1 for s in statuses if s == "succeeded"),
    )


async def _replay_effect_statuses(
    o: Orchestrator, event: ChangeEvent, build: BuildRecord
) -> None:
    """Re-post finished effect statuses for a duplicate delivery of a
    context that already ran effects."""
    run_id = await builds_q.effect_run_by_context(
        o.pool,
        build_id=build.id,
        branch=event.branch,
        pr_number=event.pr_number,
    )
    if run_id is None:
        return
    effects = [
        e
        for e in await builds_q.effects_for_run(o.pool, run_id=run_id)
        if e.status in ("succeeded", "failed")
    ]
    if not effects:
        return
    for effect in effects:
        await o.reporter.effect_finished(
            event,
            build,
            effect.name,
            success=effect.status == "succeeded",
            error=effect.error,
        )
    await o.reporter.effects_finished(
        event,
        build,
        failed=sum(1 for e in effects if e.status != "succeeded"),
        succeeded=sum(1 for e in effects if e.status == "succeeded"),
    )


async def _run_one_effect(
    o: Orchestrator,
    event: ChangeEvent,
    ctx: EffectsContext,
    build: BuildRecord,
    run_id: int | None,
    name: str,
) -> None:
    """One effect with its own row and log."""
    # A rerun resets the existing effect row.
    await builds_q.start_effect(
        o.pool, build_id=build.id, run_id=run_id, name=name, status="running"
    )
    # A green commit status on a failed deploy hides the failure; report
    # per-effect status so the forge reflects the real outcome.
    await o.reporter.effect_started(event, build, name)
    # Effect names come from untrusted flakes; percent-encode so
    # the log file cannot escape the log directory. The "effects/"
    # subdirectory keeps them apart from attribute logs (a flat
    # prefix would collide with an attribute named "effect-X").
    log_key = f"effect:{name}" if run_id is None else f"effect:{run_id}:{name}"
    log_file = (
        f"effects/{quote(name, safe='')}.zst"
        if run_id is None
        else f"effects/{run_id}/{quote(name, safe='')}.zst"
    )
    async with o.open_log(build.id, log_key, log_file) as writer:
        try:
            success = await run_effect(ctx, name, writer.write)
        except Exception as e:
            # Any escape would leave the row running forever
            # (nothing re-runs effects) and kill the loop for the
            # remaining effects.
            logger.exception(
                "effect crashed",
                extra={"build_id": build.id, "effect": name},
            )
            await writer.write(f"\n{e}\n".encode())
            success = False
    error = None
    if not success:
        logger.error("effect failed", extra={"build_id": build.id, "effect": name})
        error = failure_excerpt(writer.tail_lines()) or None
    await builds_q.finish_effect(
        o.pool,
        build_id=build.id,
        run_id=run_id,
        name=name,
        status="succeeded" if success else "failed",
        error=error,
        log_path=str(writer.path.relative_to(o.config.state_dir)),
        log_size=writer.bytes_seen,
        log_truncated=writer.truncated,
    )
    await o.reporter.effect_finished(event, build, name, success=success, error=error)
