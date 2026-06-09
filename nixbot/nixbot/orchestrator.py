"""Build orchestrator: change event → build record → eval → attribute
builds → aggregate result.

Wires together the repo manager (clone/worktree + PR merge), the eval
runner, the dependency-aware scheduler, the build executor, gcroots/
outputs updates, post-build steps, and effects. Status reporting is a
callback so forge integration (4.4) and the web frontend can subscribe
without coupling.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol
from urllib.parse import quote

from . import build_run, gcroots, outputs
from .canceller import (
    CancellationManager,
    RegisterOutcome,
    branch_key,
    has_skip_ci_marker,
)
from .db import BuildStatus
from .effects import (
    EffectsContext,
    EffectsError,
    effects_context,
    list_effects,
    run_effect,
    should_run_effects,
)
from .effects_state import TaskTokens
from .events import ChangeEvent, NullStatusReporter, RepoInfo, StatusReporter
from .executor import LogWriter, failure_excerpt
from .gitrepo import GitError, MergeConflictError, run_git
from .repo_config import CONFIG_FILENAMES, BranchConfig
from .web.logs import LogRegistry
from .work_queue import WorkQueue

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from pathlib import Path

    from .config import Config
    from .db import BuildDB, BuildRecord
    from .gitrepo import FetchCredentials, RepoManager
    from .models import NixEvalJobSuccess
    from .nix_eval import (
        EvalResult,
        EvalSettings,
        JobBatchCallback,
        StderrLineCallback,
    )
    from .scheduler import AttributeResult, BuildOutcome, FailedBuildCache

    GcrootRegistrar = Callable[[Path, str, str, str], Awaitable[None]]
    OutputWriter = Callable[[Path, str, str, str, str, str, str], Path]

logger = logging.getLogger(__name__)


def pr_refspec(forge: str, pr_number: int) -> str:
    """GitLab serves MR heads under refs/merge-requests/<iid>/*;
    GitHub and Gitea use refs/pull/<number>/*."""
    ref = (
        f"refs/merge-requests/{pr_number}"
        if forge == "gitlab"
        else f"refs/pull/{pr_number}"
    )
    return f"+{ref}/*:{ref}/*"


class EvalRunnerLike(Protocol):
    """What the orchestrator needs from nix_eval.EvalRunner."""

    async def run(
        self,
        worktree_path: Path,
        branch_config: BranchConfig,
        settings: EvalSettings,
        on_jobs: JobBatchCallback | None = None,
        on_stderr_line: StderrLineCallback | None = None,
    ) -> EvalResult: ...


class AttributeExecutor(Protocol):
    """What the orchestrator needs from executor.NixBuildExecutor."""

    async def build_attribute(
        self,
        build_key: object,
        job: NixEvalJobSuccess,
        log_writer: LogWriter,
        cwd: Path,
        cancel_event: asyncio.Event | None = None,
    ) -> BuildOutcome: ...


@dataclass
class Orchestrator:
    config: Config
    db: BuildDB
    repos: RepoManager
    eval_runner: EvalRunnerLike
    executor: AttributeExecutor
    reporter: StatusReporter = field(default_factory=NullStatusReporter)
    # Project id -> cache; scoped so one project's failures cannot
    # affect another's builds.
    failed_build_cache: Callable[[int], FailedBuildCache] | None = None
    # build id -> cancel event, set by the cancellation manager.
    cancel_events: dict[int, asyncio.Event] = field(default_factory=dict)
    # (build id, attr) -> cancel event for a single queued/running
    # attribute; registered for the lifetime of the executor job.
    attr_cancel_events: dict[tuple[int, str], asyncio.Event] = field(
        default_factory=dict
    )
    # Injectable for tests; defaults to the real implementations.
    register_gcroot: GcrootRegistrar = gcroots.register_gcroot
    write_output_path: OutputWriter = outputs.write_output_path
    canceller: CancellationManager = field(default_factory=CancellationManager)
    # Live log fan-out for the web frontend's SSE endpoints.
    log_registry: LogRegistry = field(default_factory=LogRegistry)
    # Per-run bearer tokens for the hercules state API.
    task_tokens: TaskTokens = field(default_factory=TaskTokens)
    # Second contexts attached to an in-flight build, for status fan-out.
    linked_events: dict[int, list[ChangeEvent]] = field(default_factory=dict)

    def _log_dir(self, build_id: int) -> Path:
        return self.config.state_dir / "logs" / str(build_id)

    def gcroots_dir(self, build: BuildRecord) -> Path:
        return self.config.state_dir / "eval-gcroots" / str(build.id)

    async def handle_change_event(
        self, event: ChangeEvent, credentials: FetchCredentials | None = None
    ) -> BuildRecord | None:
        """Full lifecycle for one change event. Returns the build record
        (None when the checkout failed before a build existed)."""
        repo = event.repo

        if has_skip_ci_marker(event.commit_message):
            logger.info(
                "skipping build due to [skip ci] marker",
                extra={"repo": repo.name, "commit": event.commit_sha},
            )
            return None

        # Fetch and create the per-build worktree; PR head is merged
        # into the base branch locally. PR refs are fetched per PR:
        # fetching all of them is unbounded on PR-heavy repos.
        refspecs = ["+refs/heads/*:refs/heads/*"]
        if event.pr_number is not None:
            refspecs.append(pr_refspec(repo.forge, event.pr_number))
        await self.repos.fetch(repo.key, repo.clone_url, refspecs, credentials)
        try:
            # Unique token: concurrent events for the same commit must
            # not share (and destroy) one checkout.
            worktree = await self.repos.checkout_for_build(
                repo.key,
                f"{repo.id}-{event.commit_sha[:12]}-{uuid.uuid4().hex[:8]}",
                base_commit=event.base_sha or event.commit_sha,
                head_commit=event.commit_sha if event.base_sha else None,
            )
        except MergeConflictError as e:
            # Merge conflict: failed build, status on the head SHA.
            build = await self.db.create_failed_build(
                repo.id,
                event.commit_sha,
                event.branch,
                str(e),
                pr_number=event.pr_number,
                pr_author=event.pr_author,
            )
            await self.reporter.eval_finished(event, build, success=False, warnings=[])
            await self.reporter.build_finished(event, build, BuildStatus.FAILED, 0, [])
            return build

        try:
            tree_hash = await worktree.tree_hash()
            build, created = await self.db.get_or_create_build(
                repo.id,
                tree_hash,
                event.commit_sha,
                event.branch,
                pr_number=event.pr_number,
                pr_author=event.pr_author,
            )
            await self._dispatch_build(
                event,
                build,
                created=created,
                tree_hash=tree_hash,
                worktree_path=worktree.path,
                credentials=credentials,
            )
            return build
        finally:
            await self.repos.remove_worktree(worktree)

    async def _dispatch_build(  # noqa: PLR0913
        self,
        event: ChangeEvent,
        build: BuildRecord,
        *,
        created: bool,
        tree_hash: str,
        worktree_path: Path,
        credentials: FetchCredentials | None,
    ) -> None:
        """Decide what this event means for the (possibly shared) build:
        reuse a terminal result, drop a stale event, attach to an
        in-flight build, or run it."""
        repo = event.repo
        key = branch_key(event.branch, event.pr_number)
        # A branch push reusing a PR build already shed the PR identity
        # (and took over the branch field) in get_or_create_build.
        # Out-of-order delivery check: an event whose commit is an
        # ancestor of the context's running build is stale. Checked
        # before the reuse branch too: a stale redelivery matching an
        # old terminal build must not supersede the in-flight build.
        incoming_stale = False
        running_commit = self.canceller.running_commit_for(repo.id, key)
        if running_commit is not None and running_commit != event.commit_sha:
            incoming_stale = await self._is_ancestor(
                repo.key, event.commit_sha, running_commit
            )

        if not created and build.status in (
            BuildStatus.SUCCEEDED,
            BuildStatus.FAILED,
        ):
            await self._reuse_terminal_build(
                event,
                build,
                key,
                tree_hash,
                worktree_path=worktree_path,
                credentials=credentials,
                incoming_stale=incoming_stale,
            )
            return

        in_flight = build.id in self.cancel_events
        rebuild = created or (not in_flight and build.status == BuildStatus.CANCELLED)
        if rebuild or in_flight:
            cancel_event = self.cancel_events.setdefault(build.id, asyncio.Event())
        else:
            # Not running here (e.g. crashed build awaiting recovery):
            # a stored entry would block its rerun.
            cancel_event = asyncio.Event()
        outcome = self.canceller.register(
            repo.id,
            key,
            build.id,
            tree_hash,
            event.commit_sha,
            cancel_event,
            incoming_is_ancestor_of_running=incoming_stale,
        )
        if outcome == RegisterOutcome.STALE:
            if rebuild:
                self.cancel_events.pop(build.id, None)
            if created:
                await self.db.set_build_status(build.id, BuildStatus.CANCELLED)
                await self.reporter.build_finished(
                    event, build, BuildStatus.CANCELLED, build.status_generation, []
                )
            return
        if not rebuild:
            await self._attach_linked_event(event, build)
            return
        try:
            await self.run_build(event, build, worktree_path, credentials)
        finally:
            self.canceller.complete(build.id)
            self.cancel_events.pop(build.id, None)
            self.linked_events.pop(build.id, None)

    async def run_build(
        self,
        event: ChangeEvent,
        build: BuildRecord,
        worktree_path: Path,
        credentials: FetchCredentials | None = None,
    ) -> None:
        """Evaluate and build; every attribute completion is one
        transactional DB write, then the result is re-aggregated."""
        await build_run.run_build(self, event, build, worktree_path, credentials)

    async def refresh_schedules(self, event: ChangeEvent) -> None:
        """Queue `onSchedule` re-discovery after a successful
        default-branch build; the service's scheduled-effects loop only
        sweeps what the executor stores."""
        if event.pr_number is not None or event.branch != event.repo.default_branch:
            return
        await WorkQueue(self.db.pool).enqueue(
            "refresh-schedules",
            f"schedules-{event.repo.id}",
            {"project_id": event.repo.id, "rev": event.commit_sha},
        )

    @asynccontextmanager
    async def rerun_worktree(
        self,
        info: RepoInfo,
        build: BuildRecord,
        prefix: str,
        credentials: FetchCredentials | None,
    ) -> AsyncIterator[tuple[ChangeEvent, Path]]:
        """Event reconstruction plus a fresh worktree at the recorded
        commit; shared by the rerun paths."""
        event = ChangeEvent(
            repo=info,
            branch=build.branch,
            commit_sha=build.commit_sha,
            pr_number=build.pr_number,
        )
        # PR head commits are only reachable via the PR refs.
        refspecs = ["+refs/heads/*:refs/heads/*"]
        if build.pr_number is not None:
            refspecs.append(pr_refspec(info.forge, build.pr_number))
        await self.repos.fetch(info.key, info.clone_url, refspecs, credentials)
        worktree = await self.repos.checkout_for_build(
            info.key,
            f"{prefix}-{build.id}",
            base_commit=build.commit_sha,
        )
        try:
            yield event, worktree.path
        finally:
            await self.repos.remove_worktree(worktree)

    async def rerun_pending_attributes(
        self,
        info: RepoInfo,
        build: BuildRecord,
        pending_jobs: list[NixEvalJobSuccess],
        credentials: FetchCredentials | None = None,
    ) -> None:
        """Re-run only the pending attributes of an existing build using
        the stored eval results — no re-evaluation (attribute restarts
        and crash recovery)."""
        if build.id in self.cancel_events:
            # Already running; a concurrent rerun would double-write
            # attribute completions.
            return
        # Claim the slot before the first await; concurrent reruns
        # must not pass the guard together.
        cancel_event = self.cancel_events[build.id] = asyncio.Event()
        try:
            current = await self.db.get_build(build.id)
            if current is not None and current.status == "cancelled":
                # Cancelled between scheduling the rerun and getting here.
                return
            # Pending rows for systems no longer in build_systems would
            # stay non-terminal forever: the scheduler drops their jobs.
            # Drop the rows too (same as never recording them).
            unsupported = [
                job
                for job in pending_jobs
                if job.system not in self.config.build_systems
            ]
            if unsupported:
                await self.db.pool.execute(
                    "DELETE FROM build_attributes WHERE build_id = $1 "
                    "AND attr = ANY($2::text[])",
                    build.id,
                    [job.attr for job in unsupported],
                )
                pending_jobs = [
                    job
                    for job in pending_jobs
                    if job.system in self.config.build_systems
                ]
            # No re-eval happens on this path; go straight to building.
            await self.db.set_build_status(build.id, BuildStatus.BUILDING)
            # Register so supersede/PR-close cancellation also covers
            # recovered and restarted builds.
            self.canceller.register(
                info.id,
                branch_key(build.branch, build.pr_number),
                build.id,
                build.tree_hash or "",
                build.commit_sha,
                cancel_event,
            )
            async with self.rerun_worktree(info, build, "rerun", credentials) as (
                event,
                worktree_path,
            ):
                # cache_failures=False: see _ReadOnlyFailedBuildCache.
                status = await build_run.build_attributes(
                    self,
                    event,
                    build,
                    worktree_path,
                    pending_jobs,
                    cache_failures=False,
                )
                if status == BuildStatus.SUCCEEDED:
                    # Crash recovery before effects started; the
                    # started-flag keeps already-deployed builds from
                    # re-deploying.
                    await self.maybe_run_effects(
                        event, build, worktree_path, credentials
                    )
                    await self.refresh_schedules(event)
        finally:
            self.canceller.complete(build.id)
            self.cancel_events.pop(build.id, None)

    async def rerun_effects(
        self,
        info: RepoInfo,
        build: BuildRecord,
        credentials: FetchCredentials | None = None,
    ) -> None:
        """Effects-only restart: fresh worktree at the recorded commit,
        attributes untouched."""
        if build.id in self.cancel_events:
            # A concurrent rerun (or double click) would deploy twice.
            return
        self.cancel_events[build.id] = asyncio.Event()
        try:
            # Reset under the claim: resetting earlier (e.g. in the
            # service) could clobber a rerun already in flight.
            await self.db.pool.execute(
                "UPDATE builds SET effects_started = FALSE WHERE id = $1", build.id
            )
            await self.db.pool.execute(
                "UPDATE build_effects SET status = 'pending', error = NULL, "
                "finished_at = NULL, log_path = NULL, log_size = 0, "
                "log_truncated = FALSE WHERE build_id = $1",
                build.id,
            )
            async with self.rerun_worktree(info, build, "effects", credentials) as (
                event,
                worktree_path,
            ):
                await self.maybe_run_effects(event, build, worktree_path, credentials)
                await self.refresh_schedules(event)
            # The enqueued effect items share this build's key and only
            # become claimable once this item finishes.
        finally:
            self.cancel_events.pop(build.id, None)

    async def maybe_run_effects(
        self,
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
            config_text = await self.repos.show_file(
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
        # The started-flag guards against auto-re-running effects on
        # crash recovery (deploys are not idempotent).
        if not await self.db.mark_effects_started(build.id):
            return
        task_token = self.task_tokens.issue(build.project_id)
        ctx = effects_context(
            self.config,
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
            self.task_tokens.revoke(task_token)
        # Effects removed from the flake since the last run would
        # otherwise linger as stale pending rows.
        await self.db.pool.execute(
            "DELETE FROM build_effects WHERE build_id = $1 "
            "AND NOT (name = ANY($2::text[]))",
            build.id,
            names,
        )
        await self._enqueue_effects(build, names)

    async def _enqueue_effects(self, build: BuildRecord, names: list[str]) -> None:
        """One queue item per effect, on the build's dedup key."""
        queue = WorkQueue(self.db.pool)
        for name in names:
            await self.db.start_effect(build.id, name, status="pending")
            await queue.enqueue(
                "effect", f"build-{build.id}", {"build_id": build.id, "name": name}
            )

    async def run_effect_item(
        self,
        info: RepoInfo,
        build: BuildRecord,
        name: str,
        credentials: FetchCredentials | None = None,
    ) -> None:
        """Dispatcher entry for one queued effect."""
        row = await self.db.pool.fetchval(
            "SELECT status FROM build_effects WHERE build_id = $1 AND name = $2",
            build.id,
            name,
        )
        if row != "pending":
            # Swept after a crash mid-run, or already terminal; started
            # effects never auto-re-run (deploys are not idempotent).
            return
        async with self.rerun_worktree(info, build, "effect", credentials) as (
            event,
            worktree_path,
        ):
            task_token = self.task_tokens.issue(build.project_id)
            try:
                ctx = effects_context(
                    self.config,
                    info,
                    worktree_path=worktree_path,
                    rev=event.commit_sha,
                    branch=event.branch,
                    git_token=credentials.token if credentials is not None else None,
                    task_token=task_token,
                )
                await self._run_one_effect(ctx, build, name)
            finally:
                self.task_tokens.revoke(task_token)

    @asynccontextmanager
    async def open_log(
        self, build_id: int, key: str, filename: str
    ) -> AsyncIterator[LogWriter]:
        """LogWriter registered for live streaming; closed and
        unregistered on exit. Shared by attribute and effect runs."""
        log_path = self._log_dir(build_id) / filename
        writer = LogWriter(path=log_path, size_limit=self.config.log_size_limit)
        self.log_registry.register(build_id, key, writer)
        try:
            yield writer
        finally:
            await writer.close()
            self.log_registry.unregister(build_id, key)

    async def _run_one_effect(
        self, ctx: EffectsContext, build: BuildRecord, name: str
    ) -> None:
        """One effect with its own row and log."""
        await self.db.start_effect(build.id, name)
        # Effect names come from untrusted flakes; percent-encode so
        # the log file cannot escape the log directory. The "effects/"
        # subdirectory keeps them apart from attribute logs (a flat
        # prefix would collide with an attribute named "effect-X").
        async with self.open_log(
            build.id, f"effect:{name}", f"effects/{quote(name, safe='')}.zst"
        ) as writer:
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
        await self.db.finish_effect(
            build.id,
            name,
            success=success,
            error=error,
            log_path=str(writer.path.relative_to(self.config.state_dir)),
            log_size=writer.bytes_seen,
            log_truncated=writer.truncated,
        )

    async def _attach_linked_event(
        self, event: ChangeEvent, build: BuildRecord
    ) -> None:
        """In-flight (or recovering) build shared with another context:
        attach for the final status fan-out."""
        self.linked_events.setdefault(build.id, []).append(event)
        await self.reporter.build_started(event, build)
        # The build may have turned terminal between the record fetch
        # and the attach: the final fan-out already happened and would
        # never cover this event. Replay the final status instead.
        current = await self.db.get_build(build.id)
        if current is not None and current.status in BuildStatus.TERMINAL:
            with contextlib.suppress(KeyError, ValueError):
                self.linked_events[build.id].remove(event)
            await self._replay_terminal_status(event, current)

    async def _replay_terminal_status(
        self, event: ChangeEvent, build: BuildRecord
    ) -> None:
        """Re-post the final eval and build statuses of an already
        terminal build for a new context; without this the context's
        nix-eval/nix-build checks stay pending forever. A succeeded
        build with zero attributes is a genuine empty-but-green eval,
        not an eval failure."""
        if build.status == BuildStatus.CANCELLED:
            await self.reporter.eval_cancelled(event, build)
        else:
            eval_success = build.status == BuildStatus.SUCCEEDED or bool(
                await self.db.get_attribute_statuses(build.id)
            )
            await self.reporter.eval_finished(
                event, build, success=eval_success, warnings=[]
            )
        await self.reporter.build_finished(
            event, build, build.status, build.status_generation, []
        )

    async def finish_linked(
        self,
        build: BuildRecord,
        status: str,
        generation: int,
        results: list[AttributeResult],
        *,
        eval_success: bool | None = None,
    ) -> None:
        """Final status fan-out for second contexts attached to this
        build; eval_success is None when no eval result exists."""
        for linked in self.linked_events.pop(build.id, []):
            if eval_success is not None:
                await self.reporter.eval_finished(
                    linked, build, success=eval_success, warnings=[]
                )
            elif status == BuildStatus.CANCELLED:
                # Cancel during eval: the linked contexts' nix-eval
                # status would otherwise stay pending forever.
                await self.reporter.eval_cancelled(linked, build)
            await self.reporter.build_finished(
                linked, build, status, generation, results
            )

    async def _is_ancestor(
        self, project_key: str, ancestor: str, descendant: str
    ) -> bool:
        try:
            await run_git(
                ["merge-base", "--is-ancestor", ancestor, descendant],
                cwd=self.repos.clone_path(project_key),
            )
        except GitError:
            return False
        return True

    async def _reuse_terminal_build(  # noqa: PLR0913
        self,
        event: ChangeEvent,
        build: BuildRecord,
        key: str,
        tree_hash: str,
        *,
        worktree_path: Path,
        credentials: FetchCredentials | None,
        incoming_stale: bool,
    ) -> None:
        """Same content already built in another context: only report
        the existing result for this context. Still register so an
        in-flight build of this context's previous content is
        superseded, and a push reusing a PR build gets its
        gcroots/outputs updates."""
        logger.info(
            "reusing build for tree hash",
            extra={"build_id": build.id, "tree_hash": tree_hash},
        )
        outcome = self.canceller.register(
            event.repo.id,
            key,
            build.id,
            tree_hash,
            event.commit_sha,
            asyncio.Event(),
            incoming_is_ancestor_of_running=incoming_stale,
        )
        if outcome == RegisterOutcome.STALE:
            # Redelivered out-of-order event: superseding the in-flight
            # newer build with this old result would cancel it.
            return
        self.canceller.complete(build.id)
        if build.status == BuildStatus.SUCCEEDED:
            # Guarded like in-build post-processing: a gcroots/outputs
            # failure must not strand this context without a status.
            try:
                await self._post_process_existing(event, build)
                # A build that ran as a PR never started effects, so a
                # default-branch push reusing it must still deploy; the
                # effects_started flag prevents re-deploys.
                await self.maybe_run_effects(event, build, worktree_path, credentials)
                await self.refresh_schedules(event)
            except Exception:
                logger.exception(
                    "post-processing reused build failed",
                    extra={"build_id": build.id},
                )
        await self._replay_terminal_status(event, build)

    async def _post_process_existing(
        self, event: ChangeEvent, build: BuildRecord
    ) -> None:
        """Gcroots/outputs updates for a context reusing an already
        succeeded build (e.g. default-branch push reusing a PR build)."""
        rows = await self.db.pool.fetch(
            "SELECT attr, outputs FROM build_attributes "
            "WHERE build_id = $1 AND status IN ('succeeded', 'skipped_local')",
            build.id,
        )
        pairs = []
        for row in rows:
            out = (json.loads(row["outputs"]) if row["outputs"] else {}).get("out")
            if out:
                pairs.append((row["attr"], out))
        await self.post_process_skipped(event, pairs)

    async def post_process_skipped(
        self, event: ChangeEvent, skipped: list[tuple[str, str]]
    ) -> None:
        branches = self.config.branches
        repo = event.repo
        if event.pr_number is not None:
            return  # push events only, matching current behavior
        for attr, out_path in skipped:
            if not out_path:
                continue
            # Forge-scoped paths: the same owner/repo on two forges
            # must not share gc-roots or outputs files.
            if branches.do_register_gcroot(repo.default_branch, event.branch):
                await self.register_gcroot(
                    self.config.gcroots_dir, repo.key, attr, out_path
                )
            if self.config.outputs_path is not None and branches.do_update_outputs(
                repo.default_branch, event.branch
            ):
                self.write_output_path(
                    self.config.outputs_path,
                    repo.forge,
                    repo.owner,
                    repo.repo,
                    event.branch,
                    attr,
                    out_path,
                )
