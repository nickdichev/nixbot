"""Build execution: evaluation, attribute scheduling, result
persistence, and the scheduler executor adapter.

Calls back into other concerns only via Orchestrator methods, which
keeps the module dependency graph acyclic.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shutil
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .db import BuildStatus
from .executor import failure_excerpt
from .live_warnings import LiveWarningAggregator
from .memory import calculate_eval_workers
from .models import NixEvalJobSuccess
from .nix_eval import EvalError, EvalSettings
from .post_build import build_props, run_post_build_steps
from .repo_config import BranchConfig
from .scheduler import (
    AttributeResult,
    AttributeStatus,
    BuildOutcome,
    JobScheduler,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from .db import BuildRecord
    from .events import ChangeEvent
    from .gitrepo import FetchCredentials
    from .models import NixEvalJob
    from .orchestrator import Orchestrator
    from .scheduler import CachedFailure, FailedBuildCache

logger = logging.getLogger(__name__)

LIVE_WARNINGS_FLUSH_INTERVAL = 2.0


async def run_build(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    worktree_path: Path,
    credentials: FetchCredentials | None = None,
) -> None:
    """Evaluate and build; every attribute completion is one
    transactional DB write, then the result is re-aggregated."""
    try:
        await _run_build_inner(o, event, build, worktree_path, credentials)
    except Exception as e:
        # Catch-all: a DB outage or GitError mid-eval would
        # otherwise wedge the build in 'evaluating' with no
        # terminal forge status.
        if isinstance(e, EvalError):
            logger.warning(
                "evaluation failed",
                extra={"build_id": build.id, "error": str(e)},
            )
        else:
            logger.exception(
                "build failed with unexpected error", extra={"build_id": build.id}
            )
        # Skip settling when the final fan-out already happened
        # (e.g. the effects phase failed): the build's aggregated
        # result must not be overwritten with a failure.
        current = await o.db.get_build(build.id)
        if current is None or current.status not in BuildStatus.TERMINAL:
            await _settle_aborted(o, event, build, BuildStatus.FAILED, error=str(e))
    finally:
        # Eval gc-roots only need to outlive the build; without
        # cleanup the nix store grows unboundedly.
        shutil.rmtree(o.gcroots_dir(build), ignore_errors=True)


def _eval_settings(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    credentials: FetchCredentials | None,
) -> EvalSettings:
    # Auto-sized workers come with a matching per-worker memory
    # limit; the configured limit acts as a ceiling. An explicit
    # worker count keeps the configured limit as-is.
    if o.config.eval_worker_count:
        worker_count = o.config.eval_worker_count
        eval_max_memory = o.config.eval_max_memory_size
    else:
        worker_config = calculate_eval_workers()
        worker_count = worker_config.count
        eval_max_memory = min(
            o.config.eval_max_memory_size, worker_config.max_memory_mib
        )
    # PR-controlled eval can fetch arbitrary flake inputs with the
    # netrc; an instance-wide token (Gitea/GitLab) would let a
    # malicious PR read any private repo on the forge. Only
    # repo-scoped tokens (GitHub) reach PR evals.
    netrc_file = None
    if credentials is not None and (event.pr_number is None or credentials.repo_scoped):
        netrc_file = credentials.netrc_file
    return EvalSettings(
        gc_roots_dir=o.gcroots_dir(build),
        timeout=o.config.eval_timeout,
        worker_count=worker_count,
        max_memory_size_mib=eval_max_memory,
        show_trace=o.config.show_trace_on_failure,
        netrc_file=netrc_file,
        # The worktree's .git points into the central clone; the
        # sandboxed evaluator needs to read it.
        extra_ro_paths=[o.repos.clone_path(event.repo.key)],
    )


async def _settle_aborted(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    status: str,
    *,
    error: str | None = None,
) -> None:
    """Terminal bookkeeping and status fan-out for a build that
    ended without a normal aggregation (failure or cancellation)."""
    await o.db.settle_unfinished_attributes(build.id)
    await o.db.set_build_status(build.id, status, error=error)
    if status == BuildStatus.CANCELLED:
        await o.reporter.eval_cancelled(event, build)
    else:
        await o.reporter.eval_finished(event, build, success=False, warnings=[])
    await o.reporter.build_finished(event, build, status, build.status_generation, [])
    await o.finish_linked(
        build,
        status,
        build.status_generation,
        [],
        eval_success=None if status == BuildStatus.CANCELLED else False,
    )


async def _reap(*tasks: asyncio.Future[Any]) -> None:
    """Cancel and await tasks, swallowing their errors."""
    for task in tasks:
        if not task.done():
            task.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await task


async def _run_build_inner(
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    worktree_path: Path,
    credentials: FetchCredentials | None,
) -> None:
    await o.reporter.build_started(event, build)
    await o.db.set_build_status(build.id, BuildStatus.EVALUATING)

    branch_config = BranchConfig.load(worktree_path)
    eval_settings = _eval_settings(o, event, build, credentials)
    # Race the evaluation against the cancel event: a superseded
    # build must not hold the eval slot to completion.
    cancel_event = o.cancel_events.setdefault(build.id, asyncio.Event())

    jobs_queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()

    async def record_job_batch(jobs: list[NixEvalJob]) -> None:
        # Pending rows appear in the UI while the eval is running.
        await o.db.record_attributes(
            build.id,
            [
                job
                for job in jobs
                if isinstance(job, NixEvalJobSuccess)
                and job.system in o.config.build_systems
            ],
        )
        await jobs_queue.put(jobs)

    # Deduplicated warnings appear on the build page while the eval
    # runs; DB writes are throttled since retry storms emit one
    # line per narinfo.
    live_warnings = LiveWarningAggregator()
    last_flush = 0.0

    async def record_stderr_line(line: str) -> None:
        nonlocal last_flush
        if not live_warnings.add(line):
            return
        now = time.monotonic()
        if now - last_flush >= LIVE_WARNINGS_FLUSH_INTERVAL:
            last_flush = now
            await o.db.set_eval_warnings(build.id, json.dumps(live_warnings.snapshot()))

    eval_task = asyncio.ensure_future(
        o.eval_runner.run(
            worktree_path,
            branch_config,
            eval_settings,
            on_jobs=record_job_batch,
            on_stderr_line=record_stderr_line,
        )
    )
    # Builds start as soon as the first eval batch arrives.
    build_task = asyncio.create_task(
        build_attributes(o, event, build, worktree_path, jobs_queue)
    )
    cancel_wait = asyncio.ensure_future(cancel_event.wait())
    try:
        try:
            await asyncio.wait(
                {eval_task, cancel_wait}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            cancel_wait.cancel()
        if cancel_event.is_set() and not eval_task.done():
            await _reap(eval_task, build_task)
            await _settle_aborted(o, event, build, BuildStatus.CANCELLED)
            return
        # EvalError and anything else propagate to run_build,
        # which settles the build as failed. Flush warnings dropped
        # by the throttle either way (run_build settles failures).
        try:
            eval_result = await eval_task
        finally:
            if live_warnings:
                await o.db.set_eval_warnings(
                    build.id, json.dumps(live_warnings.snapshot())
                )
        await o.db.set_build_status(build.id, BuildStatus.BUILDING)
        # Idempotent backstop for the streaming inserts above;
        # pending rows are what crash recovery resumes from. The
        # scheduler drops unsupported systems; their pending rows
        # would never turn terminal, so don't record them.
        await o.db.record_attributes(
            build.id,
            [
                job
                for job in eval_result.jobs
                if isinstance(job, NixEvalJobSuccess)
                and job.system in o.config.build_systems
            ],
        )
        await o.reporter.eval_finished(
            event,
            build,
            success=True,
            warnings=[str(g["message"]) for g in live_warnings.snapshot()],
        )

        # Re-send the complete eval result: the scheduler dedupes
        # by attr, so this only schedules jobs a streamed batch
        # missed (e.g. eval runners without on_jobs support).
        await jobs_queue.put(list(eval_result.jobs))
        await jobs_queue.put(None)
        status = await build_task
    except BaseException:
        # Reap both tasks or the build task leaks forever blocked
        # on the jobs queue and the evaluator process outlives the
        # build (nix_eval kills the evaluator on cancellation).
        await _reap(eval_task, build_task)
        raise

    if status == BuildStatus.SUCCEEDED:
        await o.maybe_run_effects(event, build, worktree_path, credentials)


async def build_attributes(  # noqa: PLR0913
    o: Orchestrator,
    event: ChangeEvent,
    build: BuildRecord,
    worktree_path: Path,
    jobs: Sequence[NixEvalJob] | asyncio.Queue[list[NixEvalJob] | None],
    *,
    cache_failures: bool = True,
) -> str:
    """Schedule the attribute builds, persist their results, and
    re-aggregate the build (shared by fresh builds and reruns).
    Accepts either a complete job list or a queue fed during an
    ongoing evaluation. Returns the aggregated build status."""
    cancel_event = o.cancel_events.setdefault(build.id, asyncio.Event())

    async def record_early(result: AttributeResult) -> None:
        """Persist skips and dependency failures as they happen;
        otherwise they stay pending until the whole build ends."""
        await o.db.complete_attribute(build.id, result, if_unfinished=True)

    failed_build_cache: FailedBuildCache | None = (
        o.failed_build_cache(build.project_id)
        if o.failed_build_cache is not None and o.config.cache_failed_builds
        else None
    )
    if failed_build_cache is not None and not cache_failures:
        failed_build_cache = _ReadOnlyFailedBuildCache(failed_build_cache)
    scheduler = JobScheduler(
        _OrchestratorExecutor(o, event, build, worktree_path, cancel_event),
        o.config.build_systems,
        failed_build_cache=failed_build_cache,
        build_url=f"{o.config.url}/repos/{event.repo.forge}/{event.repo.name}/builds/{build.number}",
        on_result=record_early,
    )
    if isinstance(jobs, asyncio.Queue):
        schedule_result = await scheduler.run_incremental(jobs)
    else:
        schedule_result = await scheduler.run(list(jobs))

    # Persist results the executor adapter didn't already write
    # (failed_eval, dependency_failed, cached_failure, skips).
    for result in schedule_result.results:
        await o.db.complete_attribute(build.id, result, if_unfinished=True)

    # Skipped-as-local attributes still get gcroots/outputs
    # updates. A filesystem error here must not skip the final
    # aggregation and status fan-out below.
    post_process_error: str | None = None
    try:
        await o.post_process_skipped(event, schedule_result.skipped_out_paths)
    except Exception as e:
        logger.exception(
            "post-processing skipped attributes failed",
            extra={"build_id": build.id},
        )
        post_process_error = str(e)

    status, generation = await o.db.aggregate_build(build.id)
    if post_process_error is not None:
        status = BuildStatus.FAILED
        await o.db.set_build_status(
            build.id, BuildStatus.FAILED, error=post_process_error
        )
    await o.reporter.build_finished(
        event,
        build,
        status,
        generation,
        schedule_result.results,
        attr_statuses=await o.db.get_attribute_statuses(build.id),
        attr_prefix=BranchConfig.load(worktree_path).attribute,
    )
    await o.finish_linked(
        build, status, generation, schedule_result.results, eval_success=True
    )
    o.cancel_events.pop(build.id, None)

    if status == BuildStatus.SUCCEEDED:
        await o.refresh_schedules(event)
    return status


class _ReadOnlyFailedBuildCache:
    """Failed-build cache that skips known failures but records none.

    Recovery/restart reruns rebuild jobs from DB rows without dependency
    closures, so dependents of one broken drv fail with their own build
    error; recording those would poison the cache."""

    def __init__(self, inner: FailedBuildCache) -> None:
        self._inner = inner

    async def check(self, drv_path: str) -> CachedFailure | None:
        return await self._inner.check(drv_path)

    async def add(self, drv_path: str, url: str) -> None:
        pass


class _OrchestratorExecutor:
    """Scheduler executor adapter: runs the build, then post-build
    steps, gcroots, outputs, and writes the attribute completion as one
    transactional write."""

    def __init__(
        self,
        orchestrator: Orchestrator,
        event: ChangeEvent,
        build: BuildRecord,
        worktree_path: Path,
        cancel_event: asyncio.Event,
    ) -> None:
        self.o = orchestrator
        self.event = event
        self.build_record = build
        self.worktree_path = worktree_path
        self.cancel_event = cancel_event

    async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
        try:
            return await self._build_inner(job)
        except Exception:
            logger.exception(
                "unexpected error building attribute",
                extra={"build_id": self.build_record.id, "attr": job.attr},
            )
            result = AttributeResult(
                attr=job.attr,
                status=AttributeStatus.failed,
                job=job,
                error="internal error, see service logs",
                drv_path=job.drv_path,
                system=job.system,
            )
            await self.o.db.complete_attribute(self.build_record.id, result)
            # Internal errors are not derivation failures: don't cache.
            return BuildOutcome.failure_no_cache

    async def _build_inner(self, job: NixEvalJobSuccess) -> BuildOutcome:
        if not await self.o.db.mark_attribute_building(
            self.build_record.id, job.attr, job.system, job.drv_path
        ):
            # Cancelled externally while waiting on dependencies: do
            # not resurrect the row by building it anyway.
            return BuildOutcome.cancelled
        # Per-attribute cancellation: the executor watches one event, so
        # mirror the build-level cancel into the attribute's own event.
        attr_cancel = asyncio.Event()
        self.o.attr_cancel_events[(self.build_record.id, job.attr)] = attr_cancel

        async def _mirror_build_cancel() -> None:
            await self.cancel_event.wait()
            attr_cancel.set()

        mirror = asyncio.create_task(_mirror_build_cancel())
        # Attribute names come from untrusted flakes; percent-encode
        # so the log file cannot escape the log directory.
        try:
            async with self.o.open_log(
                self.build_record.id, job.attr, f"{quote(job.attr, safe='')}.zst"
            ) as writer:
                outcome = await self.o.executor.build_attribute(
                    self.build_record.id,
                    job,
                    writer,
                    self.worktree_path,
                    attr_cancel,
                )
                if outcome == BuildOutcome.success and self.o.config.post_build_steps:
                    props = build_props(self.event, job)
                    step_results = await run_post_build_steps(
                        self.o.config.post_build_steps, props, self.worktree_path
                    )
                    for step in step_results:
                        await writer.write(
                            f"\npost-build step {step.name}: "
                            f"{'ok' if step.success else 'failed'}\n".encode()
                        )
                        await writer.write(step.output.encode())
                    if any(step.failed for step in step_results):
                        # The derivation built: fail the attribute without
                        # poisoning the failed-build cache.
                        outcome = BuildOutcome.post_build_failure
        finally:
            mirror.cancel()
            self.o.attr_cancel_events.pop((self.build_record.id, job.attr), None)

        status = {
            BuildOutcome.success: AttributeStatus.succeeded,
            BuildOutcome.failure: AttributeStatus.failed,
            BuildOutcome.failure_no_cache: AttributeStatus.failed,
            BuildOutcome.post_build_failure: AttributeStatus.failed,
            BuildOutcome.cancelled: AttributeStatus.cancelled,
        }[outcome]
        # Failed attributes carry a log-tail excerpt so the build page
        # answers "why" without a click into the log. ANSI stays: the
        # web layer renders it, the API strips it.
        error = None
        if status == AttributeStatus.failed:
            error = failure_excerpt(writer.tail_lines()) or None
        result = AttributeResult(
            attr=job.attr,
            status=status,
            job=job,
            error=error,
            out_path=job.outputs.get("out"),
            drv_path=job.drv_path,
            system=job.system,
        )
        await self.o.db.complete_attribute(
            self.build_record.id,
            result,
            log_path=str(writer.path.relative_to(self.o.config.state_dir)),
            log_size=writer.bytes_seen,
            log_truncated=writer.truncated,
        )
        if outcome == BuildOutcome.success:
            try:
                await self.o.post_process_skipped(
                    self.event, [(job.attr, job.outputs.get("out") or "")]
                )
            except Exception:
                # Must not overwrite the recorded success or poison
                # the failed-build cache.
                logger.exception(
                    "post-processing failed",
                    extra={"build_id": self.build_record.id, "attr": job.attr},
                )
        return outcome
