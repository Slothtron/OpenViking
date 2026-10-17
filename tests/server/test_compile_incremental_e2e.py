"""End-to-end incremental Compile against the real ragfs binding and git.

Runtime is not called: the test drives the provider hooks the queue worker would
run, so it checks the kernel half (registry, snapshots, change plan, finish).
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.service.compile_scheduler import TZ, current_slot, next_slot
from openviking.service.compile_service import CompileRequest
from openviking.service.compile_targets import parse_schedule
from openviking.service.external_task_service import ExternalTaskSnapshot
from openviking.service.task_tracker import get_task_tracker
from openviking.storage.content_write import ContentWriteCoordinator
from openviking.storage.queuefs import get_queue_manager
from openviking_cli.session.user_id import UserIdentifier

pytest.importorskip("ragfs_python")

SRC = "viking://resources/compile_src"
TO = "viking://resources/compile_wiki"
SKILL = "viking://user/test_user/skills/wiki"


@pytest.fixture(autouse=True)
def _reindex_submission(service, monkeypatch):
    monkeypatch.setattr("openviking.server.dependencies.get_service", lambda: service)
    monkeypatch.setattr("openviking.service.reindex_executor.get_service", lambda: service)
    monkeypatch.setattr(service, "reindex", AsyncMock(return_value={"status": "accepted"}))


@pytest.fixture
async def compile_task(service):
    """Create a task with existing source/output files before its start snapshot."""
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri in (f"{SRC}/a.md", f"{TO}/page.md", f"{SKILL}/SKILL.md"):
        await _write(service, uri, "before", ctx)
    service.compile.configure_local_backend("http://127.0.0.1:1", "")
    task = await service.compile.create(
        CompileRequest.model_validate({"from": [SRC], "to": TO, "skill": SKILL}),
        connection={},
        ctx=ctx,
    )
    return ctx, task


async def _write(service, uri, text, ctx):
    await service.viking_fs.write_file(uri, text, ctx=ctx)


async def _run(service, ctx, *, write_page=None):
    compile_service = service.compile
    compile_service.configure_local_backend("http://127.0.0.1:1", "")
    request = CompileRequest.model_validate({"from": [SRC], "to": TO, "skill": SKILL})
    task = await compile_service.create(request, connection={}, ctx=ctx)
    task = await get_task_tracker().get(task.task_id, ctx.account_id, ctx.user.user_id)
    started = await compile_service.on_start(task)
    changes = None
    if started == "proceed":
        changes = await compile_service.lifecycle.runtime_changes(
            task.meta["request"], ctx.account_id, task.task_id
        )
        if write_page:
            await _write(service, f"{TO}/page.md", write_page, ctx)
        await compile_service.on_finish(task, ExternalTaskSnapshot(status="completed"))
    return started, changes


def test_schedule_slots():
    def at(day, hour, minute):
        return datetime(2026, 10, day, hour, minute, tzinfo=TZ)

    daily = parse_schedule({"frequency": "daily", "hour": 2, "minute": 30})
    assert current_slot(daily, at(13, 2, 29)) == at(12, 2, 30)
    assert current_slot(daily, at(13, 2, 30)) == at(13, 2, 30)
    assert next_slot(daily, at(13, 2, 30)) == at(14, 2, 30)
    # 2026-10-13 is a Tuesday; weekly defaults to Monday 00:00.
    assert current_slot(parse_schedule({"frequency": "weekly"}), at(13, 1, 0)) == at(12, 0, 0)
    sunday = parse_schedule({"frequency": "weekly", "weekday": 7, "hour": 23, "minute": 59})
    assert current_slot(sunday, at(13, 1, 0)) == at(11, 23, 59)
    assert next_slot(sunday, at(13, 1, 0)) == at(18, 23, 59)


@pytest.mark.asyncio
async def test_incremental_compile_on_real_git(service):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.ROOT)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SRC}/b.md", "b"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)

    started, changes = await _run(service, ctx, write_page="v1")
    assert started == "proceed"
    assert changes["scopes"] == [{"uri": SRC, "change_type": "added", "recursive": True}]

    started, _ = await _run(service, ctx)
    assert isinstance(started, ExternalTaskSnapshot) and started.status == "completed"

    await _write(service, f"{SRC}/a.md", "a2", ctx)
    started, changes = await _run(service, ctx, write_page="v2")
    assert started == "proceed"
    assert changes["scopes"] == [
        {"uri": f"{SRC}/a.md", "change_type": "modified", "recursive": False}
    ]
    config = await service.compile.get_config(TO, ctx)
    assert config["initialized"] is True and config["last_run"]["status"] == "completed"
    runs = await service.compile.list_runs(TO, ctx, limit=10, cursor=None)
    assert [run["result"] for run in runs["runs"] if run["result"]][0]["updated"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("locked_uri", [f"{SRC}/a.md", f"{TO}/page.md"])
async def test_busy_start_requeues_until_path_unlocks(service, monkeypatch, locked_uri):
    """Source or target file locks defer the same task without submitting Runtime work."""
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri in (f"{SRC}/a.md", f"{TO}/page.md", f"{SKILL}/SKILL.md"):
        await _write(service, uri, "content", ctx)
    compile_service = service.compile
    compile_service.configure_local_backend("http://127.0.0.1:1", "")
    submit = AsyncMock(return_value="runtime-id")
    monkeypatch.setattr(compile_service, "submit", submit)
    monkeypatch.setattr(
        compile_service, "get", AsyncMock(return_value=ExternalTaskSnapshot(status="completed"))
    )
    task = await compile_service.create(
        CompileRequest.model_validate({"from": [SRC], "to": TO, "skill": SKILL}),
        connection={},
        ctx=ctx,
    )
    queue = get_queue_manager().get_queue("ExternalTask")
    tracker = get_task_tracker()
    agfs = service.viking_fs._async_agfs
    lease = await agfs.pathlock_acquire_exact(service.viking_fs._uri_to_path(locked_uri, ctx=ctx))
    try:
        for _ in range(2):
            await asyncio.wait_for(queue.dequeue(), timeout=2)
            current = await tracker.get(task.task_id, ctx.account_id, ctx.user.user_id)
            assert current.status == "pending" and current.error is None
            status = await queue.get_status()
            assert (status.pending, status.in_progress, status.requeue_count) == (1, 0, _ + 1)
            submit.assert_not_awaited()
    finally:
        await agfs.pathlock_release(lease)
    await queue.dequeue()
    assert (await tracker.get(task.task_id, ctx.account_id, ctx.user.user_id)).status == "completed"
    submit.assert_awaited_once()
    assert (await queue.get_status()).is_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["commit", "reindex", "git_disabled"])
async def test_finish_commits_then_reindexes_and_resumes(
    service, monkeypatch, compile_task, failure
):
    from openviking.pyagfs.exceptions import AGFSNotSupportedError

    ctx, task = compile_task
    compile_service = service.compile
    if failure == "git_disabled":
        monkeypatch.setattr(
            service.viking_fs, "commit", AsyncMock(side_effect=AGFSNotSupportedError("disabled"))
        )
    await compile_service.on_start(task)
    await _write(service, f"{TO}/page.md", "published", ctx)
    lifecycle = compile_service.lifecycle
    commit_spy = AsyncMock(wraps=lifecycle._commit)
    monkeypatch.setattr(lifecycle, "_commit", commit_spy)
    if failure == "commit":
        commit_spy.side_effect = OSError("commit failed")
    else:
        service.reindex.side_effect = OSError("reindex failed")
    snapshot = ExternalTaskSnapshot(status="completed", meta={"commit_oid": "ignored"})
    with pytest.raises(OSError):
        await compile_service.on_finish(task, snapshot)
    run = await lifecycle.store.get_run(
        ctx.account_id, task.meta["request"]["compile"]["target_id"], task.task_id
    )
    assert run["finished_at"] is None
    resumed = await compile_service.on_start(task)
    assert isinstance(resumed, ExternalTaskSnapshot) and resumed.status == "completed"
    if failure == "commit":
        service.reindex.assert_not_awaited()
    else:
        assert run["snapshot_committed"] is True
    commit_spy.side_effect = None
    service.reindex.side_effect = None
    await compile_service.on_finish(task, snapshot)
    await compile_service.on_finish(task, snapshot)
    assert commit_spy.await_count == (2 if failure == "commit" else 1)
    kwargs = service.reindex.await_args.kwargs
    assert {k: kwargs[k] for k in ("uri", "mode", "force", "recursive", "wait")} == {
        "uri": TO,
        "mode": "semantic_and_vectors",
        "force": False,
        "recursive": True,
        "wait": False,
    }
    run = (await compile_service.list_runs(TO, ctx, limit=1, cursor=None))["runs"][0]
    assert run["status"] == "completed" and run["reindex_task_id"] == kwargs["task_id"]
    if failure != "git_disabled":
        versions = await compile_service.file_versions(f"{TO}/page.md", ctx, limit=5, before=None)
        assert sum(v["source"] == "compile" for v in versions["versions"]) == 1


@pytest.mark.asyncio
async def test_finish_accepts_async_reindex_before_completing_task(
    service, monkeypatch, compile_task
):
    from openviking.service.core import OpenVikingService

    monkeypatch.setattr(service, "reindex", OpenVikingService.reindex.__get__(service))
    ctx, task = compile_task
    queue = get_queue_manager().get_queue("ExternalTask")
    delivery = await queue.dequeue_raw()
    await service.compile.on_start(task)
    await service.fs.batch_write(
        root_uri=TO,
        operations=[{"uri": f"{TO}/page.md", "content": "published"}],
        ctx=ctx,
        defer_processing=True,
    )
    assert await service.viking_fs.read_file(f"{TO}/page.md", ctx=ctx) == "published"
    await _write(service, f"{TO}/page.md", "published", ctx)
    identity = {"account_id": ctx.account_id, "user_id": ctx.user.user_id}
    tracker = get_task_tracker()
    await tracker.start(task.task_id, **identity)
    persist = service.compile.lifecycle.store.put_run

    async def fail_final_record(account, target, run):
        if run["finished_at"] is not None:
            raise OSError("final record interrupted")
        await persist(account, target, run)

    monkeypatch.setattr(service.compile.lifecycle.store, "put_run", fail_final_record)
    with pytest.raises(OSError, match="final record interrupted"):
        await service.compile._tasks._apply_snapshot(
            ExternalTaskSnapshot(status="completed"), task_id=task.task_id, **identity
        )
    assert (await tracker.get(task.task_id, **identity)).status == "running"
    monkeypatch.setattr(service.compile.lifecycle.store, "put_run", persist)
    await service.compile._tasks._apply_snapshot(
        ExternalTaskSnapshot(status="completed"), task_id=task.task_id, **identity
    )
    await queue.ack(delivery["id"], delivery)
    assert (await tracker.get(task.task_id, **identity)).status == "completed"
    run = (await service.compile.list_runs(TO, ctx, limit=1, cursor=None))["runs"][0]
    reindex = await tracker.get(run["reindex_task_id"], **identity)
    assert reindex.task_type == "admin_reindex" and reindex.resource_id == TO


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "cancelled"])
async def test_unsuccessful_compile_rolls_back_deferred_output(service, compile_task, status):
    ctx, task = compile_task
    await service.compile.on_start(task)
    await service.fs.batch_write(
        root_uri=TO,
        operations=[
            {"uri": f"{TO}/page.md", "content": "changed"},
            {"uri": f"{TO}/new.md", "content": "new"},
        ],
        ctx=ctx,
        defer_processing=True,
    )
    await service.compile.on_finish(task, ExternalTaskSnapshot(status=status))
    assert await service.viking_fs.read_file(f"{TO}/page.md", ctx=ctx) == "before"
    assert not await service.viking_fs.exists(f"{TO}/new.md", ctx=ctx)
    service.reindex.assert_not_awaited()


@pytest.mark.asyncio
async def test_versions_candidates_edit_and_single_file_restore(service, monkeypatch):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    page = f"{TO}/page.md"
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    await _run(service, ctx, write_page="v1")
    versions = await service.compile.file_versions(page, ctx, limit=20, before=None)
    assert [(v["version_id"], v["source"]) for v in versions["versions"]] == [("v1", "compile")]
    assert versions["versions"][0]["provenance"]["source_roots"] == [SRC]

    # The source change now targets only the output bound to that source.
    await _write(service, f"{SRC}/a.md", "a2", ctx)
    _, changes = await _run(service, ctx, write_page="v2")
    assert changes["candidate_output_scopes"] == [{"uri": page, "recursive": False}]

    expected = ("v3", "manual", b"edited")
    observed = []
    enqueue = ContentWriteCoordinator._enqueue_lock_coordinator

    async def check_version_before_refresh(self, **kwargs):
        """Refresh observes committed content and version."""
        if kwargs["root_uri"] == page:
            history = await service.compile.file_versions(page, ctx, limit=20, before=None)
            latest = history["versions"][0]
            assert (latest["version_id"], latest["source"]) == expected[:2]
            body = await service.viking_fs.show(
                latest["snapshot_id"], path=latest["snapshot_path"], ctx=ctx
            )
            assert body == expected[2]
            observed.append(latest["source"])
        return await enqueue(self, **kwargs)

    monkeypatch.setattr(
        ContentWriteCoordinator, "_enqueue_lock_coordinator", check_version_before_refresh
    )

    # A console edit is a manual version and makes the file self-bound.
    await service.fs.write(page, "edited", ctx, edit_source="manual", wait=True)
    versions = await service.compile.file_versions(page, ctx, limit=20, before=None)
    assert [(v["version_id"], v["source"]) for v in versions["versions"]][:2] == [
        ("v3", "manual"),
        ("v2", "compile"),
    ]
    await _write(service, f"{SRC}/a.md", "a3", ctx)
    _, changes = await _run(service, ctx)
    assert changes["candidate_output_scopes"] == []

    # Single-file restore writes v1 back as a new version; a stale expectation conflicts.
    from openviking_cli.exceptions import ConflictError

    with pytest.raises(ConflictError) as conflict:
        await service.compile.restore_file_version(page, "v1", "v2", ctx)
    assert conflict.value.details["resource"] == "VERSION_CONFLICT"
    assert await service.viking_fs.read_file(page, ctx=ctx) == "edited"
    expected = ("v4", "restore", b"v1")
    restored = await service.compile.restore_file_version(page, "v1", "v3", ctx)
    assert restored == {"uri": page, "version_id": "v4", "restored_from": "v1"}
    assert (await service.viking_fs.read_file(page, ctx=ctx)).strip() == "v1"
    await service.resources.wait_processed()

    unchanged = await service.compile.restore_file_version(page, "v1", "v4", ctx)
    assert unchanged == {"uri": page, "version_id": "v4", "restored_from": None, "result": "noop"}
    await service.resources.wait_processed()
    versions = await service.compile.file_versions(page, ctx, limit=20, before=None)
    assert versions["current_version"] == "v4"
    assert len(versions["versions"]) == 4
    assert observed == ["manual", "restore"]


@pytest.mark.asyncio
async def test_writes_outside_compile_target_do_not_snapshot(service, monkeypatch):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    await _run(service, ctx, write_page="v1")

    async def unexpected_snapshot(*args, **kwargs):
        pytest.fail("Only writes inside an initialized Compile target create versions")

    monkeypatch.setattr(service.compile.lifecycle.versions, "_commit", unexpected_snapshot)
    for uri in [f"{SRC}/a.md", f"{TO}_sibling/page.md"]:
        await service.fs.write(uri, "outside target", ctx, wait=True)
        assert await service.viking_fs.read_file(uri, ctx=ctx) == "outside target"


@pytest.mark.asyncio
async def test_concurrent_restores_of_different_files_in_one_target(service):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    await _run(service, ctx, write_page="untouched")
    pages = [f"{TO}/concurrent-{index}.md" for index in range(5)]
    for index, uri in enumerate(pages):
        await service.fs.write(uri, f"old {index}", ctx, wait=True)
        await service.fs.write(uri, f"new {index}", ctx, wait=True)

    for source, expected, created, body in [
        ("v1", "v2", "v3", "old"),
        ("v2", "v3", "v4", "new"),
    ]:
        results = await asyncio.gather(
            *(service.compile.restore_file_version(uri, source, expected, ctx) for uri in pages),
            return_exceptions=True,
        )
        assert results == [
            {"uri": uri, "version_id": created, "restored_from": source} for uri in pages
        ]
        await service.resources.wait_processed()
        for index, uri in enumerate(pages):
            content = f"{body} {index}"
            assert await service.viking_fs.read_file(uri, ctx=ctx) == content
            versions = await service.compile.file_versions(uri, ctx, limit=20, before=None)
            assert len(versions["versions"]) == int(created[1:])
            latest = versions["versions"][0]
            assert (latest["version_id"], latest["source"]) == (created, "restore")
            snapshot = await service.viking_fs.show(
                latest["snapshot_id"], path=latest["snapshot_path"], ctx=ctx
            )
            assert snapshot == content.encode()

    assert await service.viking_fs.read_file(f"{TO}/page.md", ctx=ctx) == "untouched"
    untouched = await service.compile.file_versions(f"{TO}/page.md", ctx, limit=20, before=None)
    assert untouched["current_version"] == "v1"


@pytest.mark.asyncio
async def test_moving_and_removing_target_keeps_registry_consistent(service):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.ROOT)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    await _run(service, ctx, write_page="v1")
    moved = "viking://resources/compile_moved"
    await service.fs.mv(TO, moved, ctx)
    versions = await service.compile.file_versions(f"{moved}/page.md", ctx, limit=5, before=None)
    assert versions["versions"][0]["snapshot_path"] == f"{TO}/page.md"

    await service.fs.rm(moved, ctx, recursive=True)
    assert await service.compile.list_configs(ctx) == []
    target_id = (await service.compile.lifecycle.store.list_summaries(ctx.account_id))[0]["id"]
    assert await service.compile.lifecycle.store.list_runs(ctx.account_id, target_id)
