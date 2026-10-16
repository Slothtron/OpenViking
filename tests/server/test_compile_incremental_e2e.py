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
from openviking.service.compile_service import CompileRequest, CompileSessionStatus
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
@pytest.mark.parametrize("git_enabled", [True, False])
async def test_finish_reuses_runtime_snapshot(service, monkeypatch, git_enabled):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    compile_service = service.compile
    compile_service.configure_local_backend("http://127.0.0.1:1", "")
    request = CompileRequest.model_validate({"from": [SRC], "to": TO, "skill": SKILL})
    task = await compile_service.create(request, connection={}, ctx=ctx)
    queue = get_queue_manager().get_queue("ExternalTask")
    delivery = await queue.dequeue_raw()
    task = await get_task_tracker().get(task.task_id, ctx.account_id, ctx.user.user_id)
    await compile_service.on_start(task)
    await _write(service, f"{TO}/page.md", "published", ctx)
    head = await compile_service.lifecycle._commit([TO], "publish", ctx)
    await _write(service, f"{TO}/page.md", "later write", ctx)
    await compile_service.lifecycle._commit([TO], "later write", ctx)

    async def unexpected_commit(*args, **kwargs):
        pytest.fail("Finish must reuse the published snapshot, including disabled Git")

    monkeypatch.setattr(compile_service.lifecycle, "_commit", unexpected_commit)
    snapshot = compile_service._snapshot(
        CompileSessionStatus(
            status="completed",
            stage="completed",
            meta={"commit_oid": head if git_enabled else None},
        )
    )
    identity = {"account_id": ctx.account_id, "user_id": ctx.user.user_id}
    await get_task_tracker().start(task.task_id, **identity)
    assert await compile_service._tasks._apply_snapshot(snapshot, task_id=task.task_id, **identity)
    await queue.ack(delivery["id"], delivery)
    assert (await get_task_tracker().get(task.task_id, **identity)).status == "completed"
    run = await compile_service.lifecycle.store.get_run(
        ctx.account_id, task.meta["request"]["compile"]["target_id"], task.task_id
    )
    assert run["target_head"] == (head if git_enabled else None)
    assert run["status"] == "completed"
    if git_enabled:
        versions = await compile_service.file_versions(f"{TO}/page.md", ctx, limit=5, before=None)
        assert versions["versions"][0]["snapshot_id"] == head


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
async def test_concurrent_folder_batches_snapshot_before_refresh(service, monkeypatch):
    from openviking.pyagfs.exceptions import AGFSNotFoundError

    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await _write(service, uri, text, ctx)
    await _run(service, ctx, write_page="unchanged")
    sidecars = [
        f"{TO}/{name}"
        for name in (".abstract.md", ".overview.md", "page.md.abstract.md", "page.md.overview.md")
    ]
    for uri in sidecars:
        await _write(service, uri, "derived", ctx)
    snapshots = {}
    vfs = service.viking_fs
    commit = vfs.commit
    enqueue = ContentWriteCoordinator._enqueue_lock_coordinator

    async def capture_snapshot(**kwargs):
        result = await commit(**kwargs)
        snapshots.setdefault(asyncio.current_task(), []).append(result["commit_oid"])
        return result

    async def check_before_refresh(self, **kwargs):
        oid = snapshots[asyncio.current_task()][0]
        covered = kwargs["lock_handoff"]["covered_paths"]
        assert len(covered) == 1 and covered[0]["kind"] == "exact"
        uri = vfs._path_to_uri(covered[0]["path"], ctx=ctx)
        history = await service.compile.file_versions(uri, ctx, limit=5, before=None)
        assert history["versions"][0]["snapshot_id"] == oid
        assert await vfs.show(oid, path=uri, ctx=ctx) == b"published"
        for sidecar in sidecars:
            with pytest.raises(AGFSNotFoundError):
                await vfs.show(oid, path=sidecar, ctx=ctx)
        return await enqueue(self, **kwargs)

    monkeypatch.setattr(vfs, "commit", capture_snapshot)
    monkeypatch.setattr(ContentWriteCoordinator, "_enqueue_lock_coordinator", check_before_refresh)
    pages = [f"{TO}/{name}.md" for name in ("a", "b")]
    batches = [
        service.fs.batch_write(
            root_uri=TO,
            operations=[{"uri": uri, "content": "published", "mode": "upsert"}],
            ctx=ctx,
            wait=True,
            commit_folder=True,
        )
        for uri in pages
    ]
    results = await asyncio.wait_for(asyncio.gather(*batches), timeout=30)
    assert len(snapshots) == 2 and all(len(oids) == 1 for oids in snapshots.values())
    for uri, result in zip(pages, results, strict=True):
        assert [result["commit_oid"]] in snapshots.values()
        assert await vfs.show(result["commit_oid"], path=uri, ctx=ctx) == b"published"
    await service.resources.wait_processed()


@pytest.mark.asyncio
@pytest.mark.parametrize("git_disabled", [False, True])
async def test_folder_snapshot_error_releases_locks(service, monkeypatch, git_disabled):
    from openviking.pyagfs.exceptions import AGFSNotSupportedError

    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.USER)
    page = f"{TO}/page.md"
    await _write(service, page, "before", ctx)
    error = AGFSNotSupportedError("disabled") if git_disabled else OSError("snapshot failed")
    monkeypatch.setattr(service.viking_fs, "commit", AsyncMock(side_effect=error))
    batch = service.fs.batch_write(
        root_uri=TO,
        operations=[{"uri": page, "content": "after", "mode": "upsert"}],
        ctx=ctx,
        commit_folder=True,
    )
    if git_disabled:
        assert (await batch)["commit_oid"] is None
    else:
        enqueue = AsyncMock()
        monkeypatch.setattr(ContentWriteCoordinator, "_enqueue_lock_coordinator", enqueue)
        with pytest.raises(OSError, match="snapshot failed"):
            await batch
        enqueue.assert_not_awaited()
    await _write(service, page, "lease released", ctx)


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
