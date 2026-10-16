"""End-to-end incremental Compile against the real ragfs binding and git.

Runtime is not called: the test drives the provider hooks the queue worker would
run, so it checks the kernel half (registry, snapshots, change plan, finish).
"""

from datetime import datetime

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.service.compile_scheduler import TZ, current_slot, next_slot
from openviking.service.compile_service import CompileRequest
from openviking.service.compile_targets import parse_schedule
from openviking.service.external_task_service import ExternalTaskSnapshot
from openviking.service.task_tracker import get_task_tracker
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
async def test_versions_candidates_edit_and_single_file_restore(service):
    ctx = RequestContext(user=UserIdentifier("default", "test_user"), role=Role.ROOT)
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

    # A console edit is a manual version and makes the file self-bound.
    await service.fs.write(page, "edited", ctx, edit_source="manual")
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

    with pytest.raises(ConflictError):
        await service.compile.restore_file_version(page, "v1", "v2", ctx)
    restored = await service.compile.restore_file_version(page, "v1", "v3", ctx)
    assert restored == {"uri": page, "version_id": "v4", "restored_from": "v1"}
    assert (await service.viking_fs.read_file(page, ctx=ctx)).strip() == "v1"


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
