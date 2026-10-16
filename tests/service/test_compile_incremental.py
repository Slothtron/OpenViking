"""Core contracts for incremental and scheduled Compile."""

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.service.compile_scheduler import TZ, CompileScheduler, current_slot, is_due
from openviking.service.compile_targets import (
    CompileLifecycle,
    CompileStateStore,
    compress_scopes,
)
from openviking.service.external_task_service import ExternalTaskSnapshot
from openviking_cli.session.user_id import UserIdentifier
from tests.test_task_tracker import _FakeAgfs

CTX = RequestContext(user=UserIdentifier("acme", "alice"), role=Role.USER, api_key="key-1")
SRC, TO = "viking://resources/src", "viking://resources/wiki"
SKILL = "viking://user/alice/skills/wiki"


class _Agfs(_FakeAgfs):
    def ensure_parent_dirs(self, path, mode="755", *, ctx=None):
        return {}

    def pathlock_acquire_exact(self, *args, **kwargs):
        return {"lease_ref": "lease-1"}

    def pathlock_release(self, *args, **kwargs):
        return None


class _GitFs:
    """Fake VikingFS git: commits snapshot a dict of uri -> content."""

    def __init__(self):
        self.files = {}
        self.commits = []

    async def commit(self, *, message, paths, ctx):
        self.commits.append(dict(self.files))
        return {"commit_oid": f"{len(self.commits):040d}"}

    async def diff_paths(self, *, dirs, base, head, ctx):
        old = self.commits[int(base) - 1] if base else {}
        new = self.commits[int(head) - 1]
        out = []
        for uri in sorted(set(old) | set(new)):
            if not any(uri.startswith(f"{d}/") for d in dirs):
                continue
            if uri not in old:
                out.append({"uri": uri, "change_type": "added"})
            elif uri not in new:
                out.append({"uri": uri, "change_type": "deleted"})
            elif old[uri] != new[uri]:
                out.append({"uri": uri, "change_type": "modified"})
        return out

    async def exists(self, uri, ctx=None):
        return any(path.startswith(f"{uri}/") for path in self.files)

    async def show_blob_raw(self, target_ref, *, path, ctx=None):
        return {"oid": self.commits[int(target_ref) - 1][path]}


def _task(task_id, target_id, mode="incremental", instruction=None):
    request = {
        "from": [SRC],
        "to": TO,
        "skill": SKILL,
        "compile": {"target_id": target_id, "mode": mode, "trigger": "manual"},
    }
    if instruction:
        request["instruction"] = instruction
    return SimpleNamespace(
        task_id=task_id, account_id="acme", user_id="alice", meta={"request": request}
    )


def test_slots_follow_beijing_midnight_and_monday():
    now = datetime(2026, 10, 13, 9, 30, tzinfo=TZ)  # Tuesday
    assert current_slot("daily", now).isoformat() == "2026-10-13T00:00:00+08:00"
    assert current_slot("weekly", now).isoformat() == "2026-10-12T00:00:00+08:00"
    record = {
        "status": "active",
        "initialized": True,
        "auto_update": True,
        "auto_update_enabled_at": "2026-10-10T15:20:00+08:00",
        "last_triggered_slot": "2026-10-12T00:00:00+08:00",
    }
    assert is_due(record, current_slot("daily", now))
    assert not is_due(record, current_slot("weekly", now))
    # Enabling after the slot must not fire it retroactively.
    late = {**record, "auto_update_enabled_at": "2026-10-13T08:00:00+08:00"}
    assert not is_due(late, current_slot("daily", now))


def test_large_plans_fold_dense_directories():
    changes = [{"uri": f"{SRC}/d/{i}.md", "change_type": "modified"} for i in range(1500)]
    scopes = compress_scopes(changes, [])
    assert scopes == [
        {
            "uri": f"{SRC}/d",
            "change_type": "rescan",
            "recursive": True,
            "change_types": ["modified"],
        }
    ]


@pytest.mark.asyncio
async def test_incremental_lifecycle_tracks_progress_and_skips_unchanged_runs():
    store, fs = CompileStateStore(_Agfs()), _GitFs()
    lifecycle = CompileLifecycle(store, fs)
    fs.files = {f"{SRC}/a.md": "a", f"{SRC}/b.md": "b"}
    payload = await lifecycle.prepare(
        {"from": [SRC], "to": TO, "skill": SKILL}, CTX, instruction_set=False
    )
    target_id = payload["compile"]["target_id"]

    # First run: the whole source root is one recursive added scope.
    first = _task("cmp_1", target_id)
    assert await lifecycle.on_start(first) == "proceed"
    changes = await lifecycle.runtime_changes(first.meta["request"], "acme", "cmp_1")
    assert changes["scopes"] == [{"uri": SRC, "change_type": "added", "recursive": True}]
    fs.files[f"{TO}/page.md"] = "page"
    await lifecycle.on_finish(first, ExternalTaskSnapshot(status="completed"))
    record = await store.get("acme", target_id)
    assert record["initialized"] and record["active_task_id"] is None
    assert record["progress"]["processed_roots"] == [SRC]

    # Nothing changed and no instruction: finish without calling Runtime.
    idle = await lifecycle.on_start(_task("cmp_2", target_id))
    assert isinstance(idle, ExternalTaskSnapshot) and idle.status == "completed"
    await lifecycle.on_finish(_task("cmp_2", target_id), idle)

    # One modified file: only that file is in scope.
    fs.files[f"{SRC}/a.md"] = "a2"
    third = _task("cmp_3", target_id)
    assert await lifecycle.on_start(third) == "proceed"
    changes = await lifecycle.runtime_changes(third.meta["request"], "acme", "cmp_3")
    assert changes["scopes"] == [
        {"uri": f"{SRC}/a.md", "change_type": "modified", "recursive": False}
    ]
    await lifecycle.on_finish(third, ExternalTaskSnapshot(status="failed", error_message="x"))
    record = await store.get("acme", target_id)
    run = await store.get_run("acme", target_id, "cmp_3")
    # A failed run keeps the previous baseline and drops bulky scopes.
    assert record["progress"]["source_commit"] == changes["base"]
    assert run["status"] == "failed" and "scopes" not in run["changes"]


@pytest.mark.asyncio
async def test_target_is_serialized_across_instances():
    store, fs = CompileStateStore(_Agfs()), _GitFs()
    lifecycle = CompileLifecycle(store, fs)
    fs.files = {f"{SRC}/a.md": "a"}
    payload = await lifecycle.prepare(
        {"from": [SRC], "to": TO, "skill": SKILL}, CTX, instruction_set=False
    )
    target_id = payload["compile"]["target_id"]
    assert await lifecycle.on_start(_task("cmp_1", target_id)) == "proceed"
    await lifecycle.record_queued(_task("cmp_2", target_id))
    assert await lifecycle.on_start(_task("cmp_2", target_id)) == "busy"


class _Encryptor:
    async def encrypt(self, account_id, plaintext):
        return b"enc:" + plaintext

    async def decrypt(self, account_id, ciphertext):
        return ciphertext[len(b"enc:") :]


async def _scheduled_target(identities, ctx=CTX):
    """Register a daily target through a scheduler whose keys resolve via ``identities``."""
    store, fs = CompileStateStore(_Agfs()), _GitFs()
    fs.files = {f"{TO}/page.md": "page"}
    lifecycle = CompileLifecycle(store, fs)
    submitted = []

    async def create_scheduled(record, *, ctx, connection, idempotency_key):
        submitted.append((idempotency_key, connection, record["execution_context"]))

    scheduler = CompileScheduler(SimpleNamespace(create_scheduled=create_scheduled), store, fs)
    scheduler.credential_encryptor = _Encryptor()

    async def resolve(api_key):
        return identities[api_key]

    scheduler.set_identity_resolver(resolve)
    lifecycle.save_credential = scheduler.save_credential
    payload = await lifecycle.prepare(
        {
            "from": [SRC],
            "to": TO,
            "skill": SKILL,
            "args": {"auto_update": True, "update_frequency": "daily", "model_name": "m"},
        },
        ctx,
        instruction_set=False,
    )
    target_id = payload["compile"]["target_id"]
    async with store.edit("acme", target_id) as record:
        record["initialized"] = True
        record["auto_update_enabled_at"] = "2026-10-10T00:00:00+08:00"
    return store, scheduler, target_id, submitted


@pytest.mark.asyncio
async def test_scheduler_submits_once_per_slot_and_disables_invalid_keys():
    alice = SimpleNamespace(role=Role.USER, account_id="acme", user_id="alice")
    other = SimpleNamespace(role=Role.USER, account_id="acme", user_id="x")
    store, scheduler, target_id, submitted = await _scheduled_target(
        {"key-1": alice, "key-2": other}
    )
    now = datetime(2026, 10, 13, 1, 0, tzinfo=TZ)
    assert await scheduler.trigger_if_due("acme", target_id, now) == "submitted"
    assert await scheduler.trigger_if_due("acme", target_id, now) == "not_due"
    assert submitted[0][0] == f"compile-schedule:{target_id}:2026-10-13T00:00:00+08:00"
    assert submitted[0][1]["api_key"] == "key-1"
    assert submitted[0][2]["args"] == {"model_name": "m"}
    stored = json.dumps(await store.get("acme", target_id))
    assert "key-1" not in stored

    # A key that now resolves to another user disables the schedule next slot.
    async with store.edit("acme", target_id) as record:
        record["execution_context"]["credential"]["ciphertext"] = "ZW5jOnsiYXBpX2tleSI6ICJrZXktMiJ9"
    tomorrow = datetime(2026, 10, 14, 1, 0, tzinfo=TZ)
    assert await scheduler.trigger_if_due("acme", target_id, tomorrow) == "disabled"
    record = await store.get("acme", target_id)
    assert record["auto_update"] is False and record["last_error"]
    assert len(submitted) == 1


@pytest.mark.asyncio
async def test_trusted_root_key_runs_as_the_asserted_user():
    root = SimpleNamespace(role=Role.ROOT, account_id=None, user_id=None)
    trusted = RequestContext(user=UserIdentifier("acme", "alice"), role=Role.USER, api_key="root")
    store, scheduler, target_id, submitted = await _scheduled_target({"root": root}, trusted)
    executor = (await store.get("acme", target_id))["execution_context"]["executor"]
    assert executor == {"account_id": "acme", "user_id": "alice", "key_type": "root"}

    now = datetime(2026, 10, 13, 1, 0, tzinfo=TZ)
    assert await scheduler.trigger_if_due("acme", target_id, now) == "submitted"
    connection = submitted[0][1]
    assert (connection["account_id"], connection["user_id"]) == ("acme", "alice")


@pytest.mark.asyncio
async def test_enabling_schedule_fails_with_412_when_it_cannot_be_saved(tmp_path):
    from openviking.service.compile_scheduler import build_credential_encryptor
    from openviking_cli.exceptions import FailedPreconditionError
    from openviking_cli.utils.config.encryption_config import EncryptionConfig

    body = {"from": [SRC], "to": TO, "skill": SKILL, "args": {"auto_update": True}}

    # A read replica (no scheduler) refuses, so it never encrypts with its own key.
    lifecycle = CompileLifecycle(CompileStateStore(_Agfs()), _GitFs())
    with pytest.raises(FailedPreconditionError) as replica:
        await lifecycle.prepare(dict(body), CTX, instruction_set=False)
    assert replica.value.details["reason"] == "COMPILE_SCHEDULER_DISABLED"

    # An unwritable key directory is a configuration problem, not a server error.
    blocked = tmp_path / "ro"
    blocked.mkdir(mode=0o555)
    scheduler = CompileScheduler(SimpleNamespace(), lifecycle._store, lifecycle._fs)
    scheduler.credential_encryptor = build_credential_encryptor(
        EncryptionConfig(local={"key_file": str(blocked / ".openviking" / "master.key")})
    )
    lifecycle.save_credential = scheduler.save_credential
    with pytest.raises(FailedPreconditionError) as unwritable:
        await lifecycle.prepare(dict(body), CTX, instruction_set=False)
    assert unwritable.value.details["reason"] == "COMPILE_CREDENTIAL_KEY_UNAVAILABLE"
