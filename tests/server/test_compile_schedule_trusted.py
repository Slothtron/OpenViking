"""Scheduled Compile over HTTP in trusted mode, with encryption.enabled left off.

A trusted root key plus X-OpenViking-Account/User enables weekly updates, and
the scheduled run acts as that asserted user. AGFS content stays unencrypted.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from openviking.server.app import create_app
from openviking.server.auth.plugins import TrustedAuthPlugin
from openviking.server.config import ServerConfig
from openviking.server.dependencies import set_service
from openviking.server.identity import RequestContext, Role
from openviking.server.store_assembly import build_api_key_manager
from openviking.service.compile_scheduler import TZ, next_run_at
from openviking_cli.session.user_id import UserIdentifier

pytest.importorskip("ragfs_python")

ROOT = "trusted-root-key-for-compile"
SRC, TO = "viking://resources/sched_src", "viking://resources/sched_wiki"
SKILL = "viking://user/alice/skills/wiki"


@pytest.mark.asyncio
async def test_trusted_root_key_enables_and_triggers_schedule(service, tmp_path, monkeypatch):
    assert service._encryptor is None  # encryption.enabled stays false
    scheduler = service.compile_scheduler
    await scheduler.stop()
    monkeypatch.setattr(scheduler.credential_encryptor.provider, "key_file", tmp_path / "k")
    service.compile.configure_local_backend("http://127.0.0.1:1", "")

    app = create_app(config=ServerConfig(auth_mode="trusted", root_api_key=ROOT), service=service)
    set_service(service)
    manager = build_api_key_manager(ROOT, service.viking_fs, issue_user_keys=False)
    await manager.load()
    app.state.api_key_manager = manager
    app.state.auth_plugin = TrustedAuthPlugin()
    scheduler.set_identity_resolver(manager.resolve_identity)

    alice = RequestContext(user=UserIdentifier("acct", "alice"), role=Role.ROOT)
    for uri, text in [(f"{SRC}/a.md", "a"), (f"{SKILL}/SKILL.md", "s")]:
        await service.viking_fs.write_file(uri, text, ctx=alice)

    headers = {"X-API-Key": ROOT, "X-OpenViking-Account": "acct", "X-OpenViking-User": "alice"}
    body = {
        "from": [SRC],
        "to": TO,
        "skill": SKILL,
        "instruction": "scheduled instruction",
        "args": {
            "auto_update": True,
            "update_schedule": {"frequency": "weekly", "weekday": 2, "hour": 8, "minute": 30},
        },
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        created = await client.post("/api/v1/compile", json=body, headers=headers)
        manual_body = {key: body[key] for key in ("from", "to", "skill")}
        for extra in ({"instruction": "once"}, {}):
            manual = await client.post(
                "/api/v1/compile", json={**manual_body, **extra}, headers=headers
            )
            assert manual.status_code == 202, manual.text
            payload = manual.json()["result"]["meta"]["request"]
            assert payload.get("instruction") == extra.get("instruction")
    assert created.status_code == 202, created.text
    await manager.close()

    store = service.compile.lifecycle.store
    record = await store.get_by_to("acct", TO)
    assert record["auto_update"] is True
    assert record["instruction"] == "scheduled instruction"
    assert record["execution_context"]["executor"] == {
        "account_id": "acct",
        "user_id": "alice",
        "key_type": "root",
    }
    assert ROOT not in str(record)

    submitted = []

    async def capture(record, *, ctx, connection, idempotency_key):
        submitted.append(connection)
        assert record["instruction"] == "scheduled instruction"

    monkeypatch.setattr(service.compile, "create_scheduled", capture)
    async with store.edit("acct", record["id"]) as record:
        record["initialized"] = True
        record["auto_update_enabled_at"] = "2026-10-10T00:00:00+08:00"
    await service.viking_fs.write_file(f"{TO}/page.md", "p", ctx=alice)
    # 2026-10-13 is a Tuesday; the slot is 08:30 that day.
    before = datetime(2026, 10, 13, 8, 29, tzinfo=TZ)
    assert await scheduler.trigger_if_due("acct", record["id"], before) == "not_due"
    now = datetime(2026, 10, 13, 8, 30, tzinfo=TZ)
    report = AsyncMock(side_effect=[ConnectionError("redis unavailable")] * 2 + [None])
    monkeypatch.setattr(scheduler, "_expiry", SimpleNamespace(report=report))
    for _ in range(2):
        with pytest.raises(ConnectionError):
            await scheduler.trigger_if_due("acct", record["id"], now)
    assert await scheduler.trigger_if_due("acct", record["id"], now) == "not_due"
    assert len(submitted) == 1 and report.await_count == 3
    report.side_effect = ConnectionError("redis unavailable")
    await scheduler.reschedule("acct", record)
    assert submitted[0] == {"api_key": ROOT, "account_id": "acct", "user_id": "alice"}
    record = await store.get("acct", record["id"])
    assert record["last_triggered_slot"] == "2026-10-13T08:30:00+08:00"
    assert next_run_at(record, now) == "2026-10-20T08:30:00+08:00"
