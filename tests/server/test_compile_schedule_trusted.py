"""Scheduled Compile over HTTP in trusted mode, with encryption.enabled left off.

A trusted root key plus X-OpenViking-Account/User enables daily updates, and
the scheduled run acts as that asserted user. AGFS content stays unencrypted.
"""

from datetime import datetime

import httpx
import pytest

from openviking.server.app import create_app
from openviking.server.auth.plugins import TrustedAuthPlugin
from openviking.server.config import ServerConfig
from openviking.server.dependencies import set_service
from openviking.server.identity import RequestContext, Role
from openviking.server.store_assembly import build_api_key_manager
from openviking.service.compile_scheduler import TZ
from openviking_cli.session.user_id import UserIdentifier

pytest.importorskip("ragfs_python")

ROOT = "trusted-root-key-for-compile"
SRC, TO = "viking://resources/sched_src", "viking://resources/sched_wiki"
SKILL = "viking://user/alice/skills/wiki"


@pytest.mark.asyncio
async def test_trusted_root_key_enables_and_triggers_schedule(service, tmp_path, monkeypatch):
    assert service._encryptor is None  # encryption.enabled stays false
    scheduler = service.compile_scheduler
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
        "args": {"auto_update": True, "update_frequency": "daily"},
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        created = await client.post("/api/v1/compile", json=body, headers=headers)
    assert created.status_code == 202, created.text
    await manager.close()

    store = service.compile.lifecycle.store
    record = await store.get_by_to("acct", TO)
    assert record["auto_update"] is True
    assert record["execution_context"]["executor"] == {
        "account_id": "acct",
        "user_id": "alice",
        "key_type": "root",
    }
    assert ROOT not in str(record)

    submitted = []

    async def capture(record, *, ctx, connection, idempotency_key):
        submitted.append(connection)

    monkeypatch.setattr(service.compile, "create_scheduled", capture)
    async with store.edit("acct", record["id"]) as record:
        record["initialized"] = True
        record["auto_update_enabled_at"] = "2026-10-10T00:00:00+08:00"
    await service.viking_fs.write_file(f"{TO}/page.md", "p", ctx=alice)
    now = datetime(2026, 10, 13, 1, 0, tzinfo=TZ)
    assert await scheduler.trigger_if_due("acct", record["id"], now) == "submitted"
    assert submitted[0] == {"api_key": ROOT, "account_id": "acct", "user_id": "alice"}
