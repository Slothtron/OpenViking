# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Session privacy protects stored content and restores only at public reads."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from openviking.message import TextPart, ToolPart
from openviking.server.config import ToolOutputExternalizationConfig
from openviking.server.identity import RequestContext, Role
from openviking_cli.session.user_id import UserIdentifier


@pytest.fixture
def ctx():
    return RequestContext(user=UserIdentifier.the_default_user(), role=Role.ROOT)


@pytest.mark.parametrize(
    "extract,restore", [(True, False), (True, True), (False, False), (False, True)]
)
async def test_http_privacy_policy_controls_writes_and_reads(
    client, service, ctx, extract, restore
):
    sid = "http-privacy"
    policy = {"extract": {"enabled": extract}, "restore": {"enabled": restore}}
    payload = {"session_id": sid}
    if (extract, restore) != (True, False):
        payload["privacy_policy"] = policy
    response = await client.post("/api/v1/sessions", json=payload)
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert result["privacy_policy"] == policy
    uri = result["uri"] + "/messages.jsonl"
    # Restoration must also work for upstream references when extraction is off.
    reference = f"{{{{ov_privacy:session:{sid}:upstream}}}}"
    await service.privacy_configs.upsert(ctx, "session", sid, {"upstream": "upstream-secret"})
    response = await client.post(
        f"/api/v1/sessions/{sid}/messages/batch",
        json={
            "messages": [
                {"role": "user", "content": f"password=http-secret {reference}"},
                {
                    "role": "assistant",
                    "parts": [
                        {
                            "type": "tool",
                            "tool_id": "call-1",
                            "tool_status": "pending",
                            "tool_output": None,
                        }
                    ],
                },
            ]
        },
    )
    assert response.status_code == 200, response.text
    disk = await service.viking_fs.read_file(uri, ctx=ctx)
    assert ("http-secret" not in disk) == extract
    assert reference in disk and "upstream-secret" not in disk
    read = await client.get("/api/v1/content/read", params={"uri": uri})
    assert read.status_code == 200, read.text
    public = read.json()["result"]
    assert ("upstream-secret" in public) == restore
    assert ("http-secret" in public) == (not extract or restore)
    internal = await client.get(f"/api/v1/sessions/{sid}/context")
    assert internal.status_code == 200, internal.text
    assert "upstream-secret" not in internal.text
    current = (await client.get(f"/api/v1/privacy-configs/session/{sid}")).json()["result"]
    assert set(current["current"]["values"].values()) == (
        {"upstream-secret", "http-secret"} if extract else {"upstream-secret"}
    )
    assert await service.viking_fs.read_file(uri, ctx=ctx) == disk
    # Memory references may outlive the Session itself.
    assert (await client.delete(f"/api/v1/sessions/{sid}")).status_code == 200
    assert (await client.get(f"/api/v1/privacy-configs/session/{sid}")).status_code == 200


async def test_restore_externalized_output_without_changing_stored_messages(service, ctx):
    session = await service.sessions.create(
        ctx, "externalized", privacy_policy={"restore": {"enabled": True}}
    )
    session._tool_output_externalization_config = ToolOutputExternalizationConfig(
        enabled=True, threshold_chars=1024, preview_chars=200
    )
    output = "padding\n" * 1500 + "password=output-secret"
    await session.add_message_async("assistant", [ToolPart(tool_id="read", tool_output=output)])
    ref = session.messages[0].parts[0].tool_output_ref
    assert ref
    assert "output-secret" not in await service.viking_fs.read_file(ref + "/output.txt", ctx=ctx)
    assert await service.fs.read(ref + "/output.txt", ctx=ctx) == output
    reloaded = await service.sessions.get(session.session_id, ctx)
    assert "output-secret" not in json.dumps([message.to_dict() for message in reloaded.messages])


async def test_unloaded_session_uses_creation_time_extraction_opt_out(service, ctx):
    await service.sessions.create(ctx, "opt-out", privacy_policy={"extract": {"enabled": False}})
    session = service.sessions.session(ctx, "opt-out")
    await session.add_message_async("user", [TextPart("password=plain-secret")])
    assert "plain-secret" in await service.viking_fs.read_file(
        session.uri + "/messages.jsonl", ctx=ctx
    )
    assert await service.privacy_configs.get_current(ctx, "session", "opt-out") is None


@pytest.mark.parametrize("legacy", [False, True], ids=["auto-created", "legacy-meta"])
async def test_sessions_without_policy_use_defaults(service, ctx, legacy):
    if legacy:
        session = await service.sessions.create(ctx, "legacy")
        uri = session.uri + "/.meta.json"
        meta = json.loads(await service.viking_fs.read_file(uri, ctx=ctx))
        meta.pop("privacy_policy", None)
        await service.viking_fs.write_file(uri, json.dumps(meta), ctx=ctx)
        session = service.sessions.session(ctx, "legacy")
    else:
        session = await service.sessions.get("auto-created", ctx, auto_create=True)
    await session.add_message_async("user", [TextPart("password=default-secret")])
    uri = session.uri + "/messages.jsonl"
    disk = await service.viking_fs.read_file(uri, ctx=ctx)
    assert "default-secret" not in disk
    current = await service.privacy_configs.get_current(ctx, "session", session.session_id)
    assert list(current.values.values()) == ["default-secret"]
    assert await service.fs.read(uri, ctx=ctx) == disk


async def test_concurrent_writers_keep_all_messages_and_secrets(service, ctx):
    await service.sessions.create(ctx, "concurrent")
    await asyncio.gather(
        *(
            service.sessions.session(ctx, "concurrent").add_message_async(
                "user", [TextPart(f"password=secret-{i}")]
            )
            for i in range(8)
        )
    )
    loaded = await service.sessions.get("concurrent", ctx)
    current = await service.privacy_configs.get_current(ctx, "session", "concurrent")
    assert len(loaded.messages) == 8
    assert set(current.values.values()) == {f"secret-{i}" for i in range(8)}


async def test_privacy_storage_failure_rejects_batch_before_externalization(
    service, ctx, monkeypatch
):
    session = await service.sessions.create(ctx, "failure")
    monkeypatch.setattr(
        service.privacy_configs,
        "merge_values",
        AsyncMock(side_effect=OSError("storage unavailable")),
    )
    externalize = AsyncMock()
    monkeypatch.setattr(session._tool_outputs, "externalize_group", externalize)
    with pytest.raises(OSError, match="storage unavailable"):
        await session.add_message_async("user", [TextPart("password=secret")])
    externalize.assert_not_awaited()
    assert await service.viking_fs.read_file(session.uri + "/messages.jsonl", ctx=ctx) == ""
    assert await service.privacy_configs.get_current(ctx, "session", session.session_id) is None


@pytest.mark.parametrize(
    "policy",
    [
        [],
        {"mode": "rule"},
        {"extract": True},
        {"restore": {"enabled": True, "scope": "all"}},
        {"extract": {"enabled": "false"}},
        {"restore": {"enabled": 1}},
    ],
)
async def test_http_rejects_invalid_policy_without_creating_session(client, policy):
    response = await client.post(
        "/api/v1/sessions", json={"session_id": "invalid", "privacy_policy": policy}
    )
    assert response.status_code == 400, response.text
    assert (await client.get("/api/v1/sessions/invalid")).status_code == 404
