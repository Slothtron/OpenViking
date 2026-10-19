# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Protect Session persistence and restore before visible-read sanitization."""

import asyncio
import json
from copy import deepcopy
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from openviking.message import ContextPart, ImagePart, TextPart, ToolPart
from openviking.privacy.reference import (
    build_privacy_field_reference,
    parse_privacy_field_reference,
)
from openviking.privacy.session_extractor import extract_and_store_session_privacy
from openviking.privacy.session_restore import restore_session_read
from openviking.server.config import ToolOutputExternalizationConfig
from openviking.server.identity import RequestContext, Role
from openviking_cli.exceptions import InvalidArgumentError, ResourceExhaustedError
from openviking_cli.session.user_id import UserIdentifier


@pytest.fixture
def ctx():
    return RequestContext(user=UserIdentifier.the_default_user(), role=Role.ROOT)


async def test_http_restores_before_visible_read_sanitization(client, service, ctx):
    sid = "http-privacy"
    response = await client.post("/api/v1/sessions", json={"session_id": sid})
    assert response.status_code == 200, response.text
    uri = response.json()["result"]["uri"] + "/messages.jsonl"
    reference = build_privacy_field_reference(ctx.user.user_id, "credential", "github", "pat")
    await service.privacy_configs.upsert(ctx, "credential", "github", {"pat": "registered-secret"})
    response = await client.post(
        f"/api/v1/sessions/{sid}/messages/batch",
        json={
            "messages": [
                {
                    "role": "user",
                    "content": "password=http-secret registered-secret repeated http-secret",
                },
                {
                    "role": "assistant",
                    "parts": [
                        {
                            "type": "tool",
                            "tool_id": "pending",
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
    assert "http-secret" not in disk and "registered-secret" not in disk
    assert reference in disk
    for raw in [False, True]:
        read = await client.get("/api/v1/content/read", params={"uri": uri, "raw": raw})
        assert read.status_code == 200, read.text
        public = read.json()["result"]
        assert ("http-secret" in public) == raw
        assert ("registered-secret" in public) == raw
        assert (reference in public) == (not raw)
        json.loads(public.splitlines()[1])  # Pending tool output remains valid JSON.
    internal = await client.get(f"/api/v1/sessions/{sid}/context")
    assert internal.status_code == 200, internal.text
    assert "registered-secret" not in internal.text and "http-secret" not in internal.text
    current = await service.privacy_configs.get_current(ctx, "session", sid)
    assert list(current.values.values()) == ["http-secret"]
    # References in historical messages follow the active credential value.
    await service.privacy_configs.upsert(ctx, "credential", "github", {"pat": "rotated-secret"})
    restored = await service.fs.read(uri, ctx=ctx)
    assert "rotated-secret" in restored and "registered-secret" not in restored
    visible = await service.fs.read_visible(uri, ctx=ctx)
    assert "rotated-secret" not in visible and reference in visible
    assert await service.viking_fs.read_file(uri, ctx=ctx) == disk
    assert (await client.delete(f"/api/v1/sessions/{sid}")).status_code == 200
    assert (await client.get(f"/api/v1/privacy-configs/session/{sid}")).status_code == 200


async def test_new_equal_fields_keep_independent_references_when_one_rotates(service, ctx):
    session = await service.sessions.create(ctx, "equal-new-fields")
    original = {"password": "same-new-value", "api_key": "same-new-value"}
    await session.add_message_async("assistant", [ToolPart(tool_input=original)])
    protected = session.messages[0].parts[0].tool_input
    assert protected["password"] != protected["api_key"]
    fields = {key: parse_privacy_field_reference(ref).field for key, ref in protected.items()}
    current = await service.privacy_configs.get_current(ctx, "session", session.session_id)
    assert current.values == dict.fromkeys(fields.values(), "same-new-value")
    await service.privacy_configs.patch_values(
        ctx, "session", session.session_id, {fields["password"]: "rotated-value"}
    )
    restored = json.loads(await service.fs.read(session.uri + "/messages.jsonl", ctx=ctx))
    assert restored["parts"][0]["tool_input"] == {
        "password": "rotated-value",
        "api_key": "same-new-value",
    }


@pytest.mark.parametrize("entrypoint", ["session", "skill"])
async def test_session_and_skill_require_explicit_reference_for_ambiguous_value(
    service, ctx, entrypoint
):
    await service.privacy_configs.upsert(
        ctx, "credential", "ambiguous", {"a": "same-registered-value", "b": "same-registered-value"}
    )
    with pytest.raises(InvalidArgumentError, match="mapped to multiple fields"):
        if entrypoint == "skill":
            await service._skill_processor.prepare_skill_privacy(
                {"name": "ambiguous", "content": "password=same-registered-value"}, ctx
            )
        else:
            session = await service.sessions.create(ctx, "ambiguous")
            await session.add_message_async("user", [TextPart("password=same-registered-value")])
    if entrypoint == "session":
        assert await service.viking_fs.read_file(session.uri + "/messages.jsonl", ctx=ctx) == ""
        assert await service.privacy_configs.get_current(ctx, "session", session.session_id) is None
        refs = [
            build_privacy_field_reference(ctx.user.user_id, "credential", "ambiguous", field)
            for field in ("a", "b")
        ]
        await session.add_message_async("user", [TextPart(" ".join(refs))])
        restored = json.loads(await service.fs.read(session.uri + "/messages.jsonl", ctx=ctx))
        assert restored["parts"][0]["text"] == "same-registered-value same-registered-value"


async def test_batch_discovery_protects_all_payloads_without_mutating_inputs(service, ctx):
    await service.privacy_configs.upsert(ctx, "credential", "github", {"pat": "registered-value"})
    fresh = "alpha-987654"
    credentials = {"api_key": "nested-value", "items": [False, None, "中文"]}
    specs = [
        {
            "role": "assistant",
            "peer_id": fresh,
            "parts": [
                TextPart(f"retry {fresh}; registered-value"),
                ContextPart(uri=f"viking://resources/{fresh}", abstract=fresh),
                ImagePart(url=f"https://example.test/{fresh}"),
                ToolPart(
                    tool_id=fresh,
                    tool_input={
                        fresh: fresh,
                        "rows": ({"note": fresh}, False, None),
                        "credentials": credentials,
                    },
                    tool_output=fresh,
                    tool_output_externalization_error=fresh,
                ),
                ToolPart(tool_id="pending", tool_output=None),
                TextPart(f"password={fresh}"),
            ],
        }
    ]
    before = deepcopy(specs)
    protected = await extract_and_store_session_privacy(
        "traversal", specs, ctx, service.privacy_configs, service._privacy_sanitizer
    )
    assert specs == before
    assert protected[0]["peer_id"] == fresh
    parts = protected[0]["parts"]
    assert parts[1].uri == specs[0]["parts"][1].uri
    assert parts[2].url == specs[0]["parts"][2].url
    assert parts[3].tool_id == fresh
    assert isinstance(parts[3].tool_input["rows"], tuple)
    current = await service.privacy_configs.get_current(ctx, "session", "traversal")
    assert len(current.values) == 2
    assert credentials in current.values.values()
    reference = next(
        build_privacy_field_reference(ctx.user.user_id, "session", "traversal", field)
        for field, value in current.values.items()
        if value == fresh
    )
    assert parts[0].text.startswith(f"retry {reference}; ")
    assert parts[1].abstract == reference
    assert parts[3].tool_input[reference] == reference
    assert parts[3].tool_input["rows"][0]["note"] == reference
    assert parts[3].tool_output == reference
    assert parts[3].tool_output_externalization_error == reference
    encoded = json.dumps({**protected[0], "parts": [asdict(part) for part in parts]})
    restored = await restore_session_read(
        service.privacy_configs,
        encoded,
        ctx,
        jsonl=True,
    )
    expected = json.dumps({**before[0], "parts": [asdict(part) for part in before[0]["parts"]]})
    assert json.loads(restored) == json.loads(expected)


async def test_batch_discovery_rejects_key_collision_before_message_write(
    service, ctx, monkeypatch
):
    from openviking.privacy import session_extractor

    fixed_id = "same0001"
    monkeypatch.setattr(session_extractor, "token_urlsafe", lambda _: fixed_id)
    session = await service.sessions.create(ctx, "collision")
    reference = build_privacy_field_reference(
        ctx.user.user_id, "session", session.session_id, fixed_id
    )
    part = ToolPart(
        tool_input={"alpha-987654": 1, reference: 2}, tool_output="password=alpha-987654"
    )
    before = deepcopy(part)
    externalize = AsyncMock()
    monkeypatch.setattr(session._tool_outputs, "externalize_group", externalize)
    with pytest.raises(InvalidArgumentError, match="object key collision"):
        await session.add_message_async("assistant", [part])
    externalize.assert_not_awaited()
    assert part == before
    assert await service.viking_fs.read_file(session.uri + "/messages.jsonl", ctx=ctx) == ""


async def test_externalized_output_restores_without_corrupting_metadata(service, ctx):
    session = await service.sessions.create(ctx, "externalized")
    session._tool_output_externalization_config = ToolOutputExternalizationConfig(
        enabled=True, threshold_chars=1024, preview_chars=200
    )
    output = "padding\n" * 1500 + "password=session"
    await session.add_message_async("assistant", [ToolPart(tool_id="read", tool_output=output)])
    part = session.messages[0].parts[0]
    ref = part.tool_output_ref
    assert ref
    stored = await service.viking_fs.read_file(ref + "/output.txt", ctx=ctx)
    assert "password=session" not in stored
    assert await service.fs.read(ref + "/output.txt", ctx=ctx) == output
    assert await service.fs.read_visible(ref + "/output.txt", ctx=ctx) == stored
    metadata = json.loads(await service.viking_fs.read_file(ref + "/metadata.json", ctx=ctx))
    assert metadata["session_id"] == session.session_id
    reloaded = await service.sessions.get(session.session_id, ctx)
    assert "password=session" not in json.dumps(
        [message.to_dict() for message in reloaded.messages]
    )


async def test_auto_created_session_extracts_before_persistence(service, ctx):
    session = await service.sessions.get("auto-created", ctx, auto_create=True)
    await session.add_message_async("user", [TextPart("password=default-secret")])
    uri = session.uri + "/messages.jsonl"
    disk = await service.viking_fs.read_file(uri, ctx=ctx)
    assert "default-secret" not in disk
    current = await service.privacy_configs.get_current(ctx, "session", session.session_id)
    assert list(current.values.values()) == ["default-secret"]
    assert "default-secret" in await service.fs.read(uri, ctx=ctx)


async def test_concurrent_writers_preserve_messages_and_independent_fields(
    service, ctx, monkeypatch
):
    from openviking.privacy import session_extractor

    ids = iter(["same0001"] * 8 + [f"next{i:04d}" for i in range(8)])
    monkeypatch.setattr(session_extractor, "token_urlsafe", lambda _: next(ids))
    session = await service.sessions.create(ctx, "concurrent")
    await service.privacy_configs.upsert(
        ctx, "session", session.session_id, {"same0001": "old-secret"}
    )
    old_reference = build_privacy_field_reference(
        ctx.user.user_id, "session", session.session_id, "same0001"
    )
    await session.add_message_async("user", [TextPart(old_reference)])
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
    assert set(current.values.values()) == {"old-secret", *(f"secret-{i}" for i in range(8))}
    assert current.version == 9
    restored = await service.fs.read(loaded.uri + "/messages.jsonl", ctx=ctx)
    assert sorted(json.loads(line)["parts"][0]["text"] for line in restored.splitlines()) == [
        "old-secret",
        *(f"password=secret-{i}" for i in range(8)),
    ]


@pytest.mark.parametrize(
    "error", [ResourceExhaustedError, OSError], ids=["id-exhaustion", "storage"]
)
async def test_privacy_write_failure_rejects_batch_before_externalization(
    service, ctx, monkeypatch, error
):
    from openviking.privacy import session_extractor

    session = await service.sessions.create(ctx, "privacy-write-failure")
    await service.privacy_configs.upsert(
        ctx, "session", session.session_id, {"taken001": "old-secret"}
    )
    if error is ResourceExhaustedError:
        monkeypatch.setattr(session_extractor, "token_urlsafe", lambda _: "taken001")
    else:
        monkeypatch.setattr(
            service.privacy_configs,
            "insert_values",
            AsyncMock(side_effect=OSError("storage unavailable")),
        )
    externalize = AsyncMock()
    monkeypatch.setattr(session._tool_outputs, "externalize_group", externalize)
    with pytest.raises(error):
        await session.add_message_async("user", [TextPart("password=new-secret")])
    externalize.assert_not_awaited()
    assert await service.viking_fs.read_file(session.uri + "/messages.jsonl", ctx=ctx) == ""
    current = await service.privacy_configs.get_current(ctx, "session", session.session_id)
    assert current.values == {"taken001": "old-secret"}
    assert await service.privacy_configs.list_versions(ctx, "session", session.session_id) == [1]


async def test_values_equal_to_json_keys_do_not_change_message_identity(service, ctx):
    session = await service.sessions.create(ctx, "identity")
    original = {"password": "tool_input", "token": "assistant", "secret": "default"}
    await session.add_message_async("assistant", [ToolPart(tool_input=original)])
    # A full rewrite takes a different storage path than append.
    await session._write_to_agfs_async(session.messages)
    reloaded = await service.sessions.get(session.session_id, ctx)
    assert reloaded.messages[0].role == "assistant"
    assert isinstance(reloaded.messages[0].parts[0], ToolPart)
    restored = json.loads(await service.fs.read(session.uri + "/messages.jsonl", ctx=ctx))
    assert restored["role"] == "assistant"
    assert restored["parts"][0]["tool_input"] == original
    archive = session.uri + "/history/archive_001"
    await session._merge_archive_meta(archive, {"phase1": {"user": ctx.user.to_dict()}})
    saved = json.loads(await service.viking_fs.read_file(archive + "/.meta.json", ctx=ctx))
    assert saved["phase1"]["user"] == ctx.user.to_dict()
    await session._write_done_file(archive, "default", "default")
    done = json.loads(await service.viking_fs.read_file(archive + "/.done", ctx=ctx))
    assert done["starting_message_id"] == "default"


async def test_repeated_explicit_references_restore_exact_types_without_new_fields(service, ctx):
    session = await service.sessions.create(ctx, "repeated")
    values = {"short": "a", "number": 1234, "object": {"a": [False, None]}}
    await service.privacy_configs.upsert(ctx, "session", session.session_id, values)
    row = {
        key: build_privacy_field_reference(ctx.user.user_id, "session", session.session_id, field)
        for key, field in [("password", "short"), ("token", "number"), ("secret", "object")]
    }
    await session.add_message_async("assistant", [ToolPart(tool_input={"rows": [row] * 1300})])
    current = await service.privacy_configs.get_current(ctx, "session", session.session_id)
    assert current.values == values
    restored = json.loads(await service.fs.read(session.uri + "/messages.jsonl", ctx=ctx))
    assert (
        restored["parts"][0]["tool_input"]["rows"]
        == [{"password": "a", "token": 1234, "secret": {"a": [False, None]}}] * 1300
    )


async def test_restore_leaves_foreign_missing_and_nested_references_unchanged(
    service, ctx, monkeypatch
):
    own = build_privacy_field_reference(ctx.user.user_id, "credential", "demo", "pat")
    missing = build_privacy_field_reference(ctx.user.user_id, "credential", "demo", "missing")
    unconfigured = build_privacy_field_reference(ctx.user.user_id, "credential", "absent", "pat")
    foreign = build_privacy_field_reference("alice", "credential", "demo", "pat")
    nested = build_privacy_field_reference(ctx.user.user_id, "credential", "other", "pat")
    await service.privacy_configs.upsert(ctx, "credential", "demo", {"pat": nested})
    await service.privacy_configs.upsert(ctx, "credential", "other", {"pat": "nested-secret"})
    session = await service.sessions.create(ctx, "reference-boundary")
    await session.add_message_async("user", [TextPart(f"{own} {missing} {unconfigured} {foreign}")])
    read = AsyncMock(wraps=service.privacy_configs.get_current)
    monkeypatch.setattr(service.privacy_configs, "get_current", read)
    restored = json.loads(await service.fs.read(session.uri + "/messages.jsonl", ctx=ctx))
    assert restored["parts"][0]["text"] == f"{nested} {missing} {unconfigured} {foreign}"
    assert read.await_count == 2  # One read per target, not per occurrence.
    read.assert_any_await(ctx, "credential", "absent")
