# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Session privacy round-trips complete messages and rejects unsafe input."""

import json
from copy import deepcopy
from dataclasses import asdict

import pytest

from openviking.message.part import ContextPart, ImagePart, TextPart, ToolPart
from openviking.privacy import session_extractor as extraction
from openviking.privacy.session_extractor import extract_session_privacy_values
from openviking.privacy.session_restore import get_session_id_from_uri, restore_session_content
from openviking_cli.exceptions import InvalidArgumentError


def message_jsonl(specs):
    return (
        "\n".join(
            json.dumps(
                {**spec, "parts": [asdict(part) for part in spec["parts"]]}, ensure_ascii=False
            )
            for spec in specs
        )
        + "\n"
    )


def test_extract_and_restore_complete_messages():
    specs = [
        {
            "role": "user",
            "parts": [
                TextPart("password='中文密码'"),
                ContextPart(uri="viking://resource/doc", abstract="api_key=abstract-secret"),
                ImagePart(url="https://example.test/password=image-reference"),
            ],
        },
        {
            "role": "assistant",
            "parts": [
                ToolPart(
                    tool_input={
                        "credentials": {
                            "password": 'quote" and\nnewline',
                            "nested": [1, False, None],
                        },
                        "password": [],
                        "token": 1234,
                        "secret": {},
                        "nested": {"password": "another-secret"},
                        "region": "cn",
                    },
                    tool_output="password='hello world'",
                    tool_output_externalization_error="api_key=error-secret",
                ),
                ToolPart(tool_id="pending", tool_output=None),
            ],
        },
    ]
    before = deepcopy(specs)
    sanitized, values = extract_session_privacy_values("demo", specs)
    # Sensitive containers are captured whole, with no references inside them.
    assert list(values.values()) == [
        "中文密码",
        "abstract-secret",
        specs[1]["parts"][0].tool_input["credentials"],
        [],
        1234,
        {},
        "another-secret",
        "hello world",
        "error-secret",
    ]
    assert specs == before
    clean = message_jsonl(sanitized)
    for secret in ["中文密码", "abstract-secret", "another-secret", "hello world", "error-secret"]:
        assert secret not in clean
    assert restore_session_content(clean, "demo", values, jsonl=True) == message_jsonl(before)
    assert extract_session_privacy_values("demo", sanitized) == (sanitized, {})
    specs[1]["parts"][0].tool_input["credentials"]["nested"].append("changed")
    assert "changed" not in json.dumps(list(values.values()))


def test_existing_references_are_not_rebound_or_recursively_restored():
    old = "{{ov_privacy:session:demo:old}}"
    foreign = "{{ov_privacy:session:other:old}}"
    following = "{{ov_privacy:session:demo:following}}"
    specs = [
        {
            "parts": [
                ToolPart(tool_input={"password": "${PASSWORD}", "token": "$TOKEN"}),
                TextPart(f"{old} {foreign} password=one"),
            ]
        }
    ]
    first, values = extract_session_privacy_values("demo", specs)
    second, added = extract_session_privacy_values(
        "demo", [{"parts": [TextPart(first[0]["parts"][1].text + " password=two")]}]
    )
    assert list(values.values()) == ["one"]
    assert list(added.values()) == ["two"]
    assert not values.keys() & added.keys()
    assert first[0]["parts"][1].text in second[0]["parts"][0].text
    assert (
        restore_session_content(
            f"{old} {foreign}", "demo", {"old": following, "following": "secret"}
        )
        == f"{following} {foreign}"
    )
    assert restore_session_content(old, "demo", {}) == old


def test_sensitive_field_names_do_not_leak_into_references():
    key = "sk-proj-12345678901234567890_api_key"
    specs = [{"parts": [ToolPart(tool_input={key: "secret-value"})]}]
    sanitized, values = extract_session_privacy_values("demo", specs)
    assert key not in message_jsonl(sanitized)
    assert key not in str(list(values))
    assert restore_session_content(
        message_jsonl(sanitized), "demo", values, jsonl=True
    ) == message_jsonl(specs)


@pytest.mark.parametrize("sensitive", [False, True], ids=["nested-input", "sensitive-container"])
@pytest.mark.parametrize(
    "limit,bound",
    [
        ("MAX_TEXT_CHARS", 20),
        ("MAX_BATCH_CHARS", 20),
        ("MAX_NODES", 4),
        ("MAX_MATCHES", 1),
        ("MAX_DEPTH", 1),
    ],
)
def test_limits_reject_batch_without_mutating_input(monkeypatch, limit, bound, sensitive):
    monkeypatch.setattr(extraction, limit, bound)
    value = {"a": {"password": "secret-value" * 4}}
    specs = [
        {
            "parts": [
                ToolPart(
                    tool_input={"credentials": value} if sensitive else value,
                    tool_output="password=another-secret",
                )
            ]
        }
    ]
    before = deepcopy(specs)
    with pytest.raises(InvalidArgumentError) as exc:
        extract_session_privacy_values("demo", specs)
    assert "secret-value" not in str(exc.value)
    assert specs == before


@pytest.mark.parametrize("kind", ["cycle", "non-string-key", "unsupported-value"])
def test_invalid_sensitive_container_is_rejected(kind):
    value = {"cycle": {}, "non-string-key": {1: "secret"}, "unsupported-value": {"x": object()}}[
        kind
    ]
    if kind == "cycle":
        value["self"] = value
    with pytest.raises(InvalidArgumentError):
        extract_session_privacy_values(
            "demo", [{"parts": [ToolPart(tool_input={"credentials": value})]}]
        )


@pytest.mark.parametrize(
    "path,allowed",
    [
        ("messages.jsonl", True),
        ("history/archive_001/messages.jsonl", True),
        ("tool-results/tool-1/output.txt", True),
        (".meta.json", False),
        ("history/archive_001/.overview.md", False),
        ("tool-results/tool-1/meta.json", False),
    ],
)
def test_restore_is_scoped_to_session_content(path, allowed):
    root = "viking://user/alice/sessions"
    assert get_session_id_from_uri(f"{root}/demo/{path}", root) == ("demo" if allowed else None)
    assert get_session_id_from_uri(f"viking://user/bob/sessions/demo/{path}", root) is None


def test_invalid_message_jsonl_is_rejected():
    with pytest.raises(InvalidArgumentError):
        restore_session_content(
            "not json {{ov_privacy:session:demo:one}}", "demo", {"one": "secret"}, jsonl=True
        )
