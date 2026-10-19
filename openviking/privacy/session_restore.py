# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Restore user-owned references only at the public Session content boundary."""

import json
import re
from typing import Any

from openviking.privacy.reference import parse_privacy_field_reference
from openviking.privacy.service import UserPrivacyConfigService
from openviking.server.identity import RequestContext
from openviking_cli.exceptions import InvalidArgumentError, NotFoundError, ResourceExhaustedError

_REFERENCE = re.compile(r"\{\{[ \t]*ov_privacy:[^{}\r\n]+\}\}")
_SESSION_REFERENCE = re.compile(
    r"\{\{[ \t]*ov_privacy:session:([A-Za-z0-9_.@-]+):([A-Za-z0-9_-]{8})[ \t]*\}\}"
)
_TEXT_FIELDS = {
    "text": ("text",),
    "context": ("abstract",),
    "tool": ("tool_output", "tool_output_externalization_error"),
}
MAX_RESTORE_TARGETS = 1024


def get_session_id_from_uri(uri: str, session_root: str) -> str | None:
    prefix = session_root + "/"
    if not uri.startswith(prefix):
        return None
    session_id, *path = uri[len(prefix) :].split("/")
    if not session_id:
        return None
    if path == ["messages.jsonl"]:
        return session_id
    if len(path) == 3 and path[0] == "history" and path[1].startswith("archive_"):
        return session_id if path[2] == "messages.jsonl" else None
    if len(path) == 3 and path[0] == "tool-results" and path[2] == "output.txt":
        return session_id
    return None


def _restore_content(content: str, resolved: dict[str, Any], *, jsonl: bool) -> str:
    def text(value: str) -> str:
        return _REFERENCE.sub(
            lambda match: str(resolved[match[0]]) if match[0] in resolved else match[0], value
        )

    def structured(value):
        if isinstance(value, str):
            if value in resolved:
                return resolved[value]
            return text(value)
        if isinstance(value, list):
            return [structured(item) for item in value]
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                clean_key = text(key)
                if clean_key in result:
                    raise InvalidArgumentError("Session privacy restored object key collision")
                result[clean_key] = structured(item)
            return result
        return value

    if not resolved:
        return content
    if not jsonl:
        return text(content)
    lines = []
    for line in content.split("\n"):
        if not line.strip():
            lines.append(line)
            continue
        try:
            message = json.loads(line)
            for part in message.get("parts", []):
                for field in _TEXT_FIELDS.get(part.get("type"), ()):
                    if isinstance(part.get(field), str):
                        part[field] = text(part[field])
                if part.get("type") == "tool" and "tool_input" in part:
                    part["tool_input"] = structured(part["tool_input"])
        except (ValueError, TypeError, AttributeError):
            raise InvalidArgumentError("Session privacy requires valid message JSONL") from None
        lines.append(json.dumps(message, ensure_ascii=False))
    return "\n".join(lines)


async def restore_session_read(
    privacy: UserPrivacyConfigService,
    content: str,
    ctx: RequestContext,
    *,
    session_id: str,
    jsonl: bool = False,
) -> str:
    """Resolve user-owned references from current values without rewriting disk."""
    selectors = {}
    targets = {}
    for match in _REFERENCE.finditer(content):
        selector = match[0]
        short = _SESSION_REFERENCE.fullmatch(selector)
        if short is not None:
            if short[1] != session_id:
                continue
            target, field = ("session", session_id), short[2]
        else:
            reference = parse_privacy_field_reference(selector)
            if reference is None or reference.owner_user_id != ctx.user.user_id:
                continue
            target, field = (reference.category, reference.target_key), reference.field
        if target not in targets:
            if len(targets) >= MAX_RESTORE_TARGETS:
                raise ResourceExhaustedError("Session privacy reference target limit exceeded")
            targets[target] = None
        selectors[selector] = (target, field)
    for category, target_key in targets:
        try:
            current = await privacy.get_current(ctx, category, target_key)
        except (NotFoundError, FileNotFoundError, InvalidArgumentError):
            current = None
        targets[(category, target_key)] = current.values if current else {}
    resolved = {
        selector: targets[target][field]
        for selector, (target, field) in selectors.items()
        if field in targets[target]
    }
    return _restore_content(content, resolved, jsonl=jsonl)
