# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Restore Session privacy only at the public file-read boundary."""

import json
import re
from typing import Any, Optional

from openviking.core.namespace import canonical_session_root
from openviking.privacy.service import UserPrivacyConfigService
from openviking.server.identity import RequestContext
from openviking.session.privacy_policy import PrivacyPolicy
from openviking.storage.viking_fs import VikingFS
from openviking_cli.exceptions import InvalidArgumentError, NotFoundError

# Text fields that may contain placeholders, keyed by Part type.
_TEXT_FIELDS = {
    "text": ("text",),
    "context": ("abstract",),
    "tool": ("tool_output", "tool_output_externalization_error"),
}


def get_session_id_from_uri(uri: str, session_root: str) -> str | None:
    """Return the Session ID for message and tool-output files that may be restored."""
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


def restore_session_content(
    content: str, session_id: str, values: dict[str, Any], *, jsonl: bool = False
) -> str:
    """Replace this Session's placeholders with current values.

    Unknown keys keep their placeholder. JSONL is parsed per record so restored
    quotes or newlines cannot break the message file format.
    """
    pattern = re.compile(r"\{\{ov_privacy:session:" + re.escape(session_id) + r":([^}:]+)\}\}")

    def text(value: str) -> str:
        # One pass: values containing other references are never expanded.
        return pattern.sub(
            lambda match: str(values[match[1]]) if match[1] in values else match[0], value
        )

    def structured(value: Any) -> Any:
        if isinstance(value, str):
            match = pattern.fullmatch(value)
            if match and match[1] in values:
                return values[match[1]]
            return text(value)
        if isinstance(value, list):
            return [structured(item) for item in value]
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                restored_key = text(key)
                if restored_key in result:
                    raise InvalidArgumentError("Session privacy restored object key collision")
                result[restored_key] = structured(item)
            return result
        return value

    if not pattern.search(content):
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


async def _read_session_policy(
    viking_fs: VikingFS, session_root: str, session_id: str, ctx: RequestContext
) -> PrivacyPolicy:
    try:
        meta = json.loads(
            await viking_fs.read_file(f"{session_root}/{session_id}/.meta.json", ctx=ctx)
        )
    except (NotFoundError, FileNotFoundError):
        return PrivacyPolicy()
    return PrivacyPolicy.from_dict(meta.get("privacy_policy"))


async def restore_session_read(
    viking_fs: VikingFS,
    privacy: Optional[UserPrivacyConfigService],
    uri: str,
    content: str,
    ctx: RequestContext,
) -> str:
    """Apply the Session's restore policy to content read from ``uri``."""
    session_root = canonical_session_root(ctx)
    session_id = get_session_id_from_uri(uri, session_root)
    # Skip policy and snapshot reads unless this Session's placeholder is present.
    if (
        privacy is None
        or session_id is None
        or f"{{{{ov_privacy:session:{session_id}:" not in content
    ):
        return content
    policy = await _read_session_policy(viking_fs, session_root, session_id, ctx)
    if not policy.restore_enabled:
        return content
    current = await privacy.get_current(ctx, "session", session_id)
    if current is None:
        return content
    return restore_session_content(
        content, session_id, current.values, jsonl=uri.endswith("/messages.jsonl")
    )
