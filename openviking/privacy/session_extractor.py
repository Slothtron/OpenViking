# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Extract Session credentials before externalization and message persistence."""

from dataclasses import replace
from typing import Any
from uuid import uuid4

from openviking.message.part import ContextPart, ImagePart, TextPart, ToolPart
from openviking.privacy.rules import (
    MAX_MATCHES,
    MAX_TEXT_CHARS,
    find_privacy_matches,
    is_sensitive_key,
)
from openviking_cli.exceptions import InvalidArgumentError

MAX_BATCH_CHARS = 16 * 1024 * 1024
MAX_DEPTH = 32
MAX_NODES = 100_000


def build_session_placeholder(session_id: str, key: str) -> str:
    return f"{{{{ov_privacy:session:{session_id}:{key}}}}}"


class _SessionExtraction:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.values: dict[str, Any] = {}
        self.chars = 0
        self.nodes = 0
        self.matches = 0

    def node(self, depth: int) -> None:
        self.nodes += 1
        if depth > MAX_DEPTH or self.nodes > MAX_NODES:
            raise InvalidArgumentError("Session privacy structure limit exceeded")

    def capture(self, value: Any) -> str:
        self.matches += 1
        if self.matches > MAX_MATCHES:
            raise InvalidArgumentError("Session privacy match limit exceeded")
        # Random keys are never reused after an older snapshot is activated, and
        # cannot be bound by references the caller supplies in the same batch.
        # Field names are not used because they can contain credentials too.
        key = f"value_{uuid4().hex}"
        self.values[key] = value
        return build_session_placeholder(self.session_id, key)

    def text(self, text: str | None, *, scan: bool = True) -> str | None:
        # Pending tool calls may carry no output yet.
        if not text:
            return text
        self.chars += len(text)
        if len(text) > MAX_TEXT_CHARS or self.chars > MAX_BATCH_CHARS:
            raise InvalidArgumentError("Session privacy text limit exceeded")
        if not scan:
            return text
        chunks = []
        offset = 0
        for match in find_privacy_matches(text):
            chunks.extend((text[offset : match.start], self.capture(text[match.start : match.end])))
            offset = match.end
        chunks.append(text[offset:])
        return "".join(chunks)

    def copy_value(self, value: Any, depth: int = 0, *, scan: bool = True) -> Any:
        self.node(depth)
        if isinstance(value, str):
            return self.text(value, scan=scan)
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise InvalidArgumentError("Session privacy requires string object keys")
                clean_key = self.text(key, scan=scan)
                if clean_key in result:
                    raise InvalidArgumentError("Session privacy object key collision")
                sensitive = scan and is_sensitive_key(key)
                # A sensitive field owns the whole value. Traverse it only to
                # validate limits and copy caller-owned data, without extracting
                # or replacing anything inside it.
                copied = self.copy_value(item, depth + 1, scan=scan and not sensitive)
                if sensitive and item is not None:
                    # Preserve empty strings and existing references as before.
                    # Use a neutral sensitive name so Authorization is captured
                    # in full, including its authentication scheme.
                    if not isinstance(item, str) or find_privacy_matches(
                        item, field_name="password"
                    ):
                        copied = self.capture(copied)
                result[clean_key] = copied
            return result
        if isinstance(value, (list, tuple)):
            copied = [self.copy_value(item, depth + 1, scan=scan) for item in value]
            return tuple(copied) if isinstance(value, tuple) else copied
        if value is None or isinstance(value, (bool, int, float)):
            return value
        raise InvalidArgumentError("Session privacy requires JSON-compatible tool input")


def extract_session_privacy_values(
    session_id: str, messages_spec: list[dict]
) -> tuple[list[dict], dict[str, Any]]:
    """Return placeholderized copies of the batch and the captured values.

    The caller's specs and parts are not modified.
    """
    extraction = _SessionExtraction(session_id)
    result = []
    for spec in messages_spec:
        extraction.node(0)
        copied = dict(spec)
        parts = []
        for part in spec.get("parts", []):
            extraction.node(0)
            if isinstance(part, TextPart):
                parts.append(replace(part, text=extraction.text(part.text)))
            elif isinstance(part, ContextPart):
                parts.append(replace(part, abstract=extraction.text(part.abstract)))
            elif isinstance(part, ToolPart):
                parts.append(
                    replace(
                        part,
                        tool_input=extraction.copy_value(part.tool_input),
                        tool_output=extraction.text(part.tool_output),
                        tool_output_externalization_error=extraction.text(
                            part.tool_output_externalization_error
                        ),
                    )
                )
            elif isinstance(part, ImagePart):
                parts.append(replace(part))
            else:
                raise InvalidArgumentError("Session privacy received unsupported Part type")
        if "parts" in copied:
            copied["parts"] = parts
        result.append(copied)
    return result, extraction.values
