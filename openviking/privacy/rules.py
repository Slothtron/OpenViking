# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Offline credential detection for privacy extraction.

Rules recognize credential formats, named fields and CLI arguments. Matches
refer to the original text; callers choose how to replace and store them.
"""

import re
from dataclasses import dataclass

from openviking_cli.exceptions import InvalidArgumentError

MAX_TEXT_CHARS = 4 * 1024 * 1024
MAX_MATCHES = 100_000

# Define field aliases once; exact names accept camel case as well as separators.
_SENSITIVE_FIELDS = frozenset(
    {
        "ak",
        "sk",
        "api_key",
        "access_key",
        "access_key_id",
        "secret_key",
        "secret_access_key",
        "access_key_secret",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "session_token",
        "auth_token",
        "security_token",
        "bot_token",
        "client_secret",
        "app_secret",
        "private_key",
        "signing_key",
        "signing_secret",
        "webhook_secret",
        "password",
        "passwd",
        "pwd",
        "authorization",
        "proxy_authorization",
        "x_api_key",
        "credential",
        "credentials",
    }
)
_KEY_TRANSLATION = str.maketrans("", "", "-_ ")
_SENSITIVE_NAMES = frozenset(name.translate(_KEY_TRANSLATION) for name in _SENSITIVE_FIELDS)
# Only unambiguous credential names qualify arbitrary namespaces. A generic
# '*_key' or '*_token' suffix could also describe business keys or usage metrics.
_SENSITIVE_SUFFIXES = tuple(
    "_" + name
    for name in sorted(_SENSITIVE_FIELDS)
    if name
    not in {
        "ak",
        "sk",
        "secret",
        "token",
        "authorization",
        "proxy_authorization",
        "credential",
        "credentials",
    }
)
_ENV_PREFIXES = ("aws", "volcengine", "volc", "openviking", "openai", "anthropic", "root")
_NORMALIZED_NAMES = _SENSITIVE_NAMES | frozenset(
    prefix + name for prefix in _ENV_PREFIXES for name in _SENSITIVE_NAMES
)
_TOKEN_CHARS = r"A-Za-z0-9_./+=~\-"
# Consume the complete token, including punctuation allowed in credential bodies.
# A strict minimum avoids extracting ordinary prose such as 'sk-example'.
# Each format defines its literal hints, prefix pattern and body pattern once.
_TOKEN_FORMATS = (
    (("sk-",), r"sk-(?:proj-|svcacct-|ant-(?:api\d+-)?)?", rf"[{_TOKEN_CHARS}*]{{16,}}"),
    (("ak-", "ark-"), r"(?:ak|ark)-", rf"[{_TOKEN_CHARS}*]{{16,}}"),
    (
        ("ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"),
        r"(?:gh[pousr]_|github_pat_)",
        rf"[{_TOKEN_CHARS}*]{{16,}}",
    ),
    (("AKLT", "AKTP"), r"(?:AKLT|AKTP)", rf"[{_TOKEN_CHARS}*]{{12,}}"),
    (("AKIA", "ASIA"), r"(?:AKIA|ASIA)", rf"[A-Z0-9]{{16}}(?![{_TOKEN_CHARS}])"),
)
_TOKEN_HINTS = tuple(hint for hints, _, _ in _TOKEN_FORMATS for hint in hints)
_TOKEN_RE = re.compile(
    r"(?<![\w-])(?:" + "|".join(prefix + body for _, prefix, body in _TOKEN_FORMATS) + ")"
)
_BRACKETED_TOKEN = r"\[[^\[\]\s\"',;]+\]"
_BRACKETED_TOKEN_RE = re.compile(_BRACKETED_TOKEN)
_AUTH_SCHEME = r"(?i)\b(?:Bearer|Basic)[ \t]+"
_AUTH_SCHEME_RE = re.compile(_AUTH_SCHEME)
_AUTH_RE = re.compile(rf"{_AUTH_SCHEME}(?P<value>(?:{_BRACKETED_TOKEN}|[{_TOKEN_CHARS}*])+)")
# Bound field names; don't use a greedy '.*' expression over message bodies.
_ASSIGNMENT_RE = re.compile(
    r"(?<![\w-])(?P<key>api[ ]key|secret[ ]key|[A-Za-z_][A-Za-z0-9_-]{0,127})"
    r"[\"']?\]?[ \t]*[:=：][ \t]*",
    re.IGNORECASE,
)
_CLI_OPTION_RE = re.compile(
    r"(?<![\w-])(?:--(?P<key>[A-Za-z_][A-Za-z0-9_-]{0,127})"
    r"(?:[ \t]*=[ \t]*|[ \t]+)|-(?P<short>[uU])(?:[ \t]+|(?=[^\s=])))"
)
_ENV_REFERENCE_RE = re.compile(r"\$\{[^{}\s]+\}|\$[A-Za-z_]\w*|<[^<>]+>")


def is_sensitive_key(key: str) -> bool:
    lowered = key.lower().replace("-", "_")
    return (
        lowered.endswith(_SENSITIVE_SUFFIXES)
        or lowered.translate(_KEY_TRANSLATION) in _NORMALIZED_NAMES
    )


def _value_span(text: str, start: int) -> tuple[int, int]:
    """Locate one assignment value on the original text, preserving quotes."""
    env_reference = _ENV_REFERENCE_RE.match(text, start) if text.startswith("${", start) else None
    if env_reference:
        return start, env_reference.end()
    end = start
    if start < len(text) and text[start] in "\"'":
        quote = text[start]
        start += 1
        end = start
        while end < len(text):
            if text[end] == "\\":
                end = min(len(text), end + 2)
            elif text[end] == quote:
                break
            else:
                end += 1
    else:
        while end < len(text) and not text[end].isspace() and text[end] not in ",;，；\"'<>}{]":
            if text[end] == "[":
                bracketed = _BRACKETED_TOKEN_RE.match(text, end)
                if bracketed is None:
                    break
                end = bracketed.end()
            else:
                end += 1
    return start, end


def _shell_value_span(text: str, start: int) -> tuple[int, int]:
    """Locate one shell word, including quoted fragments and escaped spaces.

    This only locates recorded text; it never expands or executes shell input.
    A single enclosing quote pair is preserved. Concatenated or unclosed quote
    fragments are part of the captured span so no credential fragment survives.
    """
    encoded_quotes = text.startswith('\\"', start)
    quote = '"' if encoded_quotes else None
    width = 2 if encoded_quotes else 1
    end = start + 2 if encoded_quotes else start
    first_quote_end = None
    while end < len(text):
        char = text[end]
        if encoded_quotes and char == "\\" and quote != "'":
            run_end = end + 1
            while run_end < len(text) and text[run_end] == "\\":
                run_end += 1
            if run_end == end + 1 and run_end < len(text) and text[run_end] == '"':
                if quote:
                    quote = None
                    if first_quote_end is None:
                        first_quote_end = end
                else:
                    quote = '"'
                end = run_end + 1
            else:
                end = min(len(text), run_end + 1)
        elif char == "\\" and quote != "'":
            end = min(len(text), end + 2)
        elif quote:
            if char == quote and (not encoded_quotes or quote == "'"):
                quote = None
                if first_quote_end is None:
                    first_quote_end = end
            end += 1
        elif char in "\"'":
            if encoded_quotes and char == '"':
                break  # The enclosing JSON string, outside the shell word.
            quote = char
            end += 1
        elif char.isspace() or char in ";&|()<>":
            break
        else:
            end += 1
    if start < end and (encoded_quotes or text[start] in "\"'") and first_quote_end == end - width:
        return start + width, end - width
    return start, end


@dataclass(frozen=True)
class PrivacyMatch:
    start: int
    end: int
    field_name: str


_PRIVACY_REFERENCE_RE = re.compile(r"\{\{ov_privacy:[^{}\r\n]+\}\}")


def find_privacy_matches(content: str, *, field_name: str | None = None) -> list[PrivacyMatch]:
    """Return non-overlapping credential spans without modifying the input."""
    if len(content) > MAX_TEXT_CHARS:
        raise InvalidArgumentError("Privacy extraction text limit exceeded")
    matches: list[PrivacyMatch] = []

    def add_match(start: int, end: int, key: str, *, structured: bool = False) -> None:
        value = content[start:end]
        # Mask-like strings can be real credentials; preserve their original values.
        if (
            not value
            or (not structured and value in {"|", ">"})
            or _PRIVACY_REFERENCE_RE.fullmatch(value)
            or _ENV_REFERENCE_RE.fullmatch(value)
        ):
            return
        if len(matches) >= MAX_MATCHES:
            raise InvalidArgumentError("Privacy extraction match limit exceeded")
        field_name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
        field_name = field_name.lower().replace("-", "_").replace(" ", "_")
        matches.append(PrivacyMatch(start, end, field_name))

    # Structured tool inputs supply the field context without serializing JSON,
    # so escapes and whitespace remain part of the original value.
    if field_name and is_sensitive_key(field_name):
        start = 0
        if field_name.translate(_KEY_TRANSLATION).lower() in {
            "authorization",
            "proxyauthorization",
        }:
            auth = _AUTH_SCHEME_RE.match(content)
            if auth:
                start = auth.end()
        add_match(start, len(content), field_name, structured=True)
        return matches

    covered_until = 0
    assignments = _ASSIGNMENT_RE.finditer(content) if any(c in content for c in "=:：") else ()
    for match in assignments:
        if match.start() < covered_until or not is_sensitive_key(match["key"]):
            continue
        start = match.end()
        auth_start = start + 1 if start < len(content) and content[start] in "\"'" else start
        if match["key"].translate(_KEY_TRANSLATION).lower() in {
            "authorization",
            "proxyauthorization",
        } and _AUTH_SCHEME_RE.match(content, auth_start):
            continue
        start, end = _value_span(content, start)
        add_match(start, end, match["key"])
        covered_until = end

    cli_covered_until = 0
    options = (
        _CLI_OPTION_RE.finditer(content)
        if "--" in content or "-u" in content or "-U" in content
        else ()
    )
    for match in options:
        if match.start() < cli_covered_until:
            continue
        key = match["key"] or "user"
        normalized = key.translate(_KEY_TRANSLATION).lower()
        combined_auth = normalized in {"user", "proxyuser"}
        if normalized == "oauth2bearer":
            key = "access_token"
        if not combined_auth and not is_sensitive_key(key):
            continue
        start = match.end()
        while content.startswith(("\\\n", "\\\r\n"), start):
            start += 3 if content.startswith("\\\r\n", start) else 2
            while start < len(content) and content[start] in " \t":
                start += 1
        if content.startswith("--", start):
            continue
        start, end = _shell_value_span(content, start)
        if end > start and (not combined_auth or ":" in content[start:end]):
            cli_covered_until = end
            if normalized in {"authorization", "proxyauthorization"}:
                auth = _AUTH_SCHEME_RE.match(content, start, end)
                if auth:
                    start = auth.end()
            add_match(start, end, "credential" if combined_auth else key)

    if any(hint in content for hint in _TOKEN_HINTS):
        for match in _TOKEN_RE.finditer(content):
            add_match(match.start(), match.end(), "credential")
    if "b" in content or "B" in content:
        for match in _AUTH_RE.finditer(content):
            start, end = match.span("value")
            add_match(start, end, "authorization")

    # Never interpret an existing privacy reference as a new credential.
    references = list(_PRIVACY_REFERENCE_RE.finditer(content))
    reference_index = 0
    merged: list[PrivacyMatch] = []
    for match in sorted(matches, key=lambda item: item.start):
        while (
            reference_index < len(references) and references[reference_index].end() <= match.start
        ):
            reference_index += 1
        if (
            reference_index < len(references)
            and references[reference_index].start() <= match.start
            and match.end <= references[reference_index].end()
        ):
            continue
        if merged and match.start < merged[-1].end:
            previous = merged[-1]
            merged[-1] = PrivacyMatch(
                previous.start, max(previous.end, match.end), previous.field_name
            )
        else:
            merged.append(match)
    return merged
