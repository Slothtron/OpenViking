# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Privacy policy for Session message writes and file reads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from openviking_cli.exceptions import InvalidArgumentError

_POLICY_KEYS = {"extract", "restore"}
_SWITCH_KEYS = {"enabled"}


def _switch_enabled(data: Any, *, default_enabled: bool, key: str) -> bool:
    if data is None:
        return default_enabled
    if not isinstance(data, dict):
        raise InvalidArgumentError(f"privacy_policy.{key} must be an object")
    if set(data) - _SWITCH_KEYS:
        raise InvalidArgumentError(f"privacy_policy.{key} supports only: enabled")
    if "enabled" not in data:
        return default_enabled
    enabled = data["enabled"]
    if not isinstance(enabled, bool):
        raise InvalidArgumentError(f"privacy_policy.{key}.enabled must be a boolean")
    return enabled


@dataclass(frozen=True)
class PrivacyPolicy:
    """Effective privacy policy for one Session."""

    extract_enabled: bool = True
    restore_enabled: bool = False

    @classmethod
    def from_dict(cls, data: Any) -> "PrivacyPolicy":
        if data is None:
            return cls()
        if isinstance(data, PrivacyPolicy):
            return data
        if not isinstance(data, dict):
            raise InvalidArgumentError("privacy_policy must be an object")
        if set(data) - _POLICY_KEYS:
            raise InvalidArgumentError(
                "privacy_policy supports only: " + ", ".join(sorted(_POLICY_KEYS))
            )
        return cls(
            extract_enabled=_switch_enabled(
                data.get("extract"), default_enabled=True, key="extract"
            ),
            restore_enabled=_switch_enabled(
                data.get("restore"), default_enabled=False, key="restore"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "extract": {"enabled": self.extract_enabled},
            "restore": {"enabled": self.restore_enabled},
        }
