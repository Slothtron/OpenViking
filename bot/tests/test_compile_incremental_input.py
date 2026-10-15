# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Incremental Compile input: kernel change scopes become the files to read."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from vikingbot.compile.service import BotCompileService


@pytest.mark.asyncio
async def test_changed_files_expand_recursive_scopes_and_skip_deletions():
    service = BotCompileService.__new__(BotCompileService)
    service._list_source_files = AsyncMock(return_value=["viking://resources/src/new/a.md"])
    changes = {
        "scopes": [
            {"uri": "viking://resources/src/new", "change_type": "added", "recursive": True},
            {"uri": "viking://resources/src/p.md", "change_type": "modified", "recursive": False},
            {"uri": "viking://resources/src/gone.md", "change_type": "deleted", "recursive": False},
        ]
    }
    files = await service._list_changed_files(SimpleNamespace(), changes)
    assert files == ["viking://resources/src/new/a.md", "viking://resources/src/p.md"]
