# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Durable Compile target registry and run records in account _system storage.

Layout under ``/local/{account}/_system/compile/``:
``targets.json`` (summary index), ``targets/{id}/record.json`` (source of truth)
and ``targets/{id}/runs/{task_id}.json`` (one run record per Compile task).
Read-modify-write of each JSON file happens under its own PathLock, so the
layout stays consistent across instances when PathLock uses the cache provider.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import uuid4

from openviking.pyagfs import AsyncAGFSClient
from openviking.pyagfs.async_client import fs_ctx_from_agfs_path
from openviking.pyagfs.exceptions import AGFSAlreadyExistsError, AGFSNotFoundError

LOCK_TIMEOUT_SECONDS = 30.0
MAX_FINISHED_RUNS = 100
SUMMARY_FIELDS = (
    "id",
    "to",
    "status",
    "initialized",
    "auto_update",
    "update_frequency",
    "auto_update_enabled_at",
    "last_triggered_slot",
)


def new_record(target_id: str, to: str) -> Dict[str, Any]:
    return {
        "id": target_id,
        "to": to,
        "status": "active",
        "deleted_at": None,
        "from": [],
        "skill": None,
        "instruction": None,
        "initialized": False,
        "auto_update": False,
        "update_frequency": "daily",
        "auto_update_enabled_at": None,
        "last_triggered_slot": None,
        "last_error": None,
        "progress": {"source_commit": None, "processed_roots": []},
        # Snapshot the per-file version indexes were last synced to.
        "versions_head": None,
        "active_task_id": None,
        "last_run": None,
        "execution_context": {"executor": None, "credential": None, "args": {}},
    }


def summarize(record: Dict[str, Any]) -> Dict[str, Any]:
    summary = {key: record.get(key) for key in SUMMARY_FIELDS}
    summary["running"] = bool(record.get("active_task_id"))
    return summary


class CompileStateStore:
    """JSON persistence for Compile targets; all writes take the file's PathLock."""

    def __init__(self, agfs: Any) -> None:
        self._agfs = agfs if isinstance(agfs, AsyncAGFSClient) else AsyncAGFSClient(agfs)

    # ---------- paths ----------

    @staticmethod
    def root(account_id: str) -> str:
        return f"/local/{account_id}/_system/compile"

    def _targets_path(self, account_id: str) -> str:
        return f"{self.root(account_id)}/targets.json"

    def _target_dir(self, account_id: str, target_id: str) -> str:
        return f"{self.root(account_id)}/targets/{target_id}"

    def _record_path(self, account_id: str, target_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/record.json"

    def _run_path(self, account_id: str, target_id: str, task_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/runs/{task_id}.json"

    # ---------- primitives ----------

    async def read_json(self, path: str) -> Optional[Any]:
        try:
            raw = await self._agfs.read(path)
        except (AGFSNotFoundError, FileNotFoundError):
            return None
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

    async def _ensure_parent(self, path: str) -> None:
        try:
            await self._agfs.ensure_parent_dirs(path)
        except AGFSAlreadyExistsError:
            pass

    async def write_json(self, path: str, value: Any, lease: Any = None) -> None:
        """Write JSON; pass the held lease when ``path`` is already locked by the caller."""
        await self._ensure_parent(path)
        fs_ctx = fs_ctx_from_agfs_path(path)
        if lease is not None:
            fs_ctx["lease_ref"] = lease["lease_ref"]
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        await self._agfs.write(path, data, fs_ctx=fs_ctx)

    @asynccontextmanager
    async def locked(self, path: str) -> AsyncIterator[Any]:
        await self._ensure_parent(path)
        lease = await self._agfs.pathlock_acquire_exact(path, timeout_secs=LOCK_TIMEOUT_SECONDS)
        try:
            yield lease
        finally:
            await self._agfs.pathlock_release(lease)

    # ---------- targets.json ----------

    async def list_summaries(self, account_id: str) -> List[Dict[str, Any]]:
        data = await self.read_json(self._targets_path(account_id))
        return list((data or {}).get("targets", []))

    async def _sync_summary(self, account_id: str, record: Dict[str, Any]) -> None:
        """Update only this target's entry so concurrent writers never clobber each other."""
        path = self._targets_path(account_id)
        async with self.locked(path) as lease:
            targets = [
                item
                for item in await self.list_summaries(account_id)
                if item.get("id") != record["id"]
            ]
            targets.append(summarize(record))
            await self.write_json(path, {"targets": targets}, lease)

    async def find_id(self, account_id: str, to: str) -> Optional[str]:
        for item in await self.list_summaries(account_id):
            if item.get("to") == to and item.get("status") == "active":
                return item.get("id")
        return None

    # ---------- record.json ----------

    async def get(self, account_id: str, target_id: str) -> Optional[Dict[str, Any]]:
        return await self.read_json(self._record_path(account_id, target_id))

    async def get_by_to(self, account_id: str, to: str) -> Optional[Dict[str, Any]]:
        target_id = await self.find_id(account_id, to)
        return await self.get(account_id, target_id) if target_id else None

    @asynccontextmanager
    async def edit(self, account_id: str, target_id: str) -> AsyncIterator[Dict[str, Any]]:
        """Yield the record under its lock; it is saved (and summarized) on clean exit."""
        path = self._record_path(account_id, target_id)
        async with self.locked(path) as lease:
            record = await self.read_json(path)
            if record is None:
                raise LookupError(f"Compile target not found: {target_id}")
            yield record
            await self.write_json(path, record, lease)
        await self._sync_summary(account_id, record)

    async def register(self, account_id: str, to: str) -> str:
        """Return the active target ID for ``to``, creating a record on first use."""
        path = self._targets_path(account_id)
        async with self.locked(path) as lease:
            existing = await self.find_id(account_id, to)
            if existing:
                return existing
            target_id = f"target_{uuid4().hex[:16]}"
            record = new_record(target_id, to)
            await self.write_json(self._record_path(account_id, target_id), record)
            targets = await self.list_summaries(account_id)
            targets.append(summarize(record))
            await self.write_json(path, {"targets": targets}, lease)
            return target_id

    # ---------- runs ----------

    async def get_run(
        self, account_id: str, target_id: str, task_id: str
    ) -> Optional[Dict[str, Any]]:
        return await self.read_json(self._run_path(account_id, target_id, task_id))

    async def put_run(self, account_id: str, target_id: str, run: Dict[str, Any]) -> None:
        await self.write_json(self._run_path(account_id, target_id, run["task_id"]), run)

    async def list_dir(self, directory: str) -> List[str]:
        """JSON file names in ``directory``; empty when it does not exist."""
        try:
            entries = await self._agfs.ls(directory)
        except (AGFSNotFoundError, FileNotFoundError):
            return []
        return [
            str(entry.get("name"))
            for entry in entries
            if str(entry.get("name") or "").endswith(".json")
        ]

    async def remove(self, path: str, *, recursive: bool = False) -> None:
        try:
            await self._agfs.rm(path, recursive=recursive)
        except (AGFSNotFoundError, FileNotFoundError):
            pass

    async def list_runs(self, account_id: str, target_id: str) -> List[Dict[str, Any]]:
        directory = f"{self._target_dir(account_id, target_id)}/runs"
        runs = []
        for name in await self.list_dir(directory):
            run = await self.read_json(f"{directory}/{name}")
            if run is not None:
                runs.append(run)
        runs.sort(key=lambda run: (run.get("created_at") or 0, run["task_id"]), reverse=True)
        return runs

    async def prune_runs(self, account_id: str, target_id: str) -> None:
        finished = [
            run
            for run in await self.list_runs(account_id, target_id)
            if run.get("status") in {"completed", "failed", "cancelled"}
        ]
        for run in finished[MAX_FINISHED_RUNS:]:
            await self.remove(self._run_path(account_id, target_id, run["task_id"]))

    async def list_accounts(self) -> List[str]:
        try:
            entries = await self._agfs.ls("/local")
        except (AGFSNotFoundError, FileNotFoundError):
            return []
        return [
            str(entry.get("name"))
            for entry in entries
            if entry.get("name")
            and entry.get("name") != "_system"
            and entry.get("isDir", entry.get("is_dir", False))
        ]
