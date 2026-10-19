# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Compile targets: registry, incremental lifecycle and per-file versions.

State lives under ``/local/{account}/_system/compile/``: ``target_folders.json``
(summary index), ``target_folders/{id}/state.json`` (source of truth),
``target_folders/{id}/operations/{operation_id}.json`` (one record per directory operation),
``target_folders/{id}/operation_outputs/{operation_id}.json`` (immutable-path version references), and
``target_folders/{id}/file_versions/{sha256(uri)}.json`` (one version index per output
file). Each JSON's read-modify-write takes its own PathLock, so the layout stays
consistent across instances when PathLock uses the cache provider.

CompileService delegates to :class:`CompileLifecycle` from ``create``,
``on_start``, ``submit`` and ``on_finish``. A task runs after the request that
authorized it has ended, so Git access uses an internal ``bypass_acl`` context
for the task owner. Version bodies stay in Git; an index entry points at the
snapshot (``snapshot_id`` + ``snapshot_path``) holding its content.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import sys
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import (
    Annotated,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Union,
)
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from openviking.pyagfs import AsyncAGFSClient
from openviking.pyagfs.async_client import fs_ctx_from_agfs_path
from openviking.pyagfs.exceptions import (
    AGFSAlreadyExistsError,
    AGFSNotFoundError,
    AGFSNotSupportedError,
    GitConcurrentCommitError,
)
from openviking.server.identity import RequestContext, Role
from openviking.service.external_task_service import ExternalTaskSnapshot
from openviking.service.task_tracker_concurrency import run_to_completion
from openviking.storage.acl import AclAction
from openviking.storage.errors import LockAcquisitionError, ResourceBusyError
from openviking_cli.exceptions import (
    ConflictError,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
)
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

# Per-call references are collected in memory and persisted once before terminal status.
_operation_outputs: ContextVar[Optional[dict]] = ContextVar("operation_outputs", default=None)


# ==========================================================================
# Registry storage
# ==========================================================================


LOCK_TIMEOUT_SECONDS = 30.0
SUMMARY_FIELDS = (
    "id",
    "to",
    "status",
    "deleted_at",
    "initialized",
    "auto_update",
    "update_schedule",
    "auto_update_enabled_at",
    "last_triggered_slot",
    "content_updated_at",
    "first_compile_created_at",
    "compile_type",
    "conflicts_num",
)


def new_record(target_id: str, to: str) -> Dict[str, Any]:
    return {
        "id": target_id,
        "to": to,
        "compile_type": "compile",  # Compile category for this target.
        "conflicts_num": 0,  # Number of conflicting files under this target.
        "status": "active",
        "deleted_at": None,  # Last deletion time; retained on reactivation to reject old tasks.
        "from": [],
        "skill": None,
        "instruction": None,
        "initialized": False,
        "auto_update": False,
        "update_schedule": dict(DEFAULT_SCHEDULE),
        "auto_update_enabled_at": None,
        "last_triggered_slot": None,
        "last_error": None,
        "progress": {"source_commit": None, "processed_roots": []},
        # Snapshot the per-file version indexes were last synced to.
        "versions_head": None,
        "active_task_id": None,
        "last_run": None,
        "initial_operation_id": None,  # First registered Compile task; missing on legacy targets.
        "content_updated_at": None,  # Business-file change time in Unix seconds; None when unknown.
        "first_compile_created_at": None,  # First task creation in Unix seconds; None when unknown.
        "execution_context": {"executor": None, "credential": None, "args": {}},
    }


def summarize(record: Dict[str, Any]) -> Dict[str, Any]:
    summary = {key: record.get(key) for key in SUMMARY_FIELDS}
    summary["compile_type"] = record.get("compile_type", "compile")
    summary["conflicts_num"] = record.get("conflicts_num", 0)
    summary["running"] = bool(record.get("active_task_id"))
    return summary


class CompileStore(Protocol):
    """Target, operation and version-index persistence used by Compile.

    Edits save on clean exit. Object locks serialize their read-modify-write
    scopes; lock tokens are opaque and belong to the selected implementation.
    """

    async def register(self, account_id: str, to: str) -> str: ...
    async def find_id(
        self, account_id: str, to: str, *, include_deleted: bool = False
    ) -> Optional[str]: ...
    async def get(self, account_id: str, target_id: str) -> Optional[Dict[str, Any]]: ...
    async def get_by_to(
        self, account_id: str, to: str, *, include_deleted: bool = False
    ) -> Optional[Dict[str, Any]]: ...
    def edit(self, account_id: str, target_id: str) -> AsyncContextManager[Dict[str, Any]]: ...
    async def list_summaries(
        self, account_id: str, inherited_lease: Any = None
    ) -> List[Dict[str, Any]]: ...
    async def list_accounts(self) -> List[str]: ...
    async def mark_content_updated(self, account_id: str, target_id: str) -> None: ...

    async def get_operation(
        self, account_id: str, target_id: str, operation_id: str
    ) -> Optional[Dict[str, Any]]: ...
    async def put_operation(
        self, account_id: str, target_id: str, operation: Dict[str, Any]
    ) -> None: ...
    async def list_operations(self, account_id: str, target_id: str) -> List[Dict[str, Any]]: ...
    def lock_operation(
        self,
        account_id: str,
        target_id: str,
        operation_id: str,
        phase: Literal["create", "finish"],
    ) -> AsyncContextManager[None]: ...
    async def get_outputs(
        self, account_id: str, target_id: str, operation_id: str
    ) -> Optional[dict]: ...
    async def merge_outputs(
        self,
        account_id: str,
        target_id: str,
        operation_id: str,
        outputs: List[dict],
        *,
        versions_available: bool = True,
    ) -> None: ...

    async def get_version_index(
        self, account_id: str, target_id: str, uri: str
    ) -> Optional[Dict[str, Any]]: ...
    async def list_version_indexes(
        self, account_id: str, target_id: str
    ) -> List[Dict[str, Any]]: ...
    async def put_version_index(
        self, account_id: str, target_id: str, index: Dict[str, Any], lock_token: Any = None
    ) -> None: ...
    async def delete_version_index(self, account_id: str, target_id: str, uri: str) -> None: ...
    async def clear_output_data(self, account_id: str, target_id: str) -> None: ...
    def lock_version_index(
        self, account_id: str, target_id: str, uri: str, inherited_lock: Any = None
    ) -> AsyncContextManager[Any]: ...


class CompileStateStore:
    """JSON persistence for Compile targets; all writes take the file's PathLock."""

    def __init__(self, agfs: Any) -> None:
        self._agfs = agfs if isinstance(agfs, AsyncAGFSClient) else AsyncAGFSClient(agfs)

    # ---------- paths ----------

    @staticmethod
    def root(account_id: str) -> str:
        return f"/local/{account_id}/_system/compile"

    def _targets_path(self, account_id: str) -> str:
        return f"{self.root(account_id)}/target_folders.json"

    def _target_dir(self, account_id: str, target_id: str) -> str:
        return f"{self.root(account_id)}/target_folders/{target_id}"

    def _record_path(self, account_id: str, target_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/state.json"

    def _operation_path(self, account_id: str, target_id: str, operation_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/operations/{operation_id}.json"

    # ---------- primitives ----------

    async def read_json(self, path: str) -> Optional[Any]:
        try:
            raw = await self._agfs.read(path)
        except (AGFSNotFoundError, FileNotFoundError):
            return None
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)

    async def _ensure_parent(self, path: str, fs_ctx: Any = None) -> None:
        try:
            await self._agfs.ensure_parent_dirs(path, fs_ctx=fs_ctx)
        except AGFSAlreadyExistsError:
            pass

    async def write_json(self, path: str, value: Any, lease: Any = None) -> None:
        """Write JSON; pass the held lease when ``path`` is already locked by the caller."""
        fs_ctx = fs_ctx_from_agfs_path(path)
        if lease is not None:
            fs_ctx["lease_ref"] = lease["lease_ref"]
        await self._ensure_parent(path, fs_ctx)
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        await self._agfs.write(path, data, fs_ctx=fs_ctx)

    @asynccontextmanager
    async def locked(self, path: str, inherited_lease: Any = None) -> AsyncIterator[Any]:
        """Lock a JSON path, or reuse a caller-owned tree lease covering that path."""
        if inherited_lease is not None:
            yield inherited_lease
            return
        await self._ensure_parent(path)
        lease = await self._agfs.pathlock_acquire_exact(path, timeout_secs=LOCK_TIMEOUT_SECONDS)
        try:
            yield lease
        finally:
            await self._agfs.pathlock_release(lease)

    # ---------- target_folders.json ----------

    async def list_summaries(
        self, account_id: str, inherited_lease: Any = None
    ) -> List[Dict[str, Any]]:
        path = self._targets_path(account_id)
        async with self.locked(path, inherited_lease):
            data = await self.read_json(path)
            return list((data or {}).get("targets", []))

    async def _sync_summary(self, account_id: str, record: Dict[str, Any]) -> None:
        """Update only this target's entry so concurrent writers never clobber each other."""
        path = self._targets_path(account_id)
        async with self.locked(path) as lease:
            data = await self.read_json(path)
            targets = [
                item for item in (data or {}).get("targets", []) if item.get("id") != record["id"]
            ]
            targets.append(summarize(record))
            await self.write_json(path, {"targets": targets}, lease)

    async def find_id(
        self, account_id: str, to: str, *, include_deleted: bool = False
    ) -> Optional[str]:
        for item in await self.list_summaries(account_id):
            if item.get("to") == to and (include_deleted or item.get("status") == "active"):
                return item.get("id")
        return None

    # ---------- state.json ----------

    async def get(self, account_id: str, target_id: str) -> Optional[Dict[str, Any]]:
        return await self.read_json(self._record_path(account_id, target_id))

    async def get_by_to(
        self, account_id: str, to: str, *, include_deleted: bool = False
    ) -> Optional[Dict[str, Any]]:
        target_id = await self.find_id(account_id, to, include_deleted=include_deleted)
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
        """Activate the path's persistent identity, resetting deleted output state."""
        path = self._targets_path(account_id)
        async with self.locked(path) as lease:
            data = await self.read_json(path)
            targets = list((data or {}).get("targets", []))
            existing = next((item["id"] for item in targets if item["to"] == to), None)
            if existing is None:
                target_id = f"target_{uuid4().hex[:16]}"
                record = new_record(target_id, to)
                await self.write_json(self._record_path(account_id, target_id), record)
                targets.append(summarize(record))
                await self.write_json(path, {"targets": targets}, lease)
                return target_id
        # State edits acquire the registry lock when saving, so release it first.
        async with self.edit(account_id, existing) as record:
            if record["status"] == "deleted":
                defaults = new_record(existing, to)
                for key in (
                    "status",
                    "initialized",
                    "progress",
                    "versions_head",
                    "active_task_id",
                    "last_run",
                    "content_updated_at",
                    "conflicts_num",
                    "last_error",
                    "auto_update",
                    "auto_update_enabled_at",
                    "last_triggered_slot",
                ):
                    record[key] = defaults[key]
                record["execution_context"]["credential"] = None
        return existing

    async def mark_content_updated(self, account_id: str, target_id: str) -> None:
        """Save the observed business-file change time and synchronize its summary."""
        async with self.edit(account_id, target_id) as record:
            record["content_updated_at"] = time.time()

    # ---------- operations ----------

    async def get_operation(
        self, account_id: str, target_id: str, operation_id: str
    ) -> Optional[Dict[str, Any]]:
        return await self.read_json(self._operation_path(account_id, target_id, operation_id))

    async def put_operation(
        self, account_id: str, target_id: str, operation: Dict[str, Any]
    ) -> None:
        await self.write_json(
            self._operation_path(account_id, target_id, operation["operation_id"]), operation
        )

    @asynccontextmanager
    async def lock_operation(
        self,
        account_id: str,
        target_id: str,
        operation_id: str,
        phase: Literal["create", "finish"],
    ) -> AsyncIterator[None]:
        """Serialize one operation phase; creation and finalization use separate locks."""
        async with self.locked(
            f"{self._operation_path(account_id, target_id, operation_id)}.{phase}"
        ):
            yield

    def _outputs_path(self, account_id: str, target_id: str, operation_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/operation_outputs/{operation_id}.json"

    async def get_outputs(
        self, account_id: str, target_id: str, operation_id: str
    ) -> Optional[dict]:
        path = self._outputs_path(account_id, target_id, operation_id)
        async with self.locked(path):
            return await self.read_json(path)

    async def merge_outputs(
        self,
        account_id: str,
        target_id: str,
        operation_id: str,
        outputs: List[dict],
        *,
        versions_available: bool = True,
    ) -> None:
        """Persist one reference per output URI; retries preserve already recorded outputs.

        Null version/snapshot references indicate unavailable versions. URI paths
        stay fixed at production time so moving a live file cannot change history.
        """
        path = self._outputs_path(account_id, target_id, operation_id)
        async with self.locked(path) as lease:
            data = await self.read_json(path) or {"outputs": []}
            merged = {item["uri"]: item for item in data["outputs"]}
            merged.update((item["uri"], item) for item in outputs)
            data["outputs"] = [merged[uri] for uri in sorted(merged)]
            if not versions_available or any(item["snapshot_id"] is None for item in outputs):
                data["versions_available"] = False
            await self.write_json(path, data, lease)

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

    async def list_operations(self, account_id: str, target_id: str) -> List[Dict[str, Any]]:
        directory = f"{self._target_dir(account_id, target_id)}/operations"
        operations = []
        for name in await self.list_dir(directory):
            operation = await self.read_json(f"{directory}/{name}")
            if operation is not None:
                operations.append(operation)
        operations.sort(
            key=lambda operation: (operation.get("created_at") or 0, operation["operation_id"]),
            reverse=True,
        )
        return operations

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

    # ---------- file version indexes ----------

    def _versions_dir(self, account_id: str, target_id: str) -> str:
        return f"{self._target_dir(account_id, target_id)}/file_versions"

    def _version_path(self, account_id: str, target_id: str, uri: str) -> str:
        name = hashlib.sha256(uri.encode("utf-8")).hexdigest()
        return f"{self._versions_dir(account_id, target_id)}/{name}.json"

    async def get_version_index(
        self, account_id: str, target_id: str, uri: str
    ) -> Optional[Dict[str, Any]]:
        """Read the stored index for an output URI, or None if it is absent."""
        return await self.read_json(self._version_path(account_id, target_id, uri))

    async def list_version_indexes(self, account_id: str, target_id: str) -> List[Dict[str, Any]]:
        """Read the target's existing indexes, ignoring files removed during enumeration."""
        directory = self._versions_dir(account_id, target_id)
        indexes = []
        for name in await self.list_dir(directory):
            index = await self.read_json(f"{directory}/{name}")
            if index is not None:
                indexes.append(index)
        return indexes

    async def put_version_index(
        self, account_id: str, target_id: str, index: Dict[str, Any], lock_token: Any = None
    ) -> None:
        """Save the index at its URI, using the caller's version lock when supplied."""
        await self.write_json(
            self._version_path(account_id, target_id, index["uri"]), index, lock_token
        )

    async def delete_version_index(self, account_id: str, target_id: str, uri: str) -> None:
        """Remove an output's index; a missing index is already deleted."""
        await self.remove(self._version_path(account_id, target_id, uri))

    async def clear_output_data(self, account_id: str, target_id: str) -> None:
        """Remove output manifests and version indexes, retaining operation history."""
        await self.remove(self._versions_dir(account_id, target_id), recursive=True)
        await self.remove(
            f"{self._target_dir(account_id, target_id)}/operation_outputs", recursive=True
        )

    @asynccontextmanager
    async def lock_version_index(
        self, account_id: str, target_id: str, uri: str, inherited_lock: Any = None
    ) -> AsyncIterator[Any]:
        """Lock an index, reusing a covering content lock during whole-account restore.

        Pass the yielded opaque token to put_version_index while this scope is held.
        """
        async with self.locked(
            self._version_path(account_id, target_id, uri), inherited_lock
        ) as token:
            yield token


# ==========================================================================
# Incremental lifecycle
# ==========================================================================


CONTROL_ARGS = ("mode", "auto_update", "update_schedule")
MAX_SCOPES = 1000
GIT_RETRIES = 3
SNAPSHOT_BUSY_SECONDS = 30.0
TERMINAL = frozenset({"completed", "failed", "cancelled"})
_RESULT_KEYS = {"added": "created", "modified": "updated", "deleted": "deleted"}
_NULL_OID = "0" * 40


def task_ctx(account_id: str, user_id: str) -> RequestContext:
    return RequestContext(
        user=UserIdentifier(account_id=account_id, user_id=user_id),
        role=Role.USER,
        bypass_acl=True,
    )


def overlaps(a: str, b: str) -> bool:
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


class DailySchedule(BaseModel):
    """Beijing-time trigger time; weekly adds ``weekday`` (1=Monday … 7=Sunday)."""

    model_config = ConfigDict(extra="forbid")

    frequency: Literal["daily"]
    hour: int = Field(default=0, ge=0, le=23)
    minute: int = Field(default=0, ge=0, le=59)


class WeeklySchedule(DailySchedule):
    frequency: Literal["weekly"]
    weekday: int = Field(default=1, ge=1, le=7)


UpdateSchedule = Annotated[Union[DailySchedule, WeeklySchedule], Field(discriminator="frequency")]
DEFAULT_SCHEDULE = {"frequency": "daily", "hour": 0, "minute": 0}


def parse_schedule(value: Any) -> Dict[str, Any]:
    """Validate a schedule and fill defaults."""
    try:
        return TypeAdapter(UpdateSchedule).validate_python(value).model_dump()
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        field = "".join(f".{part}" for part in error["loc"][1:])  # loc[0] is the frequency tag
        raise InvalidArgumentError(f"args.update_schedule{field}: {error['msg']}") from exc


def split_control_args(args: Optional[Mapping[str, Any]]) -> tuple[dict, dict]:
    """Separate kernel control args from Runtime args."""
    runtime = dict(args or {})
    control = {key: runtime.pop(key) for key in CONTROL_ARGS if key in runtime}
    if control.get("mode", "incremental") not in {"incremental", "full"}:
        raise InvalidArgumentError("args.mode must be 'incremental' or 'full'")
    if "update_schedule" in control:
        control["update_schedule"] = parse_schedule(control["update_schedule"])
    if not isinstance(control.get("auto_update", False), bool):
        raise InvalidArgumentError("args.auto_update must be a boolean")
    return control, runtime


def compress_scopes(changes: List[Dict[str, str]], added_roots: List[str]) -> List[Dict[str, Any]]:
    """Roots processed in full become one recursive scope; other changes stay per file.

    While the plan exceeds ``MAX_SCOPES``, the densest parent directory is folded
    into a recursive ``rescan`` scope that keeps its underlying change types.
    """
    scopes: List[Dict[str, Any]] = [
        {"uri": root, "change_type": "added", "recursive": True} for root in added_roots
    ]
    files = [
        {"uri": c["uri"], "change_type": c["change_type"], "recursive": False} for c in changes
    ]
    while len(scopes) + len(files) > MAX_SCOPES:
        counts: Dict[str, int] = {}
        for item in files:
            parent = item["uri"].rsplit("/", 1)[0]
            counts[parent] = counts.get(parent, 0) + 1
        parent = max(counts, key=counts.get)
        folded = [item for item in files if item["uri"].rsplit("/", 1)[0] == parent]
        files = [item for item in files if item not in folded]
        files.append(
            {
                "uri": parent,
                "change_type": "rescan",
                "recursive": True,
                "change_types": sorted({item["change_type"] for item in folded}),
            }
        )
    return scopes + files


def new_operation(
    operation_id: str,
    operation_type: str,
    params: Dict[str, Any],
    actor: str,
    *,
    status: str = "running",
) -> Dict[str, Any]:
    """Build one directory operation; timestamps use Unix seconds and conflicts default to zero."""
    now = time.time()
    return {
        "operation_id": operation_id,
        "operation_type": operation_type,
        "actor": actor,
        "status": status,
        "created_at": now,
        "started_at": now if status == "running" else None,
        "finished_at": None,
        "params": params,
        "result": None,
        "conflicts_num": 0,
        "error": None,
    }


def new_compile_operation(task: Any, record: Dict[str, Any]) -> Dict[str, Any]:
    """Build a queued Compile operation linked to its execution task and original trigger."""
    request = task.meta["request"]
    trigger = request["compile"]["trigger"]
    operation_type = "schedule_compile" if trigger == "scheduled" else "manual_compile"
    if record.get("initial_operation_id") == task.task_id:
        operation_type = "initial"
    return new_operation(
        task.task_id, operation_type, _params(request), task.user_id, status="queued"
    ) | {
        "task_id": task.task_id,
        "trigger": trigger,
        "created_at": task.created_at,
        "target_base": None,
        "target_head": None,
        "snapshot_committed": False,
        "reindex_task_id": None,
        "base": None,
        "head": None,
        "changed_roots": [],
        "changes": None,
    }


class CompileLifecycle:
    def __init__(self, store: CompileStore, viking_fs: Any) -> None:
        self.store = self._store = store
        self._fs = viking_fs
        self.versions = CompileVersions(store, viking_fs)
        # Installed by CompileScheduler: async (account_id, record) -> None.
        self.reschedule = None
        # Installed by CompileScheduler: async (ctx) -> (executor, credential).
        self.save_credential = None

    # ---------- submission ----------

    async def prepare(
        self,
        payload: Dict[str, Any],
        ctx: RequestContext,
        *,
        instruction_set: bool,
        scheduled_target_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Save the registry for a manual request and return the task payload.

        Scheduled requests are built from an already locked record, so they
        only receive Compile metadata and never write the registry here.
        """
        control, runtime_args = split_control_args(payload.get("args"))
        task_payload = {key: value for key, value in payload.items() if key != "args"}
        if scheduled_target_id is not None:
            target_id, trigger = scheduled_target_id, "scheduled"
            record = await self._store.get(ctx.account_id, target_id)
        else:
            record = await self._save_submission(
                payload, control, runtime_args, instruction_set, ctx
            )
            target_id, trigger = record["id"], "manual"
            runtime_args = {**record["execution_context"]["args"], **runtime_args}
        if runtime_args:
            task_payload["args"] = runtime_args
        task_payload["compile"] = {
            "target_id": target_id,
            "deleted_at": record["deleted_at"],
            "mode": control.get("mode", "incremental"),
            "trigger": trigger,
        }
        return task_payload

    async def _save_submission(
        self,
        payload: Dict[str, Any],
        control: Dict[str, Any],
        runtime_args: Dict[str, Any],
        instruction_set: bool,
        ctx: RequestContext,
    ) -> Dict[str, Any]:
        """Save submission settings and return their state, including the deletion boundary."""
        to, sources = payload["to"], payload["from"]
        await self.validate_layout(ctx.account_id, to, sources)
        target_id = await self._store.register(ctx.account_id, to)
        async with self._store.edit(ctx.account_id, target_id) as record:
            if record["auto_update"] and record["skill"] not in {None, payload["skill"]}:
                raise ConflictError(
                    "Skill differs from the scheduled target; change it in update settings",
                    resource="COMPILE_SKILL_MISMATCH",
                )
            self.apply_sources(record, sources, payload["skill"])
            if instruction_set and ("auto_update" in control or "update_schedule" in control):
                record["instruction"] = payload.get("instruction") or None
            persisted = {k: v for k, v in runtime_args.items() if k != "last_compile_time"}
            if persisted:
                record["execution_context"]["args"] = persisted
            if "auto_update" in control or "update_schedule" in control:
                await self.apply_schedule(
                    record,
                    ctx,
                    control.get("auto_update", record["auto_update"]),
                    control.get("update_schedule", record["update_schedule"]),
                )
        if self.reschedule is not None:
            await self.reschedule(ctx.account_id, record)
        return record

    async def validate_layout(self, account_id: str, to: str, sources: List[str]) -> None:
        for source in sources:
            if overlaps(source, to):
                raise InvalidArgumentError(f"Compile source overlaps target: {source}")
        for item in await self._store.list_summaries(account_id):
            if item.get("status") == "active" and item["to"] != to and overlaps(item["to"], to):
                raise InvalidArgumentError(f"Compile target nests another target: {item['to']}")

    @staticmethod
    def apply_sources(record: Dict[str, Any], sources: List[str], skill: str) -> None:
        if record["skill"] not in {None, skill}:
            record["progress"]["processed_roots"] = []
        record["progress"]["processed_roots"] = [
            root for root in record["progress"]["processed_roots"] if root in sources
        ]
        record["from"] = list(sources)
        record["skill"] = skill

    async def apply_schedule(
        self,
        record: Dict[str, Any],
        ctx: RequestContext,
        auto_update: bool,
        schedule: Dict[str, Any],
    ) -> None:
        from openviking.service.compile_scheduler import now_iso

        changed = auto_update != record["auto_update"] or schedule != record["update_schedule"]
        if auto_update:
            if self.save_credential is None:
                # Credentials are encrypted only on scheduler instances, where they are decrypted.
                raise FailedPreconditionError(
                    "Scheduled Compile is disabled on this instance "
                    "(compile_api.scheduler_enabled=false)",
                    details={"reason": "COMPILE_SCHEDULER_DISABLED"},
                )
            executor, credential = await self.save_credential(ctx)
            record["execution_context"]["executor"] = executor
            record["execution_context"]["credential"] = credential
            record["last_error"] = None
        else:
            record["execution_context"]["credential"] = None
        if changed:
            record["auto_update_enabled_at"] = now_iso() if auto_update else None
        record["auto_update"] = auto_update
        record["update_schedule"] = schedule

    async def record_created(self, task: Any) -> None:
        """Claim the first Compile identity and earliest creation time under the target lock."""
        meta = task.meta["request"].get("compile")
        if meta is None:
            return
        async with self._store.edit(task.account_id, meta["target_id"]) as record:
            self._record_identity(task, record)

    @staticmethod
    def _record_identity(task: Any, record: Dict[str, Any]) -> None:
        """Retain the target's first task identity and earliest known creation time."""
        if "initial_operation_id" in record and record["initial_operation_id"] is None:
            record["initial_operation_id"] = task.task_id
        # Missing fields identify legacy targets whose first creation time is unknown.
        if "first_compile_created_at" in record:
            first = record["first_compile_created_at"]
            record["first_compile_created_at"] = (
                min(first, task.created_at) if first is not None else task.created_at
            )

    async def record_queued(
        self, task: Any, *, locked_record: Optional[Dict[str, Any]] = None
    ) -> None:
        """Save queued history and outputs under the target lock, reusing a caller-held record."""
        meta = task.meta["request"]["compile"]
        if locked_record is None:
            async with self._store.edit(task.account_id, meta["target_id"]) as record:
                await self.record_queued(task, locked_record=record)
            return
        if not self._task_is_current(locked_record, task):
            return
        async with self._store.lock_operation(
            task.account_id, meta["target_id"], task.task_id, "create"
        ):
            run = await self._store.get_operation(task.account_id, meta["target_id"], task.task_id)
            if run is None:
                self._record_identity(task, locked_record)
                await self._store.merge_outputs(
                    task.account_id, meta["target_id"], task.task_id, []
                )
                await self._store.put_operation(
                    task.account_id,
                    meta["target_id"],
                    new_compile_operation(task, locked_record),
                )

    # ---------- execution hooks ----------

    @staticmethod
    def _task_is_current(record: Dict[str, Any], task: Any) -> bool:
        """Only submissions registered after the last deletion can use active output state."""
        return record["status"] == "active" and (
            task.meta["request"]["compile"]["deleted_at"] == record["deleted_at"]
        )

    @asynccontextmanager
    async def _locked_output(self, to: str, ctx: RequestContext) -> AsyncIterator[Any]:
        """Yield a tree lease for snapshot/index reconciliation, never Runtime execution.

        Busy processors raise ResourceBusyError; every exit releases the lease.
        """
        try:
            lease = await self._fs._async_agfs.pathlock_acquire_tree(
                self._fs._uri_to_path(to, ctx=ctx)
            )
        except LockAcquisitionError as exc:
            raise ResourceBusyError("Compile output is being processed", uri=to) from exc
        try:
            yield lease
        finally:
            await self._fs._async_agfs.pathlock_release(lease)

    async def on_start(self, task: Any) -> Any:
        """Claim the target across instances, then snapshot and plan once."""
        meta = task.meta["request"].get("compile")
        if meta is None:
            return "proceed"
        account_id, target_id = task.account_id, meta["target_id"]
        async with self._store.edit(account_id, target_id) as record:
            if not self._task_is_current(record, task):
                return ExternalTaskSnapshot(
                    status="cancelled", error_message="Compile target was deleted"
                )
            owner = record["active_task_id"]
            if owner and owner != task.task_id:
                other = await self._store.get_operation(account_id, target_id, owner)
                if other is not None and other["status"] not in TERMINAL:
                    return "busy"
            record["active_task_id"] = task.task_id
            await self.record_queued(task, locked_record=record)
        run = await self._store.get_operation(account_id, target_id, task.task_id)
        if run is None or run["status"] == "queued":
            try:
                run = await self._begin_run(task, record, run)
            except ResourceBusyError:
                return "busy"
        if run["status"] in TERMINAL or run["status"] == "committing":
            # Committing runs resume locally without querying the Runtime session.
            return ExternalTaskSnapshot(
                status="completed" if run["status"] == "committing" else run["status"],
                result=run.get("result") or {},
            )
        return "proceed"

    async def _begin_run(
        self, task: Any, record: Dict[str, Any], run: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        meta, request = task.meta["request"]["compile"], task.meta["request"]
        ctx = task_ctx(task.account_id, task.user_id)
        incremental = meta["mode"] == "incremental"
        run = run or new_compile_operation(task, record)
        run["actor"] = task.user_id
        run["status"] = "running"
        run["started_at"] = time.time()
        paths = [request["to"], *(request["from"] if incremental else [])]
        async with self._locked_output(request["to"], ctx) as lease:
            run["target_base"] = await commit_snapshot(
                self._fs,
                paths,
                f"compile {task.task_id} start",
                ctx,
                wait_busy=False,
                lease_ref=lease,
            )
            await self.versions.record_external(task.account_id, record, run["target_base"])
            if incremental:
                run.update(await self._plan(record, request["from"], run["target_base"], ctx))
                idle = not run["changes"]["scopes"] and not request.get("instruction")
                if idle and record["initialized"]:
                    run["status"] = "completed"
            await self._store.put_operation(task.account_id, meta["target_id"], run)
        return run

    async def _plan(
        self, record: Dict[str, Any], sources: List[str], head: Optional[str], ctx: RequestContext
    ) -> Dict[str, Any]:
        base = record["progress"]["source_commit"]
        processed = set(record["progress"]["processed_roots"])
        compared = [source for source in sources if base and source in processed]
        added_roots = [source for source in sources if source not in compared]
        changes = (
            await self._fs.diff_paths(dirs=compared, base=base, head=head, ctx=ctx)
            if compared and head
            else []
        )
        summary = dict.fromkeys(_RESULT_KEYS, 0)
        for change in changes:
            summary[change["change_type"]] += 1
        changed_roots = added_roots + [
            root for root in compared if any(c["uri"].startswith(f"{root}/") for c in changes)
        ]
        scopes = compress_scopes(changes, added_roots)
        return {
            "base": base,
            "head": head,
            "changed_roots": changed_roots,
            "changes": {
                "summary": summary,
                "scope_count": len(scopes),
                "scopes": scopes,
            },
        }

    async def runtime_changes(
        self, payload: Mapping[str, Any], account_id: str, task_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return ``args.changes`` for an incremental task, read from its run record."""
        meta = payload.get("compile")
        if not meta or meta["mode"] != "incremental":
            return None
        run = await self._store.get_operation(account_id, meta["target_id"], task_id)
        if run is None or run.get("changes") is None:
            return None
        return {
            "base": run["base"],
            "head": run["head"],
            "scopes": run["changes"]["scopes"],
        }

    async def on_finish(self, task: Any, snapshot: ExternalTaskSnapshot) -> ExternalTaskSnapshot:
        """Commit successful output and accept reindex before completing the run.

        Saved snapshot and reindex identities allow interrupted finalization to
        resume without publishing a second output version or reindex request.
        """
        meta, request = task.meta["request"].get("compile"), task.meta["request"]
        if meta is None:
            return snapshot
        account_id, target_id = task.account_id, meta["target_id"]
        async with self._store.lock_operation(account_id, target_id, task.task_id, "finish"):
            run = await self._store.get_operation(account_id, target_id, task.task_id)
            if run is not None and run["finished_at"] is None:
                ctx = task_ctx(task.account_id, task.user_id)
                record = await self._store.get(account_id, target_id)
                if not self._task_is_current(record, task):
                    snapshot = ExternalTaskSnapshot(
                        status=run["status"] if run["status"] in TERMINAL else "cancelled",
                        result=run.get("result"),
                        error_message="Compile target was deleted",
                    )
                elif run["started_at"] is None or run["status"] == "completed":
                    pass  # Unstarted and unchanged runs have no output to publish.
                elif snapshot.status == "completed":
                    from openviking.server.dependencies import get_service
                    from openviking.service.reindex_executor import REINDEX_ENQUEUE_ERROR
                    from openviking.service.task_tracker import get_task_tracker

                    if run["status"] != "committing":
                        run["status"] = "committing"
                        await self._store.put_operation(account_id, target_id, run)
                    # Reindex can own content locks once versions are durable.
                    if not run.get("reindex_task_id"):
                        async with self._locked_output(request["to"], ctx) as lease:
                            if not run.get("snapshot_committed"):
                                run["target_head"] = await self._commit(
                                    [request["to"]],
                                    f"compile {task.task_id} finish",
                                    ctx,
                                    lease_ref=lease,
                                )
                                run["snapshot_committed"] = True
                                await self._store.put_operation(account_id, target_id, run)
                            head = run["target_head"]
                            changes = (
                                await self._fs.diff_paths(
                                    dirs=[request["to"]],
                                    base=run["target_base"],
                                    head=head,
                                    ctx=ctx,
                                )
                                if head and run["target_base"]
                                else []
                            )
                            run["result"] = dict.fromkeys(_RESULT_KEYS.values(), 0)
                            for change in changes:
                                run["result"][_RESULT_KEYS[change["change_type"]]] += 1
                            record = await self._store.get(account_id, target_id)
                            await self.versions.record_compile(
                                account_id, record, run, head, changes
                            )
                            if (
                                any(is_versioned(change["uri"]) for change in changes)
                                and (record.get("content_updated_at") or 0) < run["started_at"]
                            ):
                                await self._store.mark_content_updated(account_id, target_id)
                    reindex_id = run.get("reindex_task_id")
                    previous = (
                        await get_task_tracker().get(
                            reindex_id, account_id=account_id, user_id=task.user_id
                        )
                        if reindex_id
                        else None
                    )
                    if reindex_id is None or (
                        previous is not None and previous.error == REINDEX_ENQUEUE_ERROR
                    ):
                        run["reindex_task_id"] = str(uuid4())
                        await self._store.put_operation(account_id, target_id, run)
                    await get_service().reindex(
                        uri=request["to"],
                        mode="semantic_and_vectors",
                        force=False,
                        recursive=True,
                        wait=False,
                        ctx=ctx,
                        task_id=run["reindex_task_id"],
                    )
                else:
                    if run["target_base"]:
                        await self._rollback(request["to"], run, ctx)
                    run["error"] = snapshot.error_message or snapshot.status
                run["status"] = snapshot.status
                run["finished_at"] = time.time()
                (run.get("changes") or {}).pop("scopes", None)
                await self._store.put_operation(account_id, target_id, run)
            await self._release(account_id, target_id, task, run)
            return snapshot

    async def _release(
        self, account_id: str, target_id: str, task: Any, run: Optional[Dict[str, Any]]
    ) -> None:
        first_success = False
        async with self._store.edit(account_id, target_id) as record:
            if not self._task_is_current(record, task):
                return
            task_id = task.task_id
            if (
                record["active_task_id"] == task_id
                or (
                    record["active_task_id"] is None
                    and (record.get("last_run") or {}).get("task_id") == task_id
                )
            ) and run is not None:
                record["last_run"] = {
                    "task_id": task_id,
                    "status": run["status"],
                    "finished_at": run["finished_at"],
                    "error": run.get("error"),
                }
                if run["status"] == "completed":
                    first_success = not record["initialized"]
                    record["initialized"] = True
                    if run.get("head"):
                        record["progress"] = {
                            "source_commit": run["head"],
                            "processed_roots": list(run["params"]["from"]),
                        }
                # Rollback snapshots include the ordinary edits retained by the run.
                synced = run.get("target_head") or run.get("target_base")
                if synced:
                    record["versions_head"] = synced
            if record["active_task_id"] == task_id:
                record["active_task_id"] = None
        if first_success and self.reschedule is not None:
            await self.reschedule(account_id, record)

    async def _rollback(self, to: str, run: Dict[str, Any], ctx: RequestContext) -> None:
        """Restore each changed file to its latest ordinary version or start snapshot.

        The tree lease protects both content and indexes, including interrupted retries.
        """
        target_id = await self._store.find_id(ctx.account_id, to)
        restored = []
        try:
            async with self._locked_output(to, ctx) as lease:
                head = await self._commit(
                    [to], f"compile {run['task_id']} rollback point", ctx, lease_ref=lease
                )
                if not head:
                    return
                changes = {
                    change["uri"]: change["change_type"]
                    for change in await self._fs.diff_paths(
                        dirs=[to], base=run["target_base"], head=head, ctx=ctx
                    )
                }
                indexes = {
                    index["uri"]: index
                    for index in await self.versions._indexes(ctx.account_id, target_id)
                }
                ordinary = {
                    uri: version
                    for uri, index in indexes.items()
                    if (version := self.versions._ordinary_version(index, run["task_id"]))
                }
                await self.versions.record_compile(
                    ctx.account_id,
                    await self._store.get(ctx.account_id, target_id),
                    run,
                    head,
                    [
                        {"uri": uri, "change_type": change}
                        for uri, change in changes.items()
                        if change != "deleted"
                    ],
                )
                indexes = {
                    index["uri"]: index
                    for index in await self.versions._indexes(ctx.account_id, target_id)
                }
                changed = False
                for uri in changes.keys() | ordinary.keys():
                    version = ordinary.get(uri)
                    if version is None and changes[uri] == "added":
                        await self._fs.rm(uri, ctx=ctx, lease_ref=lease)
                    else:
                        source = version["snapshot_id"] if version else run["target_base"]
                        content = await self._fs.show(
                            source, path=version["snapshot_path"] if version else uri, ctx=ctx
                        )
                        if await self._fs.exists(uri, ctx=ctx) and (
                            await self._fs.read_file_bytes(uri, ctx=ctx) == content
                        ):
                            continue
                        await self._fs.write_file_bytes(uri, content, ctx=ctx, lease_ref=lease)
                        restored.append(self._fs._uri_to_tree_path(uri, ctx=ctx))
                    changed = changed or is_versioned(uri)
                if changed:
                    await self._store.mark_content_updated(ctx.account_id, target_id)
                for uri, index in indexes.items():
                    retained = [
                        v
                        for v in index["versions"]
                        if not (v["meta"]["task_id"] == run["task_id"] and v["source"] == "compile")
                    ]
                    if retained == index["versions"]:
                        continue
                    if retained:
                        index["versions"] = retained
                        await self._store.put_version_index(ctx.account_id, target_id, index)
                    else:
                        await self._store.delete_version_index(ctx.account_id, target_id, uri)
                run["target_head"] = await self._commit(
                    [to], f"compile {run['task_id']} rollback", ctx, lease_ref=lease
                )
        finally:
            if restored:
                try:
                    await self._fs._schedule_restore_reindex_for_paths(
                        written_paths=restored, deleted_paths=[], project_dir=to, real_ctx=ctx
                    )
                except Exception:
                    logger.exception("Compile rollback reindex scheduling failed")
                    self._fs._schedule_vector_rebuild(written=restored, deleted=[], ctx=ctx)

    # ---------- git ----------

    async def _commit(
        self,
        paths: List[str],
        message: str,
        ctx: RequestContext,
        *,
        lease_ref: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        return await commit_snapshot(self._fs, paths, message, ctx, lease_ref=lease_ref)


async def retry_git(action: Any) -> Any:
    """Retry a Git ref update that lost a CAS race, with a short random backoff."""
    for attempt in range(GIT_RETRIES):
        try:
            return await action()
        except GitConcurrentCommitError:
            if attempt == GIT_RETRIES - 1:
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5))


async def commit_snapshot(
    viking_fs: Any,
    paths: List[str],
    message: str,
    ctx: RequestContext,
    *,
    lease_ref: Optional[Dict[str, Any]] = None,
    wait_busy: bool = True,
) -> Optional[str]:
    """Return the snapshot oid, or None when Git is disabled (Compile then runs in full).

    Busy paths wait up to ``SNAPSHOT_BUSY_SECONDS`` unless ``wait_busy=False``
    lets the caller defer work through its queue.
    """
    deadline = time.monotonic() + SNAPSHOT_BUSY_SECONDS
    kwargs = {"lease_ref": lease_ref} if lease_ref is not None else {}
    while True:
        try:
            result = await retry_git(
                lambda: viking_fs.commit(message=message, paths=paths, ctx=ctx, **kwargs)
            )
            break
        except AGFSNotSupportedError:
            return None
        except ResourceBusyError:
            if not wait_busy or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.2)
    oid = result.get("commit_oid")
    return None if not oid or oid == _NULL_OID else oid


def _params(request: Mapping[str, Any]) -> Dict[str, Any]:
    meta = request["compile"]
    return {
        "from": request["from"],
        "skill": request["skill"],
        "instruction": request.get("instruction"),
        "args": {"mode": meta["mode"], **(request.get("args") or {})},
    }


# ==========================================================================
# Per-file versions
# ==========================================================================


MAX_VERSIONS = 100
_DERIVED = frozenset({".abstract.md", ".overview.md", ".relations.json"})
_VERSION_META_DEFAULTS = {
    "task_id": None,
    "restored_from": None,
    "after_compile_snapshot": False,
}


def is_versioned(uri: str) -> bool:
    return uri.rsplit("/", 1)[-1] not in _DERIVED


class CompileVersions:
    """Version metadata preserves ordinary edits across Compile retries and rollback."""

    def __init__(self, store: CompileStore, viking_fs: Any) -> None:
        self._store = store
        self._fs = viking_fs
        # Installed by CompileScheduler: async (account_id, record) -> None.
        self.reschedule = None

    # ---------- index storage ----------

    async def load(self, account_id: str, target_id: str, uri: str) -> Optional[Dict[str, Any]]:
        return self._normalize_index(
            await self._store.get_version_index(account_id, target_id, uri)
        )

    @staticmethod
    def _normalize_index(index: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Normalize legacy metadata in memory without changing persisted versions on read."""
        if index is not None:
            index.pop("source_roots", None)
            for version in index["versions"]:
                meta = {
                    key: version.pop(key, default)
                    for key, default in _VERSION_META_DEFAULTS.items()
                }
                version["meta"] = {**meta, **(version.get("meta") or {})}
        return index

    async def _indexes(self, account_id: str, target_id: str) -> List[Dict[str, Any]]:
        return [
            self._normalize_index(index)
            for index in await self._store.list_version_indexes(account_id, target_id)
        ]

    async def append(
        self,
        account_id: str,
        target_id: str,
        uri: str,
        snapshot_id: str,
        entry: Dict[str, Any],
        *,
        operation_id: Optional[str] = None,
        metadata_lease: Any = None,
    ) -> Dict[str, Any]:
        """Append a file version under its lock, reusing a version on operation retries.

        A whole-account restore supplies its covering lease for version metadata.
        Independent operations collect exact references before publishing completion.
        """
        context = _operation_outputs.get()
        collecting = (
            context and context["account_id"] == account_id and target_id in context["outputs"]
        )
        operation_id = operation_id or (context["operation_id"] if collecting else None)
        async with self._store.lock_version_index(
            account_id, target_id, uri, metadata_lease
        ) as token:
            index = await self.load(account_id, target_id, uri) or {
                "uri": uri,
                "next": 1,
                "versions": [],
            }
            version = next(
                (
                    v
                    for v in index["versions"]
                    if operation_id and v["meta"].get("operation_id") == operation_id
                ),
                None,
            )
            if version is None:
                version = {
                    "version_id": f"v{index['next']}",
                    "snapshot_id": snapshot_id,
                    "snapshot_path": uri,
                    "created_at": time.time(),
                    **entry,
                    "meta": {**_VERSION_META_DEFAULTS, **entry.get("meta", {})},
                }
                if operation_id:
                    version["meta"]["operation_id"] = operation_id
                index["next"] += 1
                index["versions"] = (index["versions"] + [version])[-MAX_VERSIONS:]

            async def persist():
                await self._store.put_version_index(account_id, target_id, index, token)
                if collecting and operation_id == context["operation_id"]:
                    context["outputs"][target_id][uri] = {
                        "uri": uri,
                        "version_id": version["version_id"],
                        "snapshot_id": version["snapshot_id"],
                    }

            await run_to_completion(persist)
        return version

    # ---------- target resolution ----------

    async def owning_target(
        self, account_id: str, uri: str, *, metadata_lease: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Return the active target containing ``uri``, including its first run."""
        for item in await self._store.list_summaries(account_id, metadata_lease):
            to = item.get("to") or ""
            if item.get("status") == "active" and (uri == to or uri.startswith(f"{to}/")):
                record = await self._store.get(account_id, item["id"])
                if record is not None:
                    return record
        return None

    async def record_operation(
        self,
        uris: List[str],
        ctx: RequestContext,
        operation_type: str,
        params: Dict[str, Any],
        operation: Callable[[], Awaitable[Any]],
        *,
        recursive: bool = False,
    ) -> Any:
        """Execute one call and persist its outcome once per managed target.

        Parameters contain references, never file bodies. Recursive restores include
        overlapping targets; per-target URI lists prevent exposing sibling directories.
        Exceptions and cancellation retain their original behavior after history is saved.
        """
        entries = []
        operation_id = f"op_{uuid4().hex}"
        for target in await self._store.list_summaries(ctx.account_id):
            to = target["to"]
            affected = [uri for uri in uris if uri == to or uri.startswith(f"{to}/")]
            if recursive and any(uri == "viking://" or overlaps(uri, to) for uri in uris):
                affected = affected or [to]
            if target.get("status") == "active" and affected:
                entries.append(
                    (
                        target,
                        new_operation(
                            operation_id,
                            operation_type,
                            params | {"uris": affected},
                            ctx.user.user_id,
                        ),
                    )
                )
        if not entries:
            return await operation()
        for uri in uris:
            await self._fs._ensure_access(
                uri, ctx, action=AclAction.READ if recursive else AclAction.WRITE
            )
        context = {
            "account_id": ctx.account_id,
            "operation_id": operation_id,
            "outputs": {target["id"]: {} for target, _ in entries},
            "changed": set(),
        }
        token = _operation_outputs.set(context)
        outcome = {}
        try:
            for target, entry in entries:
                async with self._store.edit(ctx.account_id, target["id"]) as record:
                    if record["status"] == "active" and record["deleted_at"] == target.get(
                        "deleted_at"
                    ):
                        await self._store.merge_outputs(
                            ctx.account_id, target["id"], operation_id, []
                        )
                await self._store.put_operation(ctx.account_id, target["id"], entry)
            result = await operation()
        except BaseException as exc:
            outcome = {key: getattr(exc, key, []) for key in ("written_paths", "deleted_paths")}
            for _, entry in entries:
                entry["status"] = (
                    "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
                )
                entry["error"] = (
                    type(exc).__name__ if len(entries) > 1 else str(exc) or type(exc).__name__
                )
            raise
        else:
            outcome = result if isinstance(result, dict) else {}
            for _, entry in entries:
                entry["status"] = "completed"
            return result
        finally:
            _operation_outputs.reset(token)

            async def finish():
                for target, entry in entries:
                    entry["finished_at"] = time.time()
                    entry["result"] = {
                        key: outcome[key]
                        for key in ("result", "version_id", "written_bytes", "mode")
                        if key in outcome
                    }
                    for key in ("created", "updated", "unchanged", "conflicts"):
                        if isinstance(outcome.get(key), list):
                            entry["result"][key] = [
                                item
                                for item in outcome[key]
                                if (item.get("uri") if isinstance(item, dict) else item)
                                in entry["params"]["uris"]
                            ]
                    if recursive:
                        for key in ("written_paths", "deleted_paths"):
                            entry["result"][key] = [
                                path
                                for path in outcome.get(key, [])
                                if (self._fs._tree_path_to_uri(path).startswith(f"{target['to']}/"))
                            ]
                    async with self._store.edit(ctx.account_id, target["id"]) as record:
                        current = record["status"] == "active" and record[
                            "deleted_at"
                        ] == target.get("deleted_at")
                        if current:
                            await self._store.merge_outputs(
                                ctx.account_id,
                                target["id"],
                                operation_id,
                                list(context["outputs"][target["id"]].values()),
                            )
                        if (
                            current
                            and recursive
                            and any(
                                is_versioned(path)
                                for key in ("written_paths", "deleted_paths")
                                for path in entry["result"][key]
                            )
                        ):
                            record["content_updated_at"] = time.time()
                    await self._store.put_operation(ctx.account_id, target["id"], entry)

            try:
                await run_to_completion(finish)
            except Exception as exc:
                completed = all(entry["status"] == "completed" for _, entry in entries)
                for target, entry in entries:
                    if entry["status"] == "completed":
                        entry.update(
                            status="failed", error=f"Failed to persist operation outputs: {exc}"
                        )
                    entry["finished_at"] = time.time()
                    try:
                        await self._store.put_operation(ctx.account_id, target["id"], entry)
                    except Exception:
                        logger.exception("Failed to persist operation failure %s", operation_id)
                if completed:
                    raise
                logger.exception("Failed to persist operation outcome %s", operation_id)

    async def _managed(self, uri: str, ctx: RequestContext) -> Dict[str, Any]:
        record = await self.owning_target(ctx.account_id, uri)
        if record is None or not is_versioned(uri):
            raise FailedPreconditionError(
                f"Not a Compile output file: {uri}", details={"reason": "NOT_COMPILE_MANAGED"}
            )
        return record

    async def _running(self, account_id: str, record: Dict[str, Any]) -> bool:
        owner = record["active_task_id"]
        if not owner:
            return False
        run = await self._store.get_operation(account_id, record["id"], owner)
        return run is not None and run["status"] not in TERMINAL

    # ---------- write hooks ----------

    async def record_compile(
        self,
        account_id: str,
        record: Dict[str, Any],
        run: Dict[str, Any],
        head: str,
        changes: List[Dict[str, str]],
    ) -> None:
        """Register final Compile versions under the caller's output tree lease.

        Ordinary versions remain immutable; full runs record all sources, while
        incremental runs extend the latest version's provenance with changed roots.
        """
        if not head:
            await self._store.merge_outputs(
                account_id, record["id"], run["operation_id"], [], versions_available=False
            )
            return
        indexes = {index["uri"]: index for index in await self._indexes(account_id, record["id"])}
        ordinary = {
            uri: version
            for uri, index in indexes.items()
            if (version := self._ordinary_version(index, run["task_id"]))
        }
        changes = {change["uri"]: change["change_type"] for change in changes}
        full = run["params"]["args"]["mode"] == "full"
        manifest = await self._store.get_outputs(account_id, record["id"], run["operation_id"])
        outputs = list((manifest or {}).get("outputs", [])) + [
            {
                "uri": version["snapshot_path"],
                "version_id": version["version_id"],
                "snapshot_id": version["snapshot_id"],
            }
            for index in indexes.values()
            for version in index["versions"]
            if version["source"] == "compile" and version["meta"]["task_id"] == run["task_id"]
        ]
        recorded = {item["uri"] for item in outputs if item["snapshot_id"] is not None}
        try:
            # Ordinary edits can differ even when final output matches the start snapshot.
            for uri in changes.keys() | ordinary.keys():
                if not is_versioned(uri):
                    continue
                if uri in recorded:
                    continue
                index, version = indexes.get(uri), ordinary.get(uri)
                change_type = changes.get(uri, "modified")
                if version:
                    if version["meta"]["after_compile_snapshot"]:
                        continue
                    if change_type != "deleted":
                        try:
                            if not await self._changed(account_id, record["id"], uri, head):
                                continue
                        except (AGFSNotFoundError, NotFoundError):
                            change_type = "deleted"
                if change_type == "deleted":
                    if version is None:
                        await self._store.delete_version_index(account_id, record["id"], uri)
                    continue
                if index and any(
                    v["meta"]["task_id"] == run["task_id"] and v["source"] == "compile"
                    for v in index["versions"]
                ):
                    continue
                if full or not (index or {}).get("versions"):
                    roots = list(run["params"]["from"])
                else:
                    previous_roots = (
                        index["versions"][-1].get("provenance", {}).get("source_roots", [])
                    )
                    roots = sorted(set(previous_roots) | set(run["changed_roots"]))
                created = await self.append(
                    account_id,
                    record["id"],
                    uri,
                    head,
                    {
                        "source": "compile",
                        "actor": run["actor"],
                        "meta": {"task_id": run["task_id"]},
                        "provenance": {
                            "trigger": run["trigger"],
                            "skill": run["params"]["skill"],
                            "source_roots": roots,
                        },
                    },
                    operation_id=run["operation_id"],
                )
                outputs.append(
                    {
                        "uri": uri,
                        "version_id": created["version_id"],
                        "snapshot_id": created["snapshot_id"],
                    }
                )
        finally:
            error = sys.exc_info()[1]
            try:
                await run_to_completion(
                    lambda: self._store.merge_outputs(
                        account_id, record["id"], run["operation_id"], outputs
                    )
                )
            except BaseException:
                if error is None:
                    raise
                logger.exception("Failed to persist Compile outputs for %s", run["operation_id"])

    @staticmethod
    def _ordinary_version(index: Optional[Dict[str, Any]], task_id: str) -> Optional[dict]:
        """Return the run's latest ordinary version, or None; Compile entries are ignored."""
        for version in reversed((index or {}).get("versions", [])):
            if version["meta"]["task_id"] == task_id and version["source"] in {
                "manual",
                "agent",
                "restore",
            }:
                return version
        return None

    async def record_external(
        self, account_id: str, record: Dict[str, Any], target_base: Optional[str]
    ) -> None:
        """At Compile start, version output changes made outside Compile and content/write.

        Compares the snapshot the indexes were last synced to (``versions_head``)
        with this run's start snapshot; on first registration every existing
        file becomes ``v1``. Files whose content already matches their latest
        version (e.g. edits recorded by content/write) are skipped.
        """
        if not target_base:
            return
        changes = await self._fs.diff_paths(
            dirs=[record["to"]],
            base=record.get("versions_head"),
            head=target_base,
            ctx=task_ctx(account_id, "_system"),
        )
        changed = False
        for change in changes:
            uri = change["uri"]
            if not is_versioned(uri):
                continue
            if change["change_type"] == "deleted":
                changed = changed or await self.load(account_id, record["id"], uri) is not None
                await self._store.delete_version_index(account_id, record["id"], uri)
            elif await self._changed(account_id, record["id"], uri, target_base):
                changed = True
                await self.append(
                    account_id,
                    record["id"],
                    uri,
                    target_base,
                    {"source": "external", "actor": None, "provenance": {}},
                )
        if changed and record.get("versions_head"):
            await self._store.mark_content_updated(account_id, record["id"])

    async def record_write(
        self,
        uris: List[str],
        ctx: RequestContext,
        edit_source: str,
        *,
        lease_ref: Optional[Dict[str, Any]] = None,
        write: Optional[Callable[[], Awaitable[Any]]] = None,
        record_version: bool = True,
    ) -> Any:
        """Write under the content lease and register ordinary versions with provenance.

        Unversioned writes still record actual content changes.
        Run IDs select rollback content; after_compile_snapshot protects newer
        ordinary versions from a retry of the frozen finish snapshot.
        """
        grouped: Dict[str, List[str]] = {}
        previous = {}
        for uri in uris:
            record = await self.owning_target(ctx.account_id, uri)
            if record is None or not is_versioned(uri):
                continue
            grouped.setdefault(record["id"], []).append(uri)
            # Working bytes can differ from the latest version during Compile.
            if write is not None:
                previous[uri] = (
                    await self._fs.read_file_bytes(uri, ctx=ctx)
                    if await self._fs.exists(uri, ctx=ctx)
                    else None
                )
        written = await write() if write is not None else None
        changed = {}
        context = _operation_outputs.get()
        for target_id, targets in grouped.items():
            targets = [
                uri
                for uri in targets
                if write is None or previous[uri] != await self._fs.read_file_bytes(uri, ctx=ctx)
            ]
            if not targets:
                continue
            changed[target_id] = targets
            if write is not None:
                await self._store.mark_content_updated(ctx.account_id, target_id)
            if context and context["account_id"] == ctx.account_id:
                context["changed"].update(targets)
                if target_id in context["outputs"]:
                    for uri in targets:
                        context["outputs"][target_id][uri] = {
                            "uri": uri,
                            "version_id": None,
                            "snapshot_id": None,
                        }
        if not record_version:
            if edit_source == "compile":
                for target_id, targets in changed.items():
                    record = await self._store.get(ctx.account_id, target_id)
                    run = (
                        await self._store.get_operation(
                            ctx.account_id, target_id, record["active_task_id"]
                        )
                        if record["active_task_id"]
                        else None
                    )
                    if run and run["started_at"] is not None and run["target_base"] is None:
                        await self._store.merge_outputs(
                            ctx.account_id,
                            target_id,
                            run["operation_id"],
                            [
                                {"uri": uri, "version_id": None, "snapshot_id": None}
                                for uri in targets
                            ],
                            versions_available=False,
                        )
            return written
        if not changed:
            return written
        snapshot = await self._commit(
            [uri for targets in changed.values() for uri in targets],
            f"{edit_source} edit",
            ctx,
            lease_ref=lease_ref,
        )
        for target_id, targets in changed.items():
            record = await self._store.get(ctx.account_id, target_id)
            run_entry = await self._write_meta(ctx.account_id, record)
            for uri in targets:
                if snapshot:
                    await self.append(
                        ctx.account_id,
                        target_id,
                        uri,
                        snapshot,
                        {
                            "source": edit_source,
                            "actor": ctx.user.user_id,
                            "provenance": {"source_roots": [uri]},
                            "meta": run_entry,
                        },
                    )
        return written

    async def _write_meta(self, account_id: str, record: dict) -> dict:
        """Keep independently written versions associated with an active Compile for rollback."""
        run = (
            await self._store.get_operation(account_id, record["id"], record["active_task_id"])
            if record["active_task_id"]
            else None
        )
        if run and run["started_at"] is not None and run["finished_at"] is None:
            return {
                "task_id": run["task_id"],
                **({"after_compile_snapshot": True} if run.get("snapshot_committed") else {}),
            }
        return {}

    async def finish_batch(self, *, ctx: RequestContext, edit_source: str, lease_ref: Any) -> None:
        """Version the batch's changed files once, before its content lease is released."""
        context = _operation_outputs.get()
        if context and context["account_id"] == ctx.account_id and context["changed"]:
            await self.record_write(
                sorted(context["changed"]), ctx, edit_source, lease_ref=lease_ref
            )

    async def record_restored(
        self,
        result: dict,
        *,
        ctx: RequestContext,
        lease_ref: Any,
        metadata_lease: Any = None,
    ) -> None:
        """Version successful snapshot-restore writes under the writeback lease.

        Partial failures expose only their confirmed written paths. Their new commit
        supplies immutable bytes; deleted paths and unchanged files are not outputs.
        """
        context = _operation_outputs.get()
        if context is None or context["account_id"] != ctx.account_id:
            return
        snapshot = result.get("new_commit_oid")
        if not snapshot:
            return
        for path in result.get("written_paths", []):
            uri = self._fs._tree_path_to_uri(path)
            record = await self.owning_target(ctx.account_id, uri, metadata_lease=metadata_lease)
            if record is None or not is_versioned(uri):
                continue
            await self.append(
                ctx.account_id,
                record["id"],
                uri,
                snapshot,
                {
                    "source": "restore",
                    "actor": ctx.user.user_id,
                    "provenance": {"source_roots": [uri]},
                    "meta": await self._write_meta(ctx.account_id, record),
                },
                metadata_lease=metadata_lease,
            )

    async def _changed(self, account_id: str, target_id: str, uri: str, snapshot: str) -> bool:
        """Whether ``uri`` in ``snapshot`` differs from its latest version's content.

        Compares blob ids at each version's own path, so moved files (whose
        latest version still points at the old ``snapshot_path``) are handled.
        """
        index = await self.load(account_id, target_id, uri)
        if index is None or not index["versions"]:
            return True
        latest = index["versions"][-1]
        ctx = task_ctx(account_id, "_system")
        try:
            before = await self._fs.show_blob_raw(
                latest["snapshot_id"], path=latest["snapshot_path"], ctx=ctx
            )
        except Exception:
            return True
        after = await self._fs.show_blob_raw(snapshot, path=uri, ctx=ctx)
        return before["oid"] != after["oid"]

    async def move(self, account_id: str, from_uri: str, to_uri: str) -> None:
        """Re-key indexes moved within one target; entries keep their old snapshot_path."""
        source = await self.owning_target(account_id, from_uri)
        if source is None:
            return
        target = await self.owning_target(account_id, to_uri)
        for index in await self._indexes(account_id, source["id"]):
            uri = index["uri"]
            if uri != from_uri and not uri.startswith(f"{from_uri}/"):
                continue
            await self._store.delete_version_index(account_id, source["id"], uri)
            if target is not None and target["id"] == source["id"]:
                index["uri"] = to_uri + uri[len(from_uri) :]
                await self._store.put_version_index(account_id, source["id"], index)

    async def drop_all(self, account_id: str, target_id: str) -> None:
        await self._store.clear_output_data(account_id, target_id)

    # ---------- fs/rm and fs/mv ----------

    async def _targets_under(self, account_id: str, uri: str) -> List[Dict[str, Any]]:
        """Active targets equal to or inside ``uri`` (a removed/moved directory)."""
        return [
            item
            for item in await self._store.list_summaries(account_id)
            if item.get("status") == "active"
            and (item["to"] == uri or item["to"].startswith(f"{uri}/"))
        ]

    async def _reject_running(self, account_id: str, uri: str) -> None:
        for item in await self._targets_under(account_id, uri):
            record = await self._store.get(account_id, item["id"])
            if record is not None and await self._running(account_id, record):
                raise ConflictError(
                    f"Compile is running on {item['to']}", resource="COMPILE_RUNNING"
                )

    async def before_remove(self, uri: str, ctx: RequestContext) -> None:
        await self._reject_running(ctx.account_id, uri)

    async def after_remove(self, uri: str, ctx: RequestContext) -> None:
        """Whole targets become ``deleted`` (runs kept); a single file loses its index."""
        removed = await self._targets_under(ctx.account_id, uri)
        for item in removed:
            async with self._store.edit(ctx.account_id, item["id"]) as record:
                record["status"] = "deleted"
                record["deleted_at"] = time.time()
                record["auto_update"] = False
                record["active_task_id"] = None
                record["execution_context"]["credential"] = None
                await self.drop_all(ctx.account_id, item["id"])
            if self.reschedule is not None:
                await self.reschedule(ctx.account_id, record)
        if not removed:
            record = await self.owning_target(ctx.account_id, uri)
            if record is not None:
                for index in await self._indexes(ctx.account_id, record["id"]):
                    if index["uri"] == uri or index["uri"].startswith(f"{uri}/"):
                        await self._store.delete_version_index(
                            ctx.account_id, record["id"], index["uri"]
                        )
                if is_versioned(uri):
                    await self._store.mark_content_updated(ctx.account_id, record["id"])

    async def before_move(self, from_uri: str, to_uri: str, ctx: RequestContext) -> None:
        await self._reject_running(ctx.account_id, from_uri)
        for item in await self._targets_under(ctx.account_id, from_uri):
            record = await self._store.get(ctx.account_id, item["id"])
            moved_to = to_uri + item["to"][len(from_uri) :]
            for other in await self._store.list_summaries(ctx.account_id):
                if (
                    other.get("status") == "active"
                    and other["id"] != item["id"]
                    and overlaps(other["to"], moved_to)
                ):
                    raise InvalidArgumentError(f"Compile target would nest {other['to']}")
            if any(overlaps(source, moved_to) for source in record["from"]):
                raise InvalidArgumentError("Compile target would overlap its sources")

    async def after_move(self, from_uri: str, to_uri: str, ctx: RequestContext) -> None:
        """Rewrite moved targets' URIs (IDs stay); otherwise re-key moved file indexes."""
        if from_uri == to_uri:
            return
        moved = await self._targets_under(ctx.account_id, from_uri)
        for item in moved:
            async with self._store.edit(ctx.account_id, item["id"]) as record:
                record["to"] = to_uri + record["to"][len(from_uri) :]
                record["content_updated_at"] = time.time()
            for index in await self._indexes(ctx.account_id, item["id"]):
                await self._store.delete_version_index(ctx.account_id, item["id"], index["uri"])
                index["uri"] = record["to"] + index["uri"][len(item["to"]) :]
                await self._store.put_version_index(ctx.account_id, item["id"], index)
        if not moved:
            await self.move(ctx.account_id, from_uri, to_uri)
            for uri in {from_uri, to_uri}:
                record = await self.owning_target(ctx.account_id, uri)
                if record is not None and is_versioned(uri):
                    await self._store.mark_content_updated(ctx.account_id, record["id"])

    # ---------- queries and single-file restore ----------

    async def versions(
        self, uri: str, ctx: RequestContext, *, limit: int, before: Optional[str]
    ) -> Dict[str, Any]:
        """Return newest-first versions and the retained count before pagination."""
        await self._fs._ensure_access(uri, ctx)
        record = await self._managed(uri, ctx)
        index = await self.load(ctx.account_id, record["id"], uri)
        entries = list(reversed((index or {}).get("versions", [])))
        total = len(entries)
        current = entries[0]["version_id"] if entries else None
        if before:
            ids = [entry["version_id"] for entry in entries]
            entries = entries[ids.index(before) + 1 :] if before in ids else []
        page = entries[:limit]
        return {
            "uri": uri,
            "current_version": current,
            "total": total,
            "versions": page,
            "next_before_version": page[-1]["version_id"] if len(entries) > limit else None,
        }

    async def restore(
        self,
        uri: str,
        restore_version: str,
        current_version: str,
        ctx: RequestContext,
        write: Callable[[bytes, Callable[..., Awaitable[Any]]], Awaitable[Any]],
    ) -> Dict[str, Any]:
        """Restore ``uri`` from ``restore_version`` under the content lock.

        ``current_version`` must match; active Compile or mismatch raises ConflictError.
        ``write`` persists and refreshes as ``ctx``; return new version metadata or noop.
        """
        await self._fs._ensure_access(uri, ctx, action=AclAction.WRITE)
        record = await self._managed(uri, ctx)
        index = await self.load(ctx.account_id, record["id"], uri) or {"versions": []}
        source = next((v for v in index["versions"] if v["version_id"] == restore_version), None)
        if source is None:
            raise NotFoundError(restore_version, "file version")
        content = await self._fs.show(source["snapshot_id"], path=source["snapshot_path"], ctx=ctx)
        result: Dict[str, Any] = {}

        async def commit_restore(uris, *, lease_ref, write):
            """Check, write and version under the content lease before refresh."""
            async with self._store.lock_version_index(ctx.account_id, target_id, uri):
                record = await self._store.get(ctx.account_id, target_id)
                if await self._running(ctx.account_id, record):
                    raise ConflictError("Compile is running", resource="COMPILE_RUNNING")
                index = await self.load(ctx.account_id, target_id, uri) or {"versions": []}
                current = index["versions"][-1]["version_id"] if index["versions"] else None
                if current != current_version:
                    raise ConflictError(
                        f"Current version is {current}, not {current_version}",
                        resource="VERSION_CONFLICT",
                    )
                written = await self.record_write(
                    uris, ctx, "restore", write=write, record_version=False
                )
                snapshot = await self._commit(
                    uris, f"restore {uri} to {restore_version}", ctx, lease_ref=lease_ref
                )
            if not snapshot or not await self._changed(ctx.account_id, target_id, uri, snapshot):
                result.update(
                    uri=uri, version_id=current, meta=dict(_VERSION_META_DEFAULTS), result="noop"
                )
            else:
                version = await self.append(
                    ctx.account_id,
                    target_id,
                    uri,
                    snapshot,
                    {
                        "source": "restore",
                        "actor": ctx.user.user_id,
                        "meta": {"restored_from": restore_version},
                        "provenance": dict(source.get("provenance") or {}),
                    },
                )
                result.update(uri=uri, version_id=version["version_id"], meta=dict(version["meta"]))
            return written

        target_id = record["id"]

        async def restore_content():
            await write(content, commit_restore)
            return result

        return await self.record_operation(
            [uri],
            ctx,
            "restore",
            {"restore_version": restore_version, "current_version": current_version},
            restore_content,
        )

    async def _commit(
        self,
        uris: List[str],
        message: str,
        ctx: RequestContext,
        *,
        lease_ref: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        return await commit_snapshot(
            self._fs, uris, message, task_ctx(ctx.account_id, ctx.user.user_id), lease_ref=lease_ref
        )
