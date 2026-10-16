# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Compile targets: registry, incremental lifecycle and per-file versions.

State lives under ``/local/{account}/_system/compile/``: ``targets.json``
(summary index), ``targets/{id}/record.json`` (source of truth),
``targets/{id}/runs/{task_id}.json`` (one run record per Compile task) and
``targets/{id}/versions/{sha256(uri)}.json`` (one version index per output
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
import time
from contextlib import asynccontextmanager
from typing import (
    Annotated,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
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
from openviking.storage.acl import AclAction
from openviking.storage.errors import ResourceBusyError
from openviking_cli.exceptions import (
    ConflictError,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
)
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)


# ==========================================================================
# Registry storage
# ==========================================================================


LOCK_TIMEOUT_SECONDS = 30.0
MAX_FINISHED_RUNS = 100
SUMMARY_FIELDS = (
    "id",
    "to",
    "status",
    "initialized",
    "auto_update",
    "update_schedule",
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
        "update_schedule": dict(DEFAULT_SCHEDULE),
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


def new_run(task_id: str, trigger: str, params: Dict[str, Any], status: str) -> Dict[str, Any]:
    return {
        "task_id": task_id,
        "trigger": trigger,
        "status": status,
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "params": params,
        "target_base": None,
        "target_head": None,
        "base": None,
        "head": None,
        "changed_roots": [],
        "changes": None,
        "result": None,
        "error": None,
    }


class CompileLifecycle:
    def __init__(self, store: CompileStateStore, viking_fs: Any) -> None:
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
        else:
            target_id, trigger = await self._save_submission(
                payload, control, runtime_args, instruction_set, ctx
            )
            record = await self._store.get(ctx.account_id, target_id)
            if record["instruction"]:
                task_payload["instruction"] = record["instruction"]
            runtime_args = {**record["execution_context"]["args"], **runtime_args}
        if runtime_args:
            task_payload["args"] = runtime_args
        task_payload["compile"] = {
            "target_id": target_id,
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
    ) -> tuple[str, str]:
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
            if instruction_set:
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
        return target_id, "manual"

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

    async def record_queued(self, task: Any) -> None:
        """Create the queued run so other instances see the task before it starts."""
        meta, request = task.meta["request"]["compile"], task.meta["request"]
        run = await self._store.get_run(task.account_id, meta["target_id"], task.task_id)
        if run is None:
            await self._store.put_run(
                task.account_id,
                meta["target_id"],
                new_run(task.task_id, meta["trigger"], _params(request), "queued"),
            )

    # ---------- execution hooks ----------

    async def on_start(self, task: Any) -> Any:
        """Claim the target across instances, then snapshot and plan once."""
        meta = task.meta["request"].get("compile")
        if meta is None:
            return "proceed"
        account_id, target_id = task.account_id, meta["target_id"]
        async with self._store.edit(account_id, target_id) as record:
            owner = record["active_task_id"]
            if owner and owner != task.task_id:
                other = await self._store.get_run(account_id, target_id, owner)
                if other is not None and other["status"] not in TERMINAL:
                    return "busy"
            record["active_task_id"] = task.task_id
        run = await self._store.get_run(account_id, target_id, task.task_id)
        if run is None or run["status"] == "queued":
            run = await self._begin_run(task, record, run)
        if run["status"] in TERMINAL:
            # Planned as a no-op, or finished before a crash: finalize without Runtime.
            return ExternalTaskSnapshot(status=run["status"], result=run.get("result") or {})
        return "proceed"

    async def _begin_run(
        self, task: Any, record: Dict[str, Any], run: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        meta, request = task.meta["request"]["compile"], task.meta["request"]
        ctx = task_ctx(task.account_id, task.user_id)
        incremental = meta["mode"] == "incremental"
        run = run or new_run(task.task_id, meta["trigger"], _params(request), "queued")
        run["actor"] = task.user_id
        run["status"] = "running"
        run["started_at"] = time.time()
        paths = [request["to"], *(request["from"] if incremental else [])]
        run["target_base"] = await self._commit(paths, f"compile {task.task_id} start", ctx)
        await self.versions.record_external(task.account_id, record, run["target_base"])
        if incremental:
            run.update(await self._plan(record, request["from"], run["target_base"], ctx))
            idle = not run["changes"]["scopes"] and not request.get("instruction")
            if idle and record["initialized"]:
                run["status"] = "completed"
        await self._store.put_run(task.account_id, meta["target_id"], run)
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
        candidates = (
            await self.versions.candidate_scopes(ctx.account_id, record, changed_roots)
            if scopes
            else []
        )
        return {
            "base": base,
            "head": head,
            "changed_roots": changed_roots,
            "changes": {
                "summary": summary,
                "scope_count": len(scopes),
                "scopes": scopes,
                "candidate_output_scopes": candidates,
            },
        }

    async def runtime_changes(
        self, payload: Mapping[str, Any], account_id: str, task_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return ``args.changes`` for an incremental task, read from its run record."""
        meta = payload.get("compile")
        if not meta or meta["mode"] != "incremental":
            return None
        run = await self._store.get_run(account_id, meta["target_id"], task_id)
        if run is None or run.get("changes") is None:
            return None
        return {
            "base": run["base"],
            "head": run["head"],
            "scopes": run["changes"]["scopes"],
            "candidate_output_scopes": run["changes"]["candidate_output_scopes"],
        }

    async def on_finish(self, task: Any, snapshot: ExternalTaskSnapshot) -> ExternalTaskSnapshot:
        """Snapshot or roll back ``to``, slim the run, then advance progress and release."""
        meta, request = task.meta["request"].get("compile"), task.meta["request"]
        if meta is None:
            return snapshot
        account_id, target_id = task.account_id, meta["target_id"]
        run = await self._store.get_run(account_id, target_id, task.task_id)
        if run is not None and run["finished_at"] is None:
            ctx = task_ctx(task.account_id, task.user_id)
            if run["target_base"] is None:
                pass  # Never started: nothing to snapshot or roll back.
            elif snapshot.status == "completed":
                runtime_meta = snapshot.meta or {}
                head = (
                    runtime_meta["commit_oid"]
                    if "commit_oid" in runtime_meta
                    else await self._commit([request["to"]], f"compile {task.task_id} finish", ctx)
                )
                run["target_head"] = head
                changes = (
                    await self._fs.diff_paths(
                        dirs=[request["to"]], base=run["target_base"], head=head, ctx=ctx
                    )
                    if head
                    else []
                )
                run["result"] = dict.fromkeys(_RESULT_KEYS.values(), 0)
                for change in changes:
                    run["result"][_RESULT_KEYS[change["change_type"]]] += 1
                record = await self._store.get(account_id, target_id)
                await self.versions.record_compile(account_id, record, run, head, changes)
            else:
                await self._rollback(request["to"], run, ctx)
                run["error"] = snapshot.error_message or snapshot.status
            run["status"] = snapshot.status
            run["finished_at"] = time.time()
            for large in ("scopes", "candidate_output_scopes"):
                (run.get("changes") or {}).pop(large, None)
            await self._store.put_run(account_id, target_id, run)
        await self._release(account_id, target_id, task.task_id, run)
        await self._store.prune_runs(account_id, target_id)
        return snapshot

    async def _release(
        self, account_id: str, target_id: str, task_id: str, run: Optional[Dict[str, Any]]
    ) -> None:
        first_success = False
        async with self._store.edit(account_id, target_id) as record:
            if record["active_task_id"] in {None, task_id} and run is not None:
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
                # Version indexes are synced to this snapshot: the finish snapshot
                # on success, the start snapshot (to restored to it) otherwise.
                synced = run.get("target_head") or run.get("target_base")
                if synced:
                    record["versions_head"] = synced
            if record["active_task_id"] == task_id:
                record["active_task_id"] = None
        if first_success and self.reschedule is not None:
            await self.reschedule(account_id, record)

    async def _rollback(self, to: str, run: Dict[str, Any], ctx: RequestContext) -> None:
        """Restore ``to`` to its start snapshot; new targets only lose files they gained."""
        head = await self._commit([to], f"compile {run['task_id']} rollback point", ctx)
        if not head:
            return
        changes = await self._fs.diff_paths(dirs=[to], base=run["target_base"], head=head, ctx=ctx)
        if any(change["change_type"] != "added" for change in changes):
            await retry_git(
                lambda: self._fs.restore(project_dir=to, source_commit=run["target_base"], ctx=ctx)
            )
            return
        for change in changes:
            await self._fs.rm(change["uri"], ctx=ctx)

    # ---------- git ----------

    async def _commit(self, paths: List[str], message: str, ctx: RequestContext) -> Optional[str]:
        return await commit_snapshot(self._fs, paths, message, ctx)


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
) -> Optional[str]:
    """Return the snapshot oid, or None when Git is disabled (Compile then runs in full).

    A just-written file stays locked while its semantic refresh runs, so a busy
    path is retried until ``SNAPSHOT_BUSY_SECONDS`` elapse.
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
            if time.monotonic() >= deadline:
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


def is_versioned(uri: str) -> bool:
    return uri.rsplit("/", 1)[-1] not in _DERIVED


class CompileVersions:
    def __init__(self, store: CompileStateStore, viking_fs: Any) -> None:
        self._store = store
        self._fs = viking_fs
        # Installed by CompileScheduler: async (account_id, record) -> None.
        self.reschedule = None

    # ---------- index storage ----------

    def _dir(self, account_id: str, target_id: str) -> str:
        return f"{self._store.root(account_id)}/targets/{target_id}/versions"

    def _path(self, account_id: str, target_id: str, uri: str) -> str:
        name = hashlib.sha256(uri.encode("utf-8")).hexdigest()
        return f"{self._dir(account_id, target_id)}/{name}.json"

    async def load(self, account_id: str, target_id: str, uri: str) -> Optional[Dict[str, Any]]:
        return await self._store.read_json(self._path(account_id, target_id, uri))

    async def _indexes(self, account_id: str, target_id: str) -> List[Dict[str, Any]]:
        directory = self._dir(account_id, target_id)
        indexes = []
        for name in await self._store.list_dir(directory):
            index = await self._store.read_json(f"{directory}/{name}")
            if index is not None:
                indexes.append(index)
        return indexes

    async def append(
        self,
        account_id: str,
        target_id: str,
        uri: str,
        snapshot_id: str,
        entry: Dict[str, Any],
        *,
        source_roots: List[str],
    ) -> Dict[str, Any]:
        path = self._path(account_id, target_id, uri)
        async with self._store.locked(path) as lease:
            index = await self._store.read_json(path) or {"uri": uri, "next": 1, "versions": []}
            version = {
                "version_id": f"v{index['next']}",
                "snapshot_id": snapshot_id,
                "snapshot_path": uri,
                "created_at": time.time(),
                **entry,
            }
            index["next"] += 1
            index["source_roots"] = source_roots
            index["versions"] = (index["versions"] + [version])[-MAX_VERSIONS:]
            await self._store.write_json(path, index, lease)
        return version

    # ---------- target resolution ----------

    async def owning_target(self, account_id: str, uri: str) -> Optional[Dict[str, Any]]:
        """Return the active, initialized target whose ``to`` contains ``uri``."""
        for item in await self._store.list_summaries(account_id):
            to = item.get("to") or ""
            if item.get("status") == "active" and uri.startswith(f"{to}/"):
                record = await self._store.get(account_id, item["id"])
                if record is not None and record["initialized"]:
                    return record
        return None

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
        run = await self._store.get_run(account_id, record["id"], owner)
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
        """One ``compile`` version per changed output; deleted outputs lose their index.

        Full (or first) Compile binds a file to all sources; incremental Compile
        adds the roots that changed in this run to the file's existing binding.
        """
        full = run["params"]["args"]["mode"] == "full"
        for change in changes:
            uri = change["uri"]
            if not is_versioned(uri):
                continue
            if change["change_type"] == "deleted":
                await self._store.remove(self._path(account_id, record["id"], uri))
                continue
            index = await self.load(account_id, record["id"], uri)
            if full or index is None:
                roots = list(run["params"]["from"])
            else:
                roots = sorted(set(index["source_roots"]) | set(run["changed_roots"]))
            await self.append(
                account_id,
                record["id"],
                uri,
                head,
                {
                    "source": "compile",
                    "actor": run["actor"],
                    "task_id": run["task_id"],
                    "provenance": {
                        "trigger": run["trigger"],
                        "skill": run["params"]["skill"],
                        "source_roots": roots,
                    },
                },
                source_roots=roots,
            )

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
        for change in changes:
            uri = change["uri"]
            if not is_versioned(uri):
                continue
            if change["change_type"] == "deleted":
                await self._store.remove(self._path(account_id, record["id"], uri))
            elif await self._changed(account_id, record["id"], uri, target_base):
                index = await self.load(account_id, record["id"], uri)
                await self.append(
                    account_id,
                    record["id"],
                    uri,
                    target_base,
                    {"source": "external", "actor": None, "provenance": {}},
                    source_roots=list((index or {}).get("source_roots") or record["from"]),
                )

    async def record_write(
        self,
        uris: List[str],
        ctx: RequestContext,
        edit_source: str,
        *,
        lease_ref: Optional[Dict[str, Any]] = None,
        write: Optional[Callable[[], Awaitable[Any]]] = None,
        snapshot_id: Optional[str] = None,
    ) -> Any:
        """Write and version managed outputs before refresh, returning the write result.

        Active Compile records its own versions; other edits become self-bound.
        Batch URIs are already written and omit ``write``.
        snapshot_id reuses a batch's directory snapshot for its file versions.
        """
        written = await write() if write is not None else None
        grouped: Dict[str, List[str]] = {}
        for uri in uris:
            record = await self.owning_target(ctx.account_id, uri)
            if (
                record is None
                or not is_versioned(uri)
                or await self._running(ctx.account_id, record)
            ):
                continue
            grouped.setdefault(record["id"], []).append(uri)
        for target_id, targets in grouped.items():
            snapshot = snapshot_id or await self._commit(
                targets, f"{edit_source} edit", ctx, lease_ref=lease_ref
            )
            for uri in targets:
                if snapshot and await self._changed(ctx.account_id, target_id, uri, snapshot):
                    await self.append(
                        ctx.account_id,
                        target_id,
                        uri,
                        snapshot,
                        {
                            "source": edit_source,
                            "actor": ctx.user.user_id,
                            "provenance": {"source_roots": [uri]},
                        },
                        source_roots=[uri],
                    )
        return written

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
            await self._store.remove(self._path(account_id, source["id"], uri))
            if target is not None and target["id"] == source["id"]:
                index["uri"] = to_uri + uri[len(from_uri) :]
                await self._store.write_json(
                    self._path(account_id, source["id"], index["uri"]), index
                )

    async def drop_all(self, account_id: str, target_id: str) -> None:
        await self._store.remove(self._dir(account_id, target_id), recursive=True)

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
                record["execution_context"]["credential"] = None
            await self.drop_all(ctx.account_id, item["id"])
            if self.reschedule is not None:
                await self.reschedule(ctx.account_id, record)
        if not removed:
            record = await self.owning_target(ctx.account_id, uri)
            if record is not None:
                for index in await self._indexes(ctx.account_id, record["id"]):
                    if index["uri"] == uri or index["uri"].startswith(f"{uri}/"):
                        await self._store.remove(
                            self._path(ctx.account_id, record["id"], index["uri"])
                        )

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
        moved = await self._targets_under(ctx.account_id, from_uri)
        for item in moved:
            async with self._store.edit(ctx.account_id, item["id"]) as record:
                record["to"] = to_uri + record["to"][len(from_uri) :]
            for index in await self._indexes(ctx.account_id, item["id"]):
                await self._store.remove(self._path(ctx.account_id, item["id"], index["uri"]))
                index["uri"] = record["to"] + index["uri"][len(item["to"]) :]
                await self._store.write_json(
                    self._path(ctx.account_id, item["id"], index["uri"]), index
                )
        if not moved:
            await self.move(ctx.account_id, from_uri, to_uri)

    # ---------- candidate outputs ----------

    async def candidate_scopes(
        self, account_id: str, record: Dict[str, Any], changed_roots: List[str]
    ) -> List[Dict[str, Any]]:
        """Outputs bound to a changed root; the whole target until any binding exists."""
        indexes = await self._indexes(account_id, record["id"])
        if not indexes:
            return [{"uri": record["to"], "recursive": True}]
        return sorted(
            (
                {"uri": index["uri"], "recursive": False}
                for index in indexes
                if any(
                    overlaps(root, changed)
                    for root in index["source_roots"]
                    for changed in changed_roots
                )
            ),
            key=lambda item: item["uri"],
        )

    # ---------- queries and single-file restore ----------

    async def versions(
        self, uri: str, ctx: RequestContext, *, limit: int, before: Optional[str]
    ) -> Dict[str, Any]:
        await self._fs._ensure_access(uri, ctx)
        record = await self._managed(uri, ctx)
        index = await self.load(ctx.account_id, record["id"], uri)
        entries = list(reversed((index or {}).get("versions", [])))
        current = entries[0]["version_id"] if entries else None
        if before:
            ids = [entry["version_id"] for entry in entries]
            entries = entries[ids.index(before) + 1 :] if before in ids else []
        page = entries[:limit]
        return {
            "uri": uri,
            "current_version": current,
            "versions": page,
            "next_before_version": page[-1]["version_id"] if len(entries) > limit else None,
        }

    async def restore(
        self,
        uri: str,
        source_version: str,
        expected_version: str,
        ctx: RequestContext,
        write: Callable[[bytes, Callable[..., Awaitable[Any]]], Awaitable[Any]],
    ) -> Dict[str, Any]:
        """Restore immutable historical bytes as a new version before refresh.

        Check ``expected_version`` under the content lock before writing.
        """
        await self._fs._ensure_access(uri, ctx, action=AclAction.WRITE)
        record = await self._managed(uri, ctx)
        path = self._path(ctx.account_id, record["id"], uri)
        index = await self._store.read_json(path) or {"versions": []}
        source = next((v for v in index["versions"] if v["version_id"] == source_version), None)
        if source is None:
            raise NotFoundError(source_version, "file version")
        content = await self._fs.show(source["snapshot_id"], path=source["snapshot_path"], ctx=ctx)
        result: Dict[str, Any] = {}

        async def commit_restore(uris, *, lease_ref, write):
            """Check, write and version under the content lease before refresh."""
            async with self._store.locked(path):
                record = await self._store.get(ctx.account_id, target_id)
                if await self._running(ctx.account_id, record):
                    raise ConflictError("Compile is running", resource="COMPILE_RUNNING")
                index = await self._store.read_json(path) or {"versions": []}
                current = index["versions"][-1]["version_id"] if index["versions"] else None
                if current != expected_version:
                    raise ConflictError(
                        f"Current version is {current}, not {expected_version}",
                        resource="VERSION_CONFLICT",
                    )
                written = await write()
                snapshot = await self._commit(
                    uris, f"restore {uri} to {source_version}", ctx, lease_ref=lease_ref
                )
            if not snapshot or not await self._changed(ctx.account_id, target_id, uri, snapshot):
                result.update(uri=uri, version_id=current, restored_from=None, result="noop")
            else:
                version = await self.append(
                    ctx.account_id,
                    target_id,
                    uri,
                    snapshot,
                    {
                        "source": "restore",
                        "actor": ctx.user.user_id,
                        "restored_from": source_version,
                        "provenance": dict(source.get("provenance") or {}),
                    },
                    source_roots=list(index.get("source_roots") or [uri]),
                )
                result.update(
                    uri=uri, version_id=version["version_id"], restored_from=source_version
                )
            return written

        target_id = record["id"]
        await write(content, commit_restore)
        return result

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
