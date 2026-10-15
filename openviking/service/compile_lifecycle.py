# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Incremental Compile lifecycle: registry updates, snapshots, change plans and finish.

CompileService delegates here from ``create``, ``on_start``, ``submit`` and
``on_finish``. State lives in :mod:`compile_state`. A task runs after the
request that authorized it has ended, so Git access uses an internal
``bypass_acl`` context for the task owner.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Dict, List, Mapping, Optional

from openviking.pyagfs.exceptions import AGFSNotSupportedError, GitConcurrentCommitError
from openviking.server.identity import RequestContext, Role
from openviking.service.compile_state import CompileStateStore
from openviking.service.external_task_service import ExternalTaskSnapshot
from openviking.storage.errors import ResourceBusyError
from openviking_cli.exceptions import ConflictError, InvalidArgumentError
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

CONTROL_ARGS = ("mode", "auto_update", "update_frequency")
FREQUENCIES = ("daily", "weekly")
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


def split_control_args(args: Optional[Mapping[str, Any]]) -> tuple[dict, dict]:
    """Separate kernel control args from Runtime args."""
    runtime = dict(args or {})
    control = {key: runtime.pop(key) for key in CONTROL_ARGS if key in runtime}
    if control.get("mode", "incremental") not in {"incremental", "full"}:
        raise InvalidArgumentError("args.mode must be 'incremental' or 'full'")
    if control.get("update_frequency", "daily") not in FREQUENCIES:
        raise InvalidArgumentError("args.update_frequency must be 'daily' or 'weekly'")
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
        from openviking.service.compile_versions import CompileVersions

        self.store = self._store = store
        self._fs = viking_fs
        self.versions = CompileVersions(store, viking_fs)
        # Installed by CompileScheduler: async (account_id, record) -> None.
        self.reschedule = None

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
            if "auto_update" in control or "update_frequency" in control:
                await self.apply_schedule(
                    record,
                    ctx,
                    control.get("auto_update", record["auto_update"]),
                    control.get("update_frequency", record["update_frequency"]),
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
        self, record: Dict[str, Any], ctx: RequestContext, auto_update: bool, frequency: str
    ) -> None:
        from openviking.service.compile_scheduler import encrypt_credential, now_iso

        changed = auto_update != record["auto_update"] or frequency != record["update_frequency"]
        if auto_update:
            record["execution_context"]["executor"] = {
                "account_id": ctx.account_id,
                "user_id": ctx.user.user_id,
            }
            record["execution_context"]["credential"] = await encrypt_credential(self._fs, ctx)
            record["last_error"] = None
        else:
            record["execution_context"]["credential"] = None
        if changed:
            record["auto_update_enabled_at"] = now_iso() if auto_update else None
        record["auto_update"] = auto_update
        record["update_frequency"] = frequency

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
                head = await self._commit([request["to"]], f"compile {task.task_id} finish", ctx)
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
    viking_fs: Any, paths: List[str], message: str, ctx: RequestContext
) -> Optional[str]:
    """Return the snapshot oid, or None when Git is disabled (Compile then runs in full).

    A just-written file stays locked while its semantic refresh runs, so a busy
    path is retried until ``SNAPSHOT_BUSY_SECONDS`` elapse.
    """
    deadline = time.monotonic() + SNAPSHOT_BUSY_SECONDS
    while True:
        try:
            result = await retry_git(
                lambda: viking_fs.commit(message=message, paths=paths, ctx=ctx)
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
