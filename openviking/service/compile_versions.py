# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Per-file version index for Compile outputs.

One JSON per output file under ``targets/{id}/versions/{sha256(uri)}.json``
holds the file's current URI, its source binding (``source_roots``) and the
newest ``MAX_VERSIONS`` entries. Bodies stay in Git; an entry points at the
snapshot (``snapshot_id`` + ``snapshot_path``) holding its content. Index
read-modify-write takes the index file's PathLock, like the other Compile state.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from openviking.server.identity import RequestContext
from openviking.service.compile_lifecycle import TERMINAL, commit_snapshot, overlaps, task_ctx
from openviking.service.compile_state import CompileStateStore
from openviking.storage.acl import AclAction
from openviking_cli.exceptions import (
    ConflictError,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
)

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

    async def record_write(self, uris: List[str], ctx: RequestContext, edit_source: str) -> None:
        """After content/write: snapshot written output files and add one version each.

        Writes while the target runs a Compile are left to that task's finish
        snapshot, so Agent output is versioned once as ``compile``. Edited files
        become self-bound and are no longer candidates for source changes.
        """
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
            snapshot = await self._commit(targets, f"{edit_source} edit", ctx)
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
        write: Callable[[bytes], Awaitable[Any]],
    ) -> Dict[str, Any]:
        """Write an earlier version back through ``write`` and record a ``restore`` version.

        ``write`` is the content/write path, so summaries and indexes refresh as
        for a normal edit. The version checks run under the version index lock,
        which also serializes concurrent restores of the same file.
        """
        await self._fs._ensure_access(uri, ctx, action=AclAction.WRITE)
        record = await self._managed(uri, ctx)
        path = self._path(ctx.account_id, record["id"], uri)
        async with self._store.locked(path):
            record = await self._store.get(ctx.account_id, record["id"])
            if await self._running(ctx.account_id, record):
                raise ConflictError("Compile is running", resource="COMPILE_RUNNING")
            index = await self._store.read_json(path) or {"versions": []}
            versions = {entry["version_id"]: entry for entry in index["versions"]}
            current = index["versions"][-1]["version_id"] if index["versions"] else None
            if current != expected_version:
                raise ConflictError(
                    f"Current version is {current}, not {expected_version}",
                    resource="VERSION_CONFLICT",
                )
            source = versions.get(source_version)
            if source is None:
                raise NotFoundError(source_version, "file version")
            content = await self._fs.show(
                source["snapshot_id"], path=source["snapshot_path"], ctx=ctx
            )
            await write(content)
            snapshot = await self._commit([uri], f"restore {uri} to {source_version}", ctx)
        if not snapshot or not await self._changed(ctx.account_id, record["id"], uri, snapshot):
            return {"uri": uri, "version_id": current, "restored_from": None, "result": "noop"}
        version = await self.append(
            ctx.account_id,
            record["id"],
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
        return {"uri": uri, "version_id": version["version_id"], "restored_from": source_version}

    async def _commit(self, uris: List[str], message: str, ctx: RequestContext) -> Optional[str]:
        return await commit_snapshot(
            self._fs, uris, message, task_ctx(ctx.account_id, ctx.user.user_id)
        )
