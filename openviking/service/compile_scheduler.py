# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Scheduled Compile: due-slot checks, task submission and two discovery backends.

``trigger_if_due`` owns the decision and submission under the target record
lock. Backends only discover candidates: ``local`` scans each account's
targets.json hourly; ``redis`` registers each target's next slot with the
distributed ExpiryScheduler and also runs a slower compensation scan, because
the record (not Redis) is the source of truth.
"""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from openviking.server.identity import RequestContext, Role
from openviking.service.compile_lifecycle import task_ctx
from openviking.service.compile_state import CompileStateStore
from openviking_cli.exceptions import ConflictError, FailedPreconditionError, NotFoundError
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

TZ = ZoneInfo("Asia/Shanghai")
COMPILE_TARGET_ITEM_TYPE = "compile_target"
LOCAL_SCAN_SECONDS = 3600.0
COMPENSATION_SCAN_SECONDS = 6 * 3600.0
CREDENTIAL_PROVIDER = "encrypted_api_key"


def now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(TZ)).astimezone(TZ).isoformat()


def current_slot(frequency: str, now: datetime) -> datetime:
    """Most recent Beijing-time 00:00 (daily) or Monday 00:00 (weekly) not after ``now``."""
    local = now.astimezone(TZ)
    slot = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if frequency == "weekly":
        slot -= timedelta(days=slot.weekday())
    return slot


def next_slot(frequency: str, now: datetime) -> datetime:
    return current_slot(frequency, now) + timedelta(days=7 if frequency == "weekly" else 1)


def next_run_at(record: Dict[str, Any], now: Optional[datetime] = None) -> Optional[str]:
    if not (record.get("auto_update") and record.get("initialized")):
        return None
    return next_slot(record["update_frequency"], now or datetime.now(TZ)).isoformat()


def is_due(record: Dict[str, Any], slot: datetime) -> bool:
    def before(value: Optional[str]) -> bool:
        return value is None or datetime.fromisoformat(value) < slot

    enabled_at = record.get("auto_update_enabled_at")
    return (
        record.get("status") == "active"
        and bool(record.get("initialized"))
        and bool(record.get("auto_update"))
        and enabled_at is not None
        and datetime.fromisoformat(enabled_at) < slot
        and before(record.get("last_triggered_slot"))
    )


async def encrypt_credential(viking_fs: Any, ctx: RequestContext) -> Dict[str, str]:
    encryptor = getattr(viking_fs, "_encryptor", None)
    if encryptor is None:
        raise FailedPreconditionError(
            "Scheduled Compile requires encryption.enabled=true",
            details={"reason": "COMPILE_CREDENTIAL_ENCRYPTION_REQUIRED"},
        )
    if not ctx.api_key:
        raise FailedPreconditionError(
            "Scheduled Compile requires an OpenViking API key",
            details={"reason": "COMPILE_CREDENTIAL_INVALID"},
        )
    plaintext = json.dumps({"api_key": ctx.api_key}).encode("utf-8")
    ciphertext = await encryptor.encrypt(ctx.account_id, plaintext)
    return {"provider": CREDENTIAL_PROVIDER, "ciphertext": base64.b64encode(ciphertext).decode()}


class CompileScheduler:
    def __init__(
        self,
        compile_service: Any,
        store: CompileStateStore,
        viking_fs: Any,
        *,
        backend: str = "local",
    ) -> None:
        self._compile = compile_service
        self._store = store
        self._fs = viking_fs
        self._backend = backend
        self._expiry: Any = None
        self._identity_resolver: Any = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # ---------- wiring ----------

    def set_identity_resolver(self, resolver: Any) -> None:
        """Install an async ``api_key -> ResolvedIdentity`` callable (APIKeyManager)."""
        self._identity_resolver = resolver

    def install_expiry(self, scheduler: Any) -> None:
        """Register the redis backend handler; call before ExpiryRuntime.start_roles()."""
        from openviking.scheduling.expiry import HandlerOptions

        self._expiry = scheduler
        scheduler.register_handler(
            COMPILE_TARGET_ITEM_TYPE,
            _ExpiryHandler(self),
            options=HandlerOptions(concurrency=4, max_attempts=3, metadata_enabled=False),
        )

    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        interval = COMPENSATION_SCAN_SECONDS if self._backend == "redis" else LOCAL_SCAN_SECONDS
        while not self._stop.is_set():
            try:
                await self.scan_once()
            except Exception:
                logger.exception("Compile scheduler scan failed")
            try:
                await asyncio.wait_for(self._stop.wait(), interval)
            except asyncio.TimeoutError:
                pass

    # ---------- discovery ----------

    async def scan_once(self, now: Optional[datetime] = None) -> None:
        now = now or datetime.now(TZ)
        for account_id in await self._store.list_accounts():
            for item in await self._store.list_summaries(account_id):
                try:
                    if is_due(item, current_slot(item["update_frequency"], now)):
                        await self.trigger_if_due(account_id, item["id"], now)
                    elif self._backend == "redis" and item.get("auto_update"):
                        await self.reschedule(account_id, item)
                except Exception:
                    logger.exception("Compile schedule check failed for %s", item.get("id"))

    async def reschedule(self, account_id: str, record: Dict[str, Any]) -> None:
        """Mirror a record's schedule into Redis; local backend needs nothing."""
        if self._expiry is None:
            return
        from openviking.scheduling.expiry import ExpiryMode, ExpiryRegistration, ItemIdentity

        identity = ItemIdentity(COMPILE_TARGET_ITEM_TYPE, account_id, "_system", record["id"])
        try:
            if (
                record.get("status") == "active"
                and record.get("initialized")
                and record.get("auto_update")
            ):
                frequency = record["update_frequency"]
                due = next_slot(frequency, datetime.now(TZ))
                await self._expiry.report(
                    ExpiryRegistration(
                        identity=identity,
                        due_at_ms=int(due.timestamp() * 1000),
                        timeout_seconds=604800 if frequency == "weekly" else 86400,
                        mode=ExpiryMode.IDLE_ONESHOT,
                    )
                )
            else:
                await self._expiry.unregister(identity)
        except Exception:
            # The compensation scan repairs a missing or stale registration.
            logger.warning("Compile reschedule failed for %s", record["id"], exc_info=True)

    # ---------- decision and submission ----------

    async def trigger_if_due(
        self, account_id: str, target_id: str, now: Optional[datetime] = None
    ) -> str:
        """Submit the target's scheduled task for the current slot at most once."""
        now = now or datetime.now(TZ)
        async with self._store.edit(account_id, target_id) as record:
            slot = current_slot(record["update_frequency"], now)
            if not is_due(record, slot):
                return "not_due"
            slot_iso = slot.isoformat()
            key = f"compile-schedule:{target_id}:{slot_iso}"
            outcome = "submitted"
            try:
                ctx, connection = await self._executor(record)
                owner_ctx = task_ctx(account_id, ctx.user.user_id)
                if not await self._fs.exists(record["to"], ctx=owner_ctx):
                    raise NotFoundError(record["to"], "compile target")
            except (FailedPreconditionError, NotFoundError) as exc:
                record["auto_update"] = False
                record["execution_context"]["credential"] = None
                record["last_error"] = (
                    f"Compile target is missing: {record['to']}"
                    if isinstance(exc, NotFoundError)
                    else exc.message
                )
                outcome = "disabled"
            else:
                try:
                    await self._compile.create_scheduled(
                        record, ctx=ctx, connection=connection, idempotency_key=key
                    )
                except ConflictError:
                    pass  # Same slot already submitted with older parameters.
            record["last_triggered_slot"] = slot_iso
        await self.reschedule(account_id, record)
        return outcome

    async def _executor(self, record: Dict[str, Any]) -> tuple[RequestContext, Dict[str, Any]]:
        """Decrypt the saved key and verify it still resolves to the saved executor."""
        context = record["execution_context"]
        executor, credential = context.get("executor"), context.get("credential")
        invalid = FailedPreconditionError(
            "Scheduled Compile API key is invalid; save the update settings with a valid key",
            details={"reason": "COMPILE_CREDENTIAL_INVALID"},
        )
        encryptor = getattr(self._fs, "_encryptor", None)
        if not executor or not credential or encryptor is None:
            raise invalid
        try:
            ciphertext = base64.b64decode(credential["ciphertext"], validate=True)
            plaintext = await encryptor.decrypt(executor["account_id"], ciphertext)
            api_key = json.loads(plaintext.decode("utf-8"))["api_key"]
        except Exception as exc:
            raise invalid from exc
        if self._identity_resolver is not None:
            try:
                identity = await self._identity_resolver(api_key)
            except Exception as exc:
                raise invalid from exc
            if (identity.account_id, identity.user_id) != (
                executor["account_id"],
                executor["user_id"],
            ):
                raise invalid
        ctx = RequestContext(
            user=UserIdentifier(account_id=executor["account_id"], user_id=executor["user_id"]),
            role=Role.USER,
            api_key=api_key,
        )
        connection = {
            "api_key": api_key,
            "account_id": executor["account_id"],
            "user_id": executor["user_id"],
        }
        return ctx, connection


class _ExpiryHandler:
    def __init__(self, scheduler: CompileScheduler) -> None:
        self._scheduler = scheduler

    async def handle(self, request: Any) -> Any:
        from openviking.scheduling.expiry import CallbackResult, CallbackStatus

        identity = request.identity
        try:
            outcome = await self._scheduler.trigger_if_due(identity.account_id, identity.local_id)
        except LookupError:
            return CallbackResult(CallbackStatus.COMPLETED, "not_found")
        except Exception:
            logger.warning("Compile schedule callback failed", exc_info=True)
            return CallbackResult(CallbackStatus.RETRY, "trigger_failed")
        return CallbackResult(CallbackStatus.COMPLETED, outcome)
