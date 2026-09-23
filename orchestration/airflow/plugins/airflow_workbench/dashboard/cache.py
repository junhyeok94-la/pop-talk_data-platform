"""Short-lived aggregate results, shared by API processes in the existing DB.

Leases are committed before computation. Same-key requests coalesce locally;
a global slot and refresh budget bound cold aggregates. Busy requests never poll.
"""

import asyncio
import hashlib
import json
import math
import time
import uuid
import weakref

from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard.database import Store

TTL = 30
MAX_ENTRIES = 32
MAX_BYTES = 2 * 1024 * 1024
SLOTS = 1
LEASE = 10
_pending = weakref.WeakKeyDictionary()


def retry_after(reason):
    if reason == "aggregate_budget":
        return max(1, math.ceil(protection.policy.compute_gap))
    if reason == "aggregate_busy":
        return LEASE
    return protection.policy.cooldown


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def probe(key, owner):
    with Store().connection() as db:
        now = db.execute(
            "SELECT extract(epoch FROM clock_timestamp())::double precision"
        ).fetchone()[0]
        row = db.execute(
            "SELECT value,sampled,expires,lease_until FROM dashboard_cache WHERE key=?",
            (key,),
        ).fetchone()
        if row and row["value"] is not None and row["expires"] > now:
            return "hit", json.loads(row["value"]), row["sampled"], None

        def deferred(reason):
            usable = (
                row
                and row["value"] is not None
                and now - row["sampled"] <= protection.policy.stale_seconds
            )
            return (
                ("stale", json.loads(row["value"]), row["sampled"], reason)
                if usable
                else ("wait", None, None, reason)
            )

        if row and row["lease_until"] > now:
            return deferred("aggregate_busy")

        reason = protection.database_pressure(db.connection)
        if reason:
            return deferred(reason)
        # Serialize short claims/eviction only, never expensive aggregate execution.
        db.lock("dashboard-cache-claims")
        row = db.execute(
            "SELECT value,sampled,expires,lease_until FROM dashboard_cache WHERE key=?",
            (key,),
        ).fetchone()
        if row and row["value"] is not None and row["expires"] > now:
            return "hit", json.loads(row["value"]), row["sampled"], None
        if row and row["lease_until"] > now:
            return deferred("aggregate_busy")
        slot = None
        for i in range(SLOTS):
            name = "dashboard-aggregate:" + str(i)
            claimed = db.execute(
                "INSERT INTO leases(name,owner,expires) VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET owner=EXCLUDED.owner,expires=EXCLUDED.expires WHERE leases.expires<=? RETURNING name",
                (name, owner, now + LEASE, now),
            ).fetchone()
            if claimed:
                slot = name
                break
        if slot is None:
            return deferred("aggregate_busy")
        budget = db.execute(
            "INSERT INTO leases(name,owner,expires) VALUES ('dashboard-refresh-budget','budget',?) "
            "ON CONFLICT(name) DO UPDATE SET expires=EXCLUDED.expires WHERE leases.expires<=? RETURNING name",
            (now + protection.policy.compute_gap, now),
        ).fetchone()
        if not budget:
            db.execute("DELETE FROM leases WHERE name=? AND owner=?", (slot, owner))
            return deferred("aggregate_budget")
        if row is None:
            count = db.execute("SELECT count(*) FROM dashboard_cache").fetchone()[0]
            if count >= MAX_ENTRIES:
                removed = db.execute(
                    "DELETE FROM dashboard_cache WHERE key IN (SELECT key FROM dashboard_cache WHERE lease_until<=? ORDER BY expires LIMIT 1) RETURNING key",
                    (now,),
                ).fetchone()
                if not removed:
                    db.execute(
                        "DELETE FROM leases WHERE name=? AND owner=?", (slot, owner)
                    )
                    return "wait", None, None, "aggregate_busy"
        db.execute(
            "INSERT INTO dashboard_cache(key,lease_owner,lease_until) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until",
            (key, owner, now + LEASE),
        )
        return "compute", None, now, slot


def finish(key, owner, slot, value=None, sampled=0):
    encoded = json.dumps(value, separators=(",", ":")) if value is not None else None
    oversized = encoded is not None and len(encoded.encode()) > MAX_BYTES
    with Store().connection() as db:
        if encoded is not None and not oversized:
            db.execute(
                "UPDATE dashboard_cache SET value=?,sampled=?,expires=?,lease_owner=NULL,lease_until=0 WHERE key=? AND lease_owner=?",
                (encoded, sampled, sampled + TTL, key, owner),
            )
        else:
            db.execute(
                "UPDATE dashboard_cache SET lease_owner=NULL,lease_until=0 WHERE key=? AND lease_owner=?",
                (key, owner),
            )
        db.execute("DELETE FROM leases WHERE name=? AND owner=?", (slot, owner))
    if oversized:
        raise ValueError(
            "집계 결과가 2MB를 초과합니다. 표시 행이나 패널 수를 줄여 주세요."
        )


async def _load(key, compute, asynchronous=False):
    owner = uuid.uuid4().hex
    reason = protection.guard.blocked()
    if reason:
        raise protection.Deferred(reason)
    action, value, sampled, slot = await protection.run(probe, key, owner)
    if action in {"hit", "stale"}:
        return value, {
            "cache_hit": True,
            "sampled_at": sampled,
            "stale": action == "stale",
            "deferred_reason": slot if action == "stale" else None,
            "retry_after": retry_after(slot) if action == "stale" else 0,
        }
    if action != "compute":
        raise protection.Deferred(slot or "aggregate_busy", retry_after(slot))
    try:
        if asynchronous:
            try:
                value = await asyncio.wait_for(compute(), timeout=3)
            except TimeoutError as exc:
                protection.guard.trip("query_budget")
                raise protection.Deferred("query_budget") from exc
        else:
            value = await protection.run(compute)
        await protection.run(finish, key, owner, slot, value, sampled)
        return value, {"cache_hit": False, "sampled_at": sampled, "stale": False}
    except BaseException:
        if asynchronous:
            # Cancellation of an async wrapper cannot stop an already running DB
            # thread. Keep the 10s lease until its bounded I/O must have finished.
            raise
        try:
            await protection.run(finish, key, owner, slot)
        except Exception:
            pass  # The finite lease recovers if the DB/worker is unavailable.
        raise


async def get(key, compute, *, asynchronous=False):
    loop = asyncio.get_running_loop()
    pending = _pending.setdefault(loop, {})
    task = pending.get(key)
    joined = task is not None
    if task is None:
        if len(pending) >= protection.policy.requests:
            raise protection.Deferred("pending_limit")
        task = asyncio.create_task(_load(key, compute, asynchronous))
        pending[key] = task
        task.add_done_callback(lambda done: pending.pop(key, None))
        # Consume errors even if every client disconnects. The finite job cleans up leases.
        task.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )
    value, info = await asyncio.shield(task)
    return value, {**info, "cache_hit": info["cache_hit"] or joined}
