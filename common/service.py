"""Core alerting logic: event ingestion, silences and the notification outbox."""
from __future__ import annotations

import math
import sqlite3
import time
from typing import Any, Callable, Optional

from .db import transaction

VALID_KINDS = ("firing", "recovery")

# The single monitored probe reports its lost/recovered transitions through the
# normal alert pipeline under this reserved fingerprint. Normal events may not
# use the "probe:" prefix, so probe alerts and regular events can never
# overwrite each other's state.
PROBE_FP_PREFIX = "probe:"
PROBE_FINGERPRINT = f"{PROBE_FP_PREFIX}heartbeat"
DEFAULT_PROBE_TIMEOUT_SECONDS = 60.0


class Reject(Exception):
    """A request that violates the ordering/idempotency rules (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate_event(payload: Any) -> dict:
    if not isinstance(payload, dict):
        raise Reject("invalid_payload", "event body must be a JSON object")
    fingerprint = payload.get("fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        raise Reject("invalid_payload", "fingerprint must be a non-empty string")
    if fingerprint.startswith(PROBE_FP_PREFIX):
        raise Reject(
            "reserved_fingerprint",
            f"fingerprints starting with {PROBE_FP_PREFIX!r} are reserved for "
            "probe heartbeat monitoring",
        )
    seq = payload.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise Reject("invalid_payload", "seq must be a non-negative integer")
    kind = payload.get("kind")
    if kind not in VALID_KINDS:
        raise Reject("invalid_payload", "kind must be 'firing' or 'recovery'")
    summary = payload.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise Reject("invalid_payload", "summary must be a non-empty string")
    return {"fingerprint": fingerprint, "seq": seq, "kind": kind, "summary": summary}


def notification_no(fingerprint: str, round_no: int, kind: str) -> str:
    # Stable and human-readable: retries reuse the exact same number, and the
    # unique index on (fingerprint, round, kind) guarantees global uniqueness.
    return f"{fingerprint}#r{round_no}:{kind}"


# --------------------------------------------------------------------------- #
# Event ingestion
# --------------------------------------------------------------------------- #
def ingest_event(conn: sqlite3.Connection, payload: dict, now: Optional[float] = None) -> dict:
    """Apply one trigger/recovery event.

    Returns a result dictionary. Raises Reject for stale/conflicting events.
    Same (fingerprint, seq, body) replays the original result; same seq with a
    different body is rejected; an older seq can never rewrite current state.
    """
    event = validate_event(payload)
    now = time.time() if now is None else now
    fp, seq, kind, summary = event["fingerprint"], event["seq"], event["kind"], event["summary"]

    with transaction(conn):
        existing = conn.execute(
            "SELECT kind, summary FROM events WHERE fingerprint=? AND seq=?", (fp, seq)
        ).fetchone()
        if existing is not None:
            if existing["kind"] != kind or existing["summary"] != summary:
                raise Reject(
                    "conflicting_event",
                    f"event {fp} seq={seq} was already recorded with a different body",
                )
            # Identical retry: the original application stands, no state change.
            return {"result": "duplicate_replayed", "event": event, "alert": _get_alert(conn, fp)}

        alert_row = conn.execute(
            "SELECT * FROM alerts WHERE fingerprint=?", (fp,)
        ).fetchone()

        if alert_row is None:
            if kind == "recovery":
                raise Reject("stale_event", "recovery event has no active alert")
            round_no = 1
            conn.execute(
                "INSERT INTO alerts(fingerprint, current_seq, status, summary, round, "
                "last_event_at, updated_at) VALUES (?,?, 'firing', ?, ?, ?, ?)",
                (fp, seq, summary, round_no, now, now),
            )
        else:
            if seq <= alert_row["current_seq"]:
                raise Reject(
                    "stale_event",
                    f"seq={seq} is not newer than current seq={alert_row['current_seq']}",
                )
            if kind == "firing":
                if alert_row["status"] == "recovered":
                    # Re-trigger after recovery starts a brand-new round.
                    round_no = alert_row["round"] + 1
                    conn.execute(
                        "UPDATE alerts SET status='firing', round=?, summary=?, "
                        "current_seq=?, last_event_at=?, updated_at=? WHERE fingerprint=?",
                        (round_no, summary, seq, now, now, fp),
                    )
                else:
                    # Newer trigger while still firing: same round, text refresh only.
                    round_no = alert_row["round"]
                    conn.execute(
                        "UPDATE alerts SET summary=?, current_seq=?, last_event_at=?, "
                        "updated_at=? WHERE fingerprint=?",
                        (summary, seq, now, now, fp),
                    )
            else:  # recovery
                round_no = alert_row["round"]
                conn.execute(
                    "UPDATE alerts SET status='recovered', summary=?, current_seq=?, "
                    "last_event_at=?, updated_at=? WHERE fingerprint=?",
                    (summary, seq, now, now, fp),
                )

        conn.execute(
            "INSERT INTO events(fingerprint, seq, kind, summary, received_at) "
            "VALUES (?,?,?,?,?)",
            (fp, seq, kind, summary, now),
        )

        notifications = _apply_notification_policy(conn, fp, round_no, kind, summary, now)
        alert = _get_alert(conn, fp)

    return {
        "result": "accepted",
        "event": event,
        "alert": alert,
        "notifications_enqueued": notifications,
    }


def _silence_active(conn: sqlite3.Connection, now: float) -> bool:
    row = conn.execute("SELECT until_ts FROM silences WHERE id=1").fetchone()
    return row is not None and row["until_ts"] > now


def _enqueue_notification(
    conn: sqlite3.Connection, fp: str, round_no: int, kind: str, summary: str, now: float
) -> bool:
    """Insert an outbox row. Returns False if an identical one already exists."""
    no = notification_no(fp, round_no, kind)
    cur = conn.execute(
        "INSERT OR IGNORE INTO notifications(notification_no, fingerprint, round, kind, "
        "summary, status, next_retry_at, enqueued_at) VALUES (?,?,?,?,?, 'pending', ?, ?)",
        (no, fp, round_no, kind, summary, now, now),
    )
    return cur.rowcount > 0


def _apply_notification_policy(
    conn: sqlite3.Connection, fp: str, round_no: int, kind: str, summary: str, now: float
) -> list[dict]:
    """Decide which notifications a newly applied event produces."""
    enqueued: list[dict] = []
    silenced = _silence_active(conn, now)

    if kind == "firing":
        # During a silence a new trigger only updates the alert; it is parked in
        # the outbox later when the silence expires (see sweep_silences).
        if not silenced:
            if _enqueue_notification(conn, fp, round_no, "firing", summary, now):
                enqueued.append(
                    {"notification_no": notification_no(fp, round_no, "firing"),
                     "kind": "firing"}
                )
    else:  # recovery
        # If the fault was ever announced (a firing notification exists for this
        # round, even if delivery is still pending), its recovery MUST be sent —
        # silence does not suppress it. The outbox worker preserves order.
        firing_row = conn.execute(
            "SELECT 1 FROM notifications WHERE fingerprint=? AND round=? AND kind='firing'",
            (fp, round_no),
        ).fetchone()
        if firing_row is not None:
            if _enqueue_notification(conn, fp, round_no, "recovery", summary, now):
                enqueued.append(
                    {"notification_no": notification_no(fp, round_no, "recovery"),
                     "kind": "recovery"}
                )
    return enqueued


def _get_alert(conn: sqlite3.Connection, fp: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM alerts WHERE fingerprint=?", (fp,)).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------------------- #
# Silence management
# --------------------------------------------------------------------------- #
def set_silence(
    conn: sqlite3.Connection, until_ts: float, reason: str, now: Optional[float] = None
) -> dict:
    now = time.time() if now is None else now
    with transaction(conn):
        conn.execute(
            "INSERT INTO silences(id, until_ts, reason, updated_at) VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET until_ts=excluded.until_ts, "
            "reason=excluded.reason, updated_at=excluded.updated_at",
            (until_ts, reason, now),
        )
    return get_silence(conn, now)


def clear_silence(conn: sqlite3.Connection, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    with transaction(conn):
        conn.execute("DELETE FROM silences WHERE id=1")
    return {"active": False, "until_ts": None, "reason": None, "now": now}


def get_silence(conn: sqlite3.Connection, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    row = conn.execute("SELECT * FROM silences WHERE id=1").fetchone()
    if row is None or row["until_ts"] <= now:
        return {"active": False, "until_ts": None, "reason": None, "now": now}
    return {
        "active": True,
        "until_ts": row["until_ts"],
        "reason": row["reason"],
        "now": now,
        "remaining_seconds": round(row["until_ts"] - now, 3),
    }


def sweep_silences(conn: sqlite3.Connection, now: Optional[float] = None) -> list[dict]:
    """Expire silences: re-issue parked firing notifications.

    When the silence window ends, every alert that is *still firing* and whose
    current round was never announced gets its firing notification enqueued.
    Alerts that already recovered stay silent for that round.
    """
    now = time.time() if now is None else now
    released: list[dict] = []
    with transaction(conn):
        if _silence_active(conn, now):
            return released
        rows = conn.execute(
            "SELECT a.fingerprint, a.round, a.summary FROM alerts a "
            "WHERE a.status='firing'"
        ).fetchall()
        for row in rows:
            exists = conn.execute(
                "SELECT 1 FROM notifications WHERE fingerprint=? AND round=? AND kind='firing'",
                (row["fingerprint"], row["round"]),
            ).fetchone()
            if exists is None:
                if _enqueue_notification(
                    conn, row["fingerprint"], row["round"], "firing", row["summary"], now
                ):
                    released.append(
                        {"fingerprint": row["fingerprint"], "round": row["round"]}
                    )
    return released


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def list_alerts(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT a.*,
               fn.status AS firing_notify_status,
               fn.notification_no AS firing_notification_no,
               rn.status AS recovery_notify_status,
               rn.notification_no AS recovery_notification_no
        FROM alerts a
        LEFT JOIN notifications fn
            ON fn.fingerprint=a.fingerprint AND fn.round=a.round AND fn.kind='firing'
        LEFT JOIN notifications rn
            ON rn.fingerprint=a.fingerprint AND rn.round=a.round AND rn.kind='recovery'
        ORDER BY a.updated_at DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def list_notifications(conn: sqlite3.Connection, limit: int = 200) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def list_events(conn: sqlite3.Connection, fingerprint: str, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM events WHERE fingerprint=? ORDER BY seq DESC LIMIT ?",
        (fingerprint, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Outbox delivery worker
# --------------------------------------------------------------------------- #
SendFunc = Callable[[dict], tuple[bool, Optional[str]]]


def _backoff_seconds(attempts: int) -> float:
    return min(float(attempts), 30.0)


def dispatch_pending(
    conn: sqlite3.Connection, send: SendFunc, now: Optional[float] = None
) -> list[dict]:
    """Deliver due outbox rows, preserving per-fingerprint ordering.

    A notification is only sent when every earlier notification of the same
    fingerprint has already been delivered. Lost receipts keep the row in
    'in_transit' and the same notification number is retried later. HTTP calls
    happen *outside* write transactions so ingestion never blocks on the wire.
    """
    now = time.time() if now is None else now
    results: list[dict] = []

    while True:
        with transaction(conn):
            row = conn.execute(
                "SELECT * FROM notifications WHERE status IN ('pending','in_transit') "
                "AND next_retry_at<=? ORDER BY id",
                (now,),
            ).fetchone()
            if row is None:
                break
            blocker = conn.execute(
                "SELECT 1 FROM notifications WHERE fingerprint=? AND id<? AND "
                "status!='delivered' LIMIT 1",
                (row["fingerprint"], row["id"]),
            ).fetchone()
            if blocker is not None:
                # Head-of-line for this fingerprint: park it briefly; rows of
                # other fingerprints are picked up on the next worker tick.
                conn.execute(
                    "UPDATE notifications SET next_retry_at=? WHERE id=?",
                    (now + 0.2, row["id"]),
                )
                break
            conn.execute(
                "UPDATE notifications SET status='in_transit', attempts=attempts+1, "
                "last_error=NULL WHERE id=?",
                (row["id"],),
            )
            payload = {
                "notification_no": row["notification_no"],
                "fingerprint": row["fingerprint"],
                "round": row["round"],
                "kind": row["kind"],
                "summary": row["summary"],
            }

        # Network I/O happens outside any write transaction so event ingestion
        # never blocks waiting on the notification endpoint.
        ok, error = send(payload)

        with transaction(conn):
            if ok:
                conn.execute(
                    "UPDATE notifications SET status='delivered', delivered_at=?, "
                    "last_error=NULL WHERE id=?",
                    (now, row["id"]),
                )
                results.append({"notification_no": row["notification_no"], "delivered": True})
            else:
                retry_at = now + _backoff_seconds(row["attempts"] + 1)
                conn.execute(
                    "UPDATE notifications SET next_retry_at=?, last_error=? WHERE id=?",
                    (retry_at, error, row["id"]),
                )
                results.append(
                    {
                        "notification_no": row["notification_no"],
                        "delivered": False,
                        "error": error,
                        "next_retry_at": retry_at,
                    }
                )
    return results


# --------------------------------------------------------------------------- #
# Probe heartbeat monitoring (single probe)
# --------------------------------------------------------------------------- #
def _probe_row(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM probe_state WHERE id=1").fetchone()


def _probe_expired(row: sqlite3.Row, now: float) -> bool:
    return row["deadline_at"] is not None and now > row["deadline_at"]


def get_probe(conn: sqlite3.Connection, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    row = _probe_row(conn)
    if row is None:
        return {
            "configured": False,
            "status": "pending",
            "timeout_seconds": None,
            "last_beat_at": None,
            "deadline_at": None,
            "remaining_seconds": None,
            "lost_round": 0,
            "now": now,
        }
    remaining = None
    if row["status"] == "online" and row["deadline_at"] is not None:
        remaining = round(row["deadline_at"] - now, 3)
    return {
        "configured": True,
        "status": row["status"],
        "timeout_seconds": row["timeout_seconds"],
        "last_beat_at": row["last_beat_at"],
        "deadline_at": row["deadline_at"],
        "remaining_seconds": remaining,
        "lost_round": row["lost_round"],
        "now": now,
    }


def set_probe_timeout(
    conn: sqlite3.Connection, timeout_seconds: float, now: Optional[float] = None
) -> dict:
    """Create/update the probe threshold. The deadline is recomputed from the
    last heartbeat (if any), so tightening the timeout can make an already
    overdue probe expire on the next sweep."""
    now = time.time() if now is None else now
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise Reject("invalid_payload", "timeout_seconds must be a positive finite number")
    with transaction(conn):
        row = _probe_row(conn)
        if row is None:
            conn.execute(
                "INSERT INTO probe_state(id, timeout_seconds, last_beat_at, "
                "deadline_at, status, lost_round, updated_at) "
                "VALUES (1, ?, NULL, NULL, 'pending', 0, ?)",
                (timeout_seconds, now),
            )
        else:
            deadline = None
            if row["last_beat_at"] is not None:
                deadline = row["last_beat_at"] + timeout_seconds
            conn.execute(
                "UPDATE probe_state SET timeout_seconds=?, deadline_at=?, "
                "updated_at=? WHERE id=1",
                (timeout_seconds, deadline, now),
            )
    return get_probe(conn, now)


def _probe_alert_transition(
    conn: sqlite3.Connection, kind: str, round_no: int, summary: str, now: float
) -> list[dict]:
    """Drive the reserved probe fingerprint through the regular alert pipeline.

    Synthetic seqs (2*round-1 firing, 2*round recovery) keep the per-fingerprint
    history strictly increasing, and _apply_notification_policy applies the
    exact same silence/outbox-ordering rules as for normal events.
    """
    fp = PROBE_FINGERPRINT
    seq = round_no * 2 - 1 if kind == "firing" else round_no * 2
    status = "firing" if kind == "firing" else "recovered"
    row = conn.execute(
        "SELECT 1 FROM alerts WHERE fingerprint=?", (fp,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO alerts(fingerprint, current_seq, status, summary, round, "
            "last_event_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (fp, seq, status, summary, round_no, now, now),
        )
    else:
        conn.execute(
            "UPDATE alerts SET current_seq=?, status=?, summary=?, round=?, "
            "last_event_at=?, updated_at=? WHERE fingerprint=?",
            (seq, status, summary, round_no, now, now, fp),
        )
    conn.execute(
        "INSERT INTO events(fingerprint, seq, kind, summary, received_at) "
        "VALUES (?,?,?,?,?)",
        (fp, seq, kind, summary, now),
    )
    return _apply_notification_policy(conn, fp, round_no, kind, summary, now)


def _mark_probe_lost(conn: sqlite3.Connection, row: sqlite3.Row, now: float) -> dict:
    """One lost round: alert goes firing and the probe flips to 'lost'.

    Callers hold a write transaction and only call this while the probe is
    'online' and expired, so each expiry produces exactly one round.
    """
    round_no = row["lost_round"] + 1
    timeout = row["timeout_seconds"]
    summary = f"probe heartbeat lost: no beat within {timeout:g}s"
    enqueued = _probe_alert_transition(conn, "firing", round_no, summary, now)
    conn.execute(
        "UPDATE probe_state SET status='lost', lost_round=?, updated_at=? WHERE id=1",
        (round_no, now),
    )
    return {"transition": "lost", "round": round_no, "notifications_enqueued": enqueued}


def _mark_probe_recovered(conn: sqlite3.Connection, round_no: int, now: float) -> dict:
    summary = "probe heartbeat restored"
    enqueued = _probe_alert_transition(conn, "recovery", round_no, summary, now)
    return {
        "transition": "recovered",
        "round": round_no,
        "notifications_enqueued": enqueued,
    }


def record_heartbeat(
    conn: sqlite3.Connection,
    now: Optional[float] = None,
    default_timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict:
    """Record one heartbeat. The deadline always counts from the server-side
    receive time, never from a client-supplied timestamp.

    A heartbeat that arrives after the deadline but before the sweep wrote the
    lost state still records the lost round first and recovers right after, so
    the outage is never silently swallowed.
    """
    now = time.time() if now is None else now
    transitions: list[dict] = []
    with transaction(conn):
        row = _probe_row(conn)
        if row is None:
            # First contact before any threshold was configured: adopt the
            # default timeout so the probe is monitored from now on.
            conn.execute(
                "INSERT INTO probe_state(id, timeout_seconds, last_beat_at, "
                "deadline_at, status, lost_round, updated_at) "
                "VALUES (1, ?, ?, ?, 'online', 0, ?)",
                (default_timeout, now, now + default_timeout, now),
            )
            first = True
        elif row["last_beat_at"] is None:
            # First heartbeat after configuration: pending -> online, no alert.
            conn.execute(
                "UPDATE probe_state SET status='online', last_beat_at=?, "
                "deadline_at=?, updated_at=? WHERE id=1",
                (now, now + row["timeout_seconds"], now),
            )
            first = True
        else:
            first = False
            lost_round = row["lost_round"]
            if row["status"] == "online" and _probe_expired(row, now):
                # Late heartbeat: record the lost round before recovering.
                transitions.append(_mark_probe_lost(conn, row, now))
                lost_round += 1
                status = "lost"
            else:
                status = row["status"]
            if status == "lost":
                transitions.append(_mark_probe_recovered(conn, lost_round, now))
            conn.execute(
                "UPDATE probe_state SET status='online', last_beat_at=?, "
                "deadline_at=?, updated_at=? WHERE id=1",
                (now, now + row["timeout_seconds"], now),
            )
    result = get_probe(conn, now)
    result["first_heartbeat"] = first
    result["transitions"] = transitions
    return result


def sweep_probes(conn: sqlite3.Connection, now: Optional[float] = None) -> list[dict]:
    """Mark the probe lost when its deadline has passed.

    Runs inside the same serialized transaction as heartbeats, so a concurrent
    heartbeat cannot produce a second lost round; continued absence after the
    transition does not re-fire (status is no longer 'online').
    """
    now = time.time() if now is None else now
    transitions: list[dict] = []
    with transaction(conn):
        row = _probe_row(conn)
        if row is not None and row["status"] == "online" and _probe_expired(row, now):
            transitions.append(_mark_probe_lost(conn, row, now))
    return transitions
