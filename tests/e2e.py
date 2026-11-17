"""End-to-end key-path tests using the stdlib runner (no third-party deps).

Run:  python3 -m tests.e2e
Both services are started in-process on real HTTP ports with isolated SQLite
databases; assertions cover every rule from the requirements.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from runner.http_runner import run_alert_service, run_notifier_service  # noqa: E402
from common import service  # noqa: E402
from common.db import close, connect, init_db  # noqa: E402

ALERT_PORT = 18080
NOTIFIER_PORT = 18081
BASE = f"http://127.0.0.1:{ALERT_PORT}"
NOTIFIER = f"http://127.0.0.1:{NOTIFIER_PORT}"


def http(method: str, url: str, body=None, expect=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode())
            status = resp.status
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read().decode())
        status = exc.code
    if expect is not None:
        assert status == expect, f"{method} {url} -> {status} (want {expect}): {payload}"
    return status, payload


def wait_until(desc, predicate, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for: {desc} (last={last!r})")


def alert_status(fp):
    _, data = http("GET", f"{BASE}/api/alerts")
    for a in data["alerts"]:
        if a["fingerprint"] == fp:
            return a
    return None


def outbox_rows(fp):
    _, data = http("GET", f"{BASE}/api/notifications")
    return [n for n in data["notifications"] if n["fingerprint"] == fp]


def received_nos():
    _, data = http("GET", f"{NOTIFIER}/api/notifications")
    return {n["notification_no"]: n for n in data["notifications"]}


def wait_delivered(fp, expected_count, timeout=10.0):
    def check():
        rows = sorted(outbox_rows(fp), key=lambda r: r["id"])
        if len(rows) != expected_count:
            return None
        return rows if all(r["status"] == "delivered" for r in rows) else None
    return wait_until(f"{fp} {expected_count} delivered", check, timeout)


PROBE_FP = "probe:heartbeat"


def probe_state():
    _, data = http("GET", f"{BASE}/api/probe")
    return data


def probe_round_delivered(round_no, kind):
    return any(
        r["round"] == round_no and r["kind"] == kind and r["status"] == "delivered"
        for r in outbox_rows(PROBE_FP)
    )


class E2ETestCase(unittest.TestCase):
    tmpdir: str
    alert_server = None
    notifier_server = None

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="alert-e2e-")
        os.environ["ALERT_DB_PATH"] = os.path.join(cls.tmpdir, "alerts.db")
        os.environ["NOTIFIER_DB_PATH"] = os.path.join(cls.tmpdir, "notifier.db")
        os.environ["NOTIFIER_URL"] = NOTIFIER
        os.environ["WORKER_INTERVAL_SECONDS"] = "0.1"
        cls.notifier_server = run_notifier_service("127.0.0.1", NOTIFIER_PORT)
        cls.alert_server = run_alert_service("127.0.0.1", ALERT_PORT)
        wait_until("services healthy", lambda: (
            http("GET", f"{BASE}/health")[0] == 200
            and http("GET", f"{NOTIFIER}/health")[0] == 200
        ))

    @classmethod
    def tearDownClass(cls):
        cls.alert_server.stop_event.set()
        cls.alert_server.shutdown()
        cls.alert_server.server_close()
        cls.notifier_server.shutdown()
        cls.notifier_server.server_close()
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_01_health_and_pages(self):
        self.assertEqual(http("GET", f"{BASE}/health", expect=200)[1]["service"],
                         "alert-console")
        self.assertEqual(http("GET", f"{NOTIFIER}/health", expect=200)[1]["service"],
                         "local-notifier")
        for url in (f"{BASE}/", f"{NOTIFIER}/"):
            with urllib.request.urlopen(url, timeout=5) as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn(b"text/html", resp.headers["Content-Type"].encode())
        with urllib.request.urlopen(f"{BASE}/static/app.js", timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn(b"javascript", resp.headers["Content-Type"].encode())
        with urllib.request.urlopen(f"{NOTIFIER}/static/notifier.js", timeout=5) as resp:
            self.assertEqual(resp.status, 200)
        # path traversal must not escape the static directory (403 or 404 both safe)
        traversal_status, _ = http("GET", f"{BASE}/static/../../common/config.py")
        self.assertIn(traversal_status, (403, 404))

    def test_02_basic_firing_recovery_and_lost_receipt(self):
        fp = "svc/basic"
        s, r1 = http("POST", f"{BASE}/api/events",
                     {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "cpu high"},
                     expect=200)
        self.assertEqual(r1["result"], "accepted")
        self.assertEqual(r1["alert"]["status"], "firing")
        self.assertEqual(r1["alert"]["round"], 1)

        # identical replay keeps the same result, creates nothing new
        s, r2 = http("POST", f"{BASE}/api/events",
                     {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "cpu high"},
                     expect=200)
        self.assertEqual(r2["result"], "duplicate_replayed")

        # same seq different body -> rejected
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "changed text"},
             expect=409)

        # recovery
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 2, "kind": "recovery", "summary": "cpu back"},
             expect=200)
        self.assertEqual(alert_status(fp)["status"], "recovered")

        # stale seq cannot rewrite state
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 2, "kind": "firing", "summary": "late trigger"},
             expect=409)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "recovery", "summary": "old recovery"},
             expect=409)
        self.assertEqual(alert_status(fp)["status"], "recovered")

        rows = wait_delivered(fp, 2)
        kinds = [r["kind"] for r in rows]
        self.assertEqual(kinds, ["firing", "recovery"])

        # notifier: each number stored exactly once despite retried receipts
        recv = received_nos()
        firing_no = next(r["notification_no"] for r in rows if r["kind"] == "firing")
        rec_no = next(r["notification_no"] for r in rows if r["kind"] == "recovery")
        self.assertIn(firing_no, recv)
        self.assertIn(rec_no, recv)
        self.assertGreaterEqual(recv[firing_no]["attempts_seen"], 1)
        self.assertEqual(recv[firing_no]["status"], "accepted")
        # ordering on the receiver side: firing arrived before recovery
        self.assertLess(recv[firing_no]["first_seen_at"], recv[rec_no]["first_seen_at"])

        # outbox attempts show the lost receipt was retried with the same number
        firing_row = next(r for r in rows if r["kind"] == "firing")
        self.assertGreaterEqual(firing_row["attempts"], 2)

    def test_03_silence_parks_new_firing_but_recovery_still_notifies(self):
        fp = "svc/silenced"
        # fault announced before the maintenance window
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "disk full"},
             expect=200)
        wait_delivered(fp, 1)

        # enable silence
        _, sil = http("POST", f"{BASE}/api/silence",
                      {"duration_seconds": 30, "reason": "maintenance"}, expect=200)
        self.assertTrue(sil["active"])

        # new trigger during silence: alert updates, no notification enqueued
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 2, "kind": "firing", "summary": "disk fuller"},
             expect=200)
        a = alert_status(fp)
        self.assertEqual(a["status"], "firing")
        self.assertEqual(a["summary"], "disk fuller")
        time.sleep(0.3)
        self.assertEqual(len(outbox_rows(fp)), 1)

        # recovery of an already-announced fault MUST still be notified
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 3, "kind": "recovery", "summary": "disk ok"},
             expect=200)
        rows = wait_delivered(fp, 2)
        self.assertEqual([r["kind"] for r in rows], ["firing", "recovery"])

        # receiver shows both; no extra firing notification appeared
        recv = received_nos()
        self.assertEqual(
            sorted(k for k in recv if k.startswith(fp)),
            sorted(r["notification_no"] for r in rows),
        )
        http("DELETE", f"{BASE}/api/silence", expect=200)

    def test_04_silence_expiry_replays_only_still_firing_unannounced(self):
        fp = "svc/parked"
        http("POST", f"{BASE}/api/silence",
             {"duration_seconds": 1, "reason": "short window"}, expect=200)

        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "latency"},
             expect=200)
        # also a second fingerprint that recovers during the silence
        fp2 = "svc/parked-recovered"
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp2, "seq": 1, "kind": "firing", "summary": "temp spike"},
             expect=200)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp2, "seq": 2, "kind": "recovery", "summary": "spike gone"},
             expect=200)

        time.sleep(0.3)
        # nothing announced yet: fp2 recovered without ever being notified,
        # so it must never produce a notification for that round
        self.assertEqual(outbox_rows(fp), [])
        self.assertEqual(outbox_rows(fp2), [])

        # wait for the silence to expire and the worker sweep to release it
        rows = wait_delivered(fp, 1, timeout=8)
        self.assertEqual(rows[0]["kind"], "firing")
        time.sleep(0.3)
        self.assertEqual(outbox_rows(fp2), [], "recovered-during-silence round stays unannounced")

    def test_05_retrigger_after_recovery_is_a_new_round(self):
        fp = "svc/rounds"
        for seq, kind, text in [
            (1, "firing", "round1 fire"),
            (2, "recovery", "round1 rec"),
            (3, "firing", "round2 fire"),
            (4, "recovery", "round2 rec"),
        ]:
            http("POST", f"{BASE}/api/events",
                 {"fingerprint": fp, "seq": seq, "kind": kind, "summary": text}, expect=200)

        a = alert_status(fp)
        self.assertEqual(a["round"], 2)
        self.assertEqual(a["status"], "recovered")
        rows = wait_delivered(fp, 4)
        self.assertEqual([(r["round"], r["kind"]) for r in rows],
                         [(1, "firing"), (1, "recovery"), (2, "firing"), (2, "recovery")])
        # notification numbers are unique per round/kind
        self.assertEqual(len({r["notification_no"] for r in rows}), 4)

    def test_06_per_fingerprint_ordering_under_loss(self):
        fp = "svc/order"
        for seq, kind, text in [
            (1, "firing", "f1"),
            (2, "recovery", "r1"),
            (3, "firing", "f2"),
        ]:
            http("POST", f"{BASE}/api/events",
                 {"fingerprint": fp, "seq": seq, "kind": kind, "summary": text}, expect=200)
        rows = wait_delivered(fp, 3)
        self.assertEqual([r["kind"] for r in rows], ["firing", "recovery", "firing"])
        recv = received_nos()
        # Receiver first-seen timestamps must follow the outbox (id) order.
        times = [recv[r["notification_no"]]["first_seen_at"] for r in rows]
        self.assertEqual(times, sorted(times))

    def test_07_invalid_payloads(self):
        # stdlib runner maps validation rejects to 409; the FastAPI app uses
        # Pydantic and answers 422 for the same malformed input.
        http("POST", f"{BASE}/api/events", {"seq": 1, "kind": "firing", "summary": "x"},
             expect=409)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": "x", "seq": -1, "kind": "firing", "summary": "x"}, expect=409)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": "x", "seq": 1, "kind": "explode", "summary": "x"}, expect=409)
        http("POST", f"{BASE}/api/silence", {"duration_seconds": -3}, expect=422)
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 0}, expect=422)
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": -5}, expect=422)

    def test_08_restart_does_not_lose_or_reorder(self):
        # Persist a partially delivered outbox, restart both servers, verify
        # everything converges with no duplicates on the receiver.
        fp = "svc/restart"
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "before restart"},
             expect=200)
        wait_delivered(fp, 1)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 2, "kind": "firing", "summary": "still firing text"},
             expect=200)  # same round refresh, no new notification

        before = received_nos()
        before_nos = {k: v["attempts_seen"] for k, v in before.items() if k.startswith(fp)}

        # restart both processes on the same database files
        self.alert_server.stop_event.set()
        self.alert_server.shutdown()
        self.alert_server.server_close()
        self.notifier_server.shutdown()
        self.notifier_server.server_close()
        time.sleep(0.2)
        type(self).notifier_server = run_notifier_service("127.0.0.1", NOTIFIER_PORT)
        type(self).alert_server = run_alert_service("127.0.0.1", ALERT_PORT)
        wait_until("services healthy after restart", lambda: (
            http("GET", f"{BASE}/health")[0] == 200
            and http("GET", f"{NOTIFIER}/health")[0] == 200
        ))

        # state survived
        self.assertEqual(alert_status(fp)["summary"], "still firing text")

        # post a recovery after restart; it must flow through and no duplicate
        # firing notification may be produced
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 3, "kind": "recovery", "summary": "after restart rec"},
             expect=200)
        rows = wait_delivered(fp, 2)
        self.assertEqual([r["kind"] for r in rows], ["firing", "recovery"])

        after = received_nos()
        for no, seen in before_nos.items():
            # the pre-restart notification number is still a single stored row
            self.assertIn(no, after)
            self.assertEqual(after[no]["first_seen_at"], before[no]["first_seen_at"])

        # a late, older trigger must not overwrite the recovered state
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 2, "kind": "firing", "summary": "late again"},
             expect=409)
        self.assertEqual(alert_status(fp)["status"], "recovered")

    def test_09_probe_lifecycle_and_single_lost_round(self):
        fp = PROBE_FP
        # nothing configured yet: pending, and certainly no lost alert
        p = probe_state()
        self.assertFalse(p["configured"])
        self.assertEqual(p["status"], "pending")

        # configure the threshold: still pending until the first heartbeat
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 1.5}, expect=200)
        self.assertEqual(probe_state()["status"], "pending")

        # no lost alert before the first heartbeat, even well past the timeout
        time.sleep(2.0)
        self.assertEqual(probe_state()["status"], "pending")
        self.assertEqual(outbox_rows(fp), [])

        # first heartbeat -> online, deadline counts from server receive time
        _, p = http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
        self.assertTrue(p["first_heartbeat"])
        self.assertEqual(p["status"], "online")
        self.assertAlmostEqual(p["deadline_at"] - p["last_beat_at"], 1.5, places=3)

        # expiry -> exactly one lost round, firing notification delivered
        wait_until("probe lost", lambda: probe_state()["status"] == "lost")
        rows = wait_delivered(fp, 1)
        self.assertEqual([(r["round"], r["kind"]) for r in rows], [(1, "firing")])

        # continued absence does NOT produce another round
        time.sleep(2.5)
        p = probe_state()
        self.assertEqual(p["status"], "lost")
        self.assertEqual(p["lost_round"], 1)
        self.assertEqual(len(outbox_rows(fp)), 1)

        # heartbeat returns -> recovery follows the firing through the outbox
        _, p = http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
        self.assertEqual(p["status"], "online")
        rows = wait_delivered(fp, 2)
        self.assertEqual([r["kind"] for r in rows], ["firing", "recovery"])
        recv = received_nos()
        times = [recv[r["notification_no"]]["first_seen_at"] for r in rows]
        self.assertEqual(times, sorted(times))

        # expiring again starts exactly one more round (round 2, not more)
        wait_until("probe lost again", lambda: probe_state()["status"] == "lost")
        self.assertEqual(probe_state()["lost_round"], 2)
        rows = wait_delivered(fp, 3)
        self.assertEqual([r["kind"] for r in rows], ["firing", "recovery", "firing"])

        # recover and park the probe with a long timeout for later tests
        http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
        rows = wait_delivered(fp, 4)
        self.assertEqual([r["kind"] for r in rows],
                         ["firing", "recovery", "firing", "recovery"])
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 300}, expect=200)

    def test_10_probe_silence_rules_and_reserved_namespace(self):
        fp = PROBE_FP
        p = probe_state()
        self.assertEqual(p["status"], "online")
        round0 = p["lost_round"]

        # normal events may not use the reserved probe namespace
        http("POST", f"{BASE}/api/events",
             {"fingerprint": fp, "seq": 1, "kind": "firing", "summary": "fake"},
             expect=409)
        http("POST", f"{BASE}/api/events",
             {"fingerprint": "probe:other", "seq": 1, "kind": "firing", "summary": "x"},
             expect=409)
        # ...and the probe alert was not overwritten by those attempts
        self.assertEqual(probe_state()["lost_round"], round0)

        # silence the console, refresh the heartbeat, then tighten the timeout
        # (the deadline is recomputed from the fresh beat, ~1s out)
        http("POST", f"{BASE}/api/silence",
             {"duration_seconds": 60, "reason": "probe silence test"}, expect=200)
        try:
            http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
            http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 1}, expect=200)
            wait_until("probe lost under silence",
                       lambda: probe_state()["status"] == "lost")
            self.assertEqual(probe_state()["lost_round"], round0 + 1)
            # silenced: the lost firing is parked, nothing enqueued for the new round
            self.assertEqual(
                [r for r in outbox_rows(fp) if r["round"] == round0 + 1], [])

            # releasing the silence parks the still-lost probe into the outbox
            http("DELETE", f"{BASE}/api/silence", expect=200)
            wait_until("lost firing released",
                       lambda: probe_round_delivered(round0 + 1, "firing"))

            # recovery must not overtake the firing even with lost receipts
            http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
            wait_until("probe recovery delivered",
                       lambda: probe_round_delivered(round0 + 1, "recovery"))
            recv = received_nos()
            self.assertLess(recv[f"{fp}#r{round0 + 1}:firing"]["first_seen_at"],
                            recv[f"{fp}#r{round0 + 1}:recovery"]["first_seen_at"])
        finally:
            http("DELETE", f"{BASE}/api/silence")

        # park the probe again for the restart test
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 300}, expect=200)

    def test_11_probe_deadline_survives_restart(self):
        round0 = probe_state()["lost_round"]
        # fresh heartbeat first, then tighten the timeout so the deadline is
        # ~2s out (config recomputes it from the last beat)
        http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
        _, p = http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 2},
                    expect=200)
        deadline, last_beat = p["deadline_at"], p["last_beat_at"]

        # restart the alert service on the same database file
        self.alert_server.stop_event.set()
        self.alert_server.shutdown()
        self.alert_server.server_close()
        time.sleep(0.2)
        type(self).alert_server = run_alert_service("127.0.0.1", ALERT_PORT)
        wait_until("alert service healthy after restart",
                   lambda: http("GET", f"{BASE}/health")[0] == 200)

        # the stored deadline is still in effect, not reset by the restart
        p2 = probe_state()
        self.assertEqual(p2["status"], "online")
        self.assertAlmostEqual(p2["deadline_at"], deadline, places=3)
        self.assertAlmostEqual(p2["last_beat_at"], last_beat, places=3)

        # without new heartbeats the persisted deadline expires: one lost round
        wait_until("probe lost after restart",
                   lambda: probe_state()["status"] == "lost")
        self.assertEqual(probe_state()["lost_round"], round0 + 1)
        wait_until("lost firing delivered",
                   lambda: probe_round_delivered(round0 + 1, "firing"))

        # recover after the restart and park the probe
        http("POST", f"{BASE}/api/probe/heartbeat", expect=200)
        wait_until("recovery delivered",
                   lambda: probe_round_delivered(round0 + 1, "recovery"))
        http("POST", f"{BASE}/api/probe/config", {"timeout_seconds": 300}, expect=200)

    def test_12_probe_late_heartbeat_and_sweep_concurrency(self):
        # Service-level test with explicit clocks: a heartbeat that arrives
        # after the deadline but before the sweep ran must still record the
        # lost round first and recover right after; concurrent sweeps and
        # heartbeats must never produce two rounds for one expiry.
        db_path = os.path.join(self.tmpdir, "probe-concurrency.db")
        init_db(db_path)
        conn = connect(db_path)
        try:
            service.set_probe_timeout(conn, 10, now=100.0)
            r = service.record_heartbeat(conn, now=100.0)
            self.assertEqual((r["status"], r["first_heartbeat"]), ("online", True))

            # expired at 110; heartbeat at 115 lands before any sweep
            r = service.record_heartbeat(conn, now=115.0)
            self.assertEqual([t["transition"] for t in r["transitions"]],
                             ["lost", "recovered"])
            self.assertEqual(r["status"], "online")
            self.assertEqual(r["lost_round"], 1)
            self.assertAlmostEqual(r["deadline_at"], 125.0)
            # the lost round is on record: alert history + outbox rows
            events = service.list_events(conn, PROBE_FP)
            self.assertEqual([(e["seq"], e["kind"]) for e in reversed(events)],
                             [(1, "firing"), (2, "recovery")])
            kinds = sorted((n["round"], n["kind"]) for n in
                           service.list_notifications(conn))
            self.assertEqual(kinds, [(1, "firing"), (1, "recovery")])

            # sweep before the new deadline: nothing; after it: one lost round
            self.assertEqual(service.sweep_probes(conn, now=120.0), [])
            lost = service.sweep_probes(conn, now=126.0)
            self.assertEqual([t["transition"] for t in lost], ["lost"])
            # already lost: further sweeps do not repeat
            self.assertEqual(service.sweep_probes(conn, now=130.0), [])
            r = service.record_heartbeat(conn, now=131.0)
            self.assertEqual([t["transition"] for t in r["transitions"]], ["recovered"])

            # refresh inside the deadline, then race sweeps vs heartbeats
            r = service.record_heartbeat(conn, now=140.0)
            self.assertEqual(r["transitions"], [])
            self.assertAlmostEqual(r["deadline_at"], 150.0)

            threads = []
            for _ in range(4):
                threads.append(threading.Thread(
                    target=service.sweep_probes, args=(conn,), kwargs={"now": 155.0}))
                threads.append(threading.Thread(
                    target=service.record_heartbeat, args=(conn,), kwargs={"now": 155.0}))
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # exactly one lost round came out of the race (round 3), never two
            p = service.get_probe(conn, now=156.0)
            self.assertEqual(p["status"], "online")
            self.assertEqual(p["lost_round"], 3)
            notes = service.list_notifications(conn)
            self.assertEqual(
                sorted((n["round"], n["kind"]) for n in notes),
                [(1, "firing"), (1, "recovery"), (2, "firing"), (2, "recovery"),
                 (3, "firing"), (3, "recovery")])
            events = service.list_events(conn, PROBE_FP)
            self.assertEqual(len(events), 6)  # no duplicated synthetic events
        finally:
            close(conn)


if __name__ == "__main__":
    unittest.main(verbosity=2)
