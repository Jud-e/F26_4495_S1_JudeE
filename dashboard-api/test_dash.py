import os, sys
os.environ["DISABLE_KAFKA"] = "1"
os.environ["DATABASE_URL"] = ""   # these tests are in-memory only; never touch a real database
sys.path.insert(0, "dashboard-api")
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import main

base = {"message_id":"m1","device_id":"infusion-pump-001","device_type":"infusion_pump","patient_id":"P-1001",
        "unit":"ICU-3","timestamp":"2026-10-15T10:00:00Z","ingested_at":"2026-10-15T10:00:01+00:00",
        "packet_size_bytes":512,"request_rate_per_min":12.0,"infusion_rate_ml_per_hr":75.0,
        "occlusion_pressure_mmHg":4.0,"battery_pct":80,"risk_score":0}
flagged = dict(base, message_id="m2", vulnerability_flags=["auto_lock_disabled","infusion_rate_mismatch"],
               remediation_tips=["tip a","tip b"], infusion_rate_ml_per_hr=300.0)
monitor = dict(base, message_id="m6", device_id="cardiac-monitor-001", device_type="cardiac_monitor", risk_score=88)

def login(c, u, p):
    r = c.post("/api/login", json={"username": u, "password": p}); return r

def hdr(tok): return {"Authorization": f"Bearer {tok}"}

with TestClient(main.app) as c:
    # pages and redirects
    assert c.get("/", follow_redirects=False).status_code in (302, 307)
    for page in ("/login", "/admin", "/nurse"):
        r = c.get(page); assert r.status_code == 200 and 'id="root"' in r.text, page
    assert c.get("/favicon.ico").status_code == 204
    assert c.get("/health").json()["devices"] == 0

    # auth: bad login, no token, good logins
    assert login(c, "admin", "nope").status_code == 401
    assert c.get("/api/devices").status_code == 401
    admin = login(c, "admin", "admin123").json(); nurse = login(c, "nurse.amara", "nurse123").json()
    other = login(c, "nurse.ben", "nurse123").json()
    assert admin["user"]["role"] == "admin" and nurse["user"]["role"] == "nurse"
    A, N, B = admin["token"], nurse["token"], other["token"]

    # role checks
    assert c.get("/api/audit", headers=hdr(N)).status_code == 403
    assert c.post("/api/devices/x/isolate", headers=hdr(N)).status_code == 403
    assert c.get("/api/me", headers=hdr(N)).json()["username"] == "nurse.amara"

    # websocket needs a valid token
    try:
        with c.websocket_connect("/ws?token=bogus"): pass
        raise AssertionError("bogus token was accepted")
    except WebSocketDisconnect: pass

    with c.websocket_connect(f"/ws?token={A}") as wa, c.websocket_connect(f"/ws?token={N}") as wn, \
         c.websocket_connect(f"/ws?token={B}") as wb:
        for w in (wa, wn, wb): assert w.receive_json()["type"] == "snapshot"

        # pump alert: admin + amara get it, ben (cardiac monitors only) does not
        c.portal.call(main.process, dict(base))
        assert wa.receive_json()["type"] == "device" and wn.receive_json()["type"] == "device"
        c.portal.call(main.process, flagged)
        assert wa.receive_json()["type"] == "device"; assert wn.receive_json()["type"] == "device"
        al = wa.receive_json(); assert al["type"] == "alert" and al["alert"]["severity"] == "high"
        assert wn.receive_json()["type"] == "alert"
        au = wa.receive_json(); assert au["type"] == "audit" and au["entry"]["action"] == "alert_raised"
        assert "nurse.amara" in au["entry"]["detail"] and "nurse.ben" not in au["entry"]["detail"]

        # dedupe still works
        c.portal.call(main.process, dict(flagged, message_id="m3"))
        assert wa.receive_json()["type"] == "device" and wn.receive_json()["type"] == "device"
        assert len(c.get("/api/alerts", headers=hdr(A)).json()) == 1

        # nurse filtering over REST
        assert len(c.get("/api/alerts", headers=hdr(N)).json()) == 1
        assert c.get("/api/alerts", headers=hdr(B)).json() == []
        assert c.get("/api/devices", headers=hdr(B)).json() == []

        # ack: ben cannot, amara can; admin sees the update and an audit entry
        aid = al["alert"]["id"]
        assert c.post(f"/api/alerts/{aid}/ack", headers=hdr(B)).status_code == 404
        r = c.post(f"/api/alerts/{aid}/ack", headers=hdr(N)).json(); assert r["ack"]["by"] == "nurse.amara"
        assert wa.receive_json()["type"] == "alert_update" and wn.receive_json()["type"] == "alert_update"
        e = wa.receive_json(); assert e["entry"]["action"] == "alert_ack" and e["entry"]["user"] == "nurse.amara"

        # second device only goes to admin + ben
        c.portal.call(main.process, monitor)
        assert wa.receive_json()["type"] == "device" and wb.receive_json()["type"] == "device"
        assert wa.receive_json()["alert"]["reasons"] == ["high_risk_score"]
        assert wb.receive_json()["type"] == "alert"
        assert wa.receive_json()["type"] == "audit"

        # isolate / restore (admin), survives new telemetry, audited
        r = c.post("/api/devices/cardiac-monitor-001/isolate", headers=hdr(A)).json(); assert r["isolated"] is True
        assert wa.receive_json()["device"]["isolated"] is True and wb.receive_json()["device"]["isolated"] is True
        assert wa.receive_json()["entry"]["action"] == "device_isolated"
        c.portal.call(main.process, dict(monitor, message_id="m7"))
        assert wa.receive_json()["device"]["isolated"] is True; wb.receive_json()
        assert c.post("/api/devices/nope/isolate", headers=hdr(A)).status_code == 404

    # audit contents (admin only), users list, logout kills the session
    actions = [e["action"] for e in c.get("/api/audit", headers=hdr(A)).json()]
    for needed in ("login", "login_failed", "alert_raised", "alert_ack", "device_isolated"):
        assert needed in actions, (needed, actions)
    users = c.get("/api/users", headers=hdr(A)).json(); assert {u["username"] for u in users} == set(main.auth.USERS)
    assert "password" not in str(users)
    assert c.post("/api/logout", headers=hdr(N)).status_code == 200
    assert c.get("/api/me", headers=hdr(N)).status_code == 401
    devs = c.get("/api/devices", headers=hdr(A)).json()
    assert [d["device_id"] for d in devs] == ["cardiac-monitor-001", "infusion-pump-001"], devs

# ---- isolation goes through the actions topic ----
sent = []
with TestClient(main.app) as c:
    A = login(c, "admin", "admin123").json()["token"]
    c.portal.call(main.process, dict(base, message_id="z1", device_id="infusion-pump-002"))
    os.environ["DISABLE_KAFKA"] = "0"               # exercise the Kafka path with a fake publisher
    main.publish_action = lambda ev: sent.append(ev)
    assert c.post("/api/devices/infusion-pump-002/isolate", headers=hdr(A)).status_code == 200
    assert sent[0]["device_id"] == "infusion-pump-002" and sent[0]["isolated"] is True and sent[0]["by"] == "admin"
    assert not main.isolated.get("infusion-pump-002")            # nothing changes until the command is read back
    c.portal.call(main.apply_action, sent[0])
    assert main.isolated["infusion-pump-002"] is True and main.devices["infusion-pump-002"]["isolated"] is True

    def boom(ev): raise RuntimeError("kafka down")
    main.publish_action = boom
    assert c.post("/api/devices/infusion-pump-002/restore", headers=hdr(A)).status_code == 503
    assert main.isolated["infusion-pump-002"] is True            # a failed command changes nothing
    assert "isolation_failed" in [e["action"] for e in c.get("/api/audit", headers=hdr(A)).json()]

    # a command for a device we have not seen yet is remembered and applied when it appears
    c.portal.call(main.apply_action, {"device_id": "cardiac-monitor-002", "isolated": True})
    c.portal.call(main.process, dict(base, message_id="z2", device_id="cardiac-monitor-002", device_type="cardiac_monitor"))
    assert main.devices["cardiac-monitor-002"]["isolated"] is True
    c.portal.call(main.apply_action, {"device_id": "x", "isolated": "yes"})   # malformed commands are ignored
    assert "x" not in main.isolated
    os.environ["DISABLE_KAFKA"] = "1"
# ---- the model's explanation reaches the alert and the device ----
with TestClient(main.app) as c:
    A = login(c, "admin", "admin123").json()["token"]
    why = ["infusion_rate_ml_per_hr = 400 (normal 20 to 150)"]
    c.portal.call(main.process, dict(base, message_id="e1", device_id="infusion-pump-099", risk_score=95, risk_reasons=why))
    a = main.alerts[0]
    assert a["device_id"] == "infusion-pump-099" and "high_risk_score" in a["reasons"] and a["explanations"] == why
    dev = next(d for d in c.get("/api/devices", headers=hdr(A)).json() if d["device_id"] == "infusion-pump-099")
    assert dev["risk_reasons"] == why
    c.portal.call(main.process, dict(base, message_id="e2", device_id="infusion-pump-098", risk_score=10, risk_reasons=why))
    assert main.devices["infusion-pump-098"]["risk_reasons"] == why      # kept on the device...
    assert not [x for x in main.alerts if x["device_id"] == "infusion-pump-098"]   # ...but a low score raises no alert
# ---- risk history: charts and exports ----
import csv as _csv, io as _io, time as _time
with TestClient(main.app) as c:
    HA = hdr(login(c, "admin", "admin123").json()["token"])
    amara = hdr(login(c, "nurse.amara", "nurse123").json()["token"]); ben = hdr(login(c, "nurse.ben", "nurse123").json()["token"])
    now = _time.time()
    def iso(ago): return _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(now - ago))
    def send(i, score, ago, dev="infusion-pump-097"):
        c.portal.call(main.process, dict(base, message_id=f"h{i}", device_id=dev, risk_score=score, ingested_at=iso(ago)))
    for i, (score, ago) in enumerate([(10, 1800), (95, 900), (20, 895), (30, 300), (5, 30)]):
        send(i, score, ago)
    send(9, 60, 29)                                                    # same 10 s slot as the 5: the higher score is kept
    h = c.get("/api/devices/infusion-pump-097/history?minutes=60", headers=HA).json()
    scores = [p["score"] for p in h["points"]]
    assert h["minutes"] == 60 and 95 in scores and 60 in scores and 5 not in scores, scores
    assert [p["t"] for p in h["points"]] == sorted(p["t"] for p in h["points"]) and len(scores) <= 125
    short = [p["score"] for p in c.get("/api/devices/infusion-pump-097/history?minutes=15", headers=HA).json()["points"]]
    assert 10 not in short and 95 in short                              # the 30-minute-old point is outside a 15-minute window
    big = c.get("/api/devices/infusion-pump-097/history?minutes=100000", headers=HA).json()
    assert big["minutes"] == main.HISTORY_HOURS * 60 and max(p["score"] for p in big["points"]) == 95   # clamped, thinned, peak kept
    # access: amara owns infusion pumps, ben does not
    assert c.get("/api/devices/infusion-pump-097/history", headers=amara).status_code == 200
    assert c.get("/api/devices/infusion-pump-097/history", headers=ben).status_code == 404
    assert c.get("/api/devices/nope/history", headers=HA).status_code == 404
    assert c.get("/api/devices/infusion-pump-097/history").status_code == 401
    # history export
    r = c.get("/api/devices/infusion-pump-097/history?minutes=60&format=csv", headers=HA)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv") and "attachment" in r.headers["content-disposition"]
    rows = list(_csv.reader(_io.StringIO(r.text.lstrip("\ufeff"))))
    assert rows[0] == ["device_id", "time", "max_risk_score"] and len(rows) == len(scores) + 1
    assert c.get("/api/devices/infusion-pump-097/history?format=xml", headers=HA).status_code == 400
    # audit export: admin only, filtered, audited, formula-safe
    assert c.get("/api/audit/export", headers=amara).status_code == 403
    assert c.get("/api/audit/export").status_code == 401
    c.portal.call(main.log, "admin", "admin", "device_isolated", "=cmd|calc", "+1 detail")
    r = c.get("/api/audit/export?action=device_isolated", headers=HA)
    rows = list(_csv.DictReader(_io.StringIO(r.text.lstrip("\ufeff"))))
    assert rows and all(x["action"] == "device_isolated" for x in rows)
    assert any(x["target"] == "'=cmd|calc" and x["detail"] == "'+1 detail" for x in rows)
    js = c.get("/api/audit/export?format=json&q=cmd", headers=HA).json()
    assert len(js) >= 1 and js[0]["target"] == "=cmd|calc"            # JSON is not altered
    assert any(e["action"] == "audit_exported" for e in main.auth.audit)
    # alert export: role filtered
    c.portal.call(main.process, dict(base, message_id="x1", device_id="cardiac-monitor-009", device_type="cardiac_monitor",
                                     risk_score=90, ingested_at=iso(5), risk_reasons=["heart_rate_bpm = 300 (normal 50 to 120)"]))
    ids = lambda hh: {x["device_id"] for x in _csv.DictReader(_io.StringIO(c.get("/api/alerts/export", headers=hh).text.lstrip("\ufeff")))}
    assert "cardiac-monitor-009" in ids(HA) and "cardiac-monitor-009" in ids(ben) and "cardiac-monitor-009" not in ids(amara)
    assert c.get("/api/alerts/export?format=json", headers=ben).json()[0]["device_id"].startswith("cardiac-monitor")
# ---- remediation: flagged -> fixed -> rechecked ----
with TestClient(main.app) as c:
    HA = hdr(login(c, "admin", "admin123").json()["token"])
    amara = hdr(login(c, "nurse.amara", "nurse123").json()["token"]); ben = hdr(login(c, "nurse.ben", "nurse123").json()["token"])
    main.RECHECK_CLEAN_MESSAGES, main.RECHECK_MIN_SECONDS = 3, 20
    t0 = _time.time()
    def stamp(off): return _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(t0 + off))
    def msg(dev, flags, off, n=[0]):
        n[0] += 1
        c.portal.call(main.process, dict(base, message_id=f"r{n[0]}", device_id=dev, vulnerability_flags=flags, risk_score=0, ingested_at=stamp(off)))
    def alert_for(dev): return next(a for a in c.get("/api/alerts", headers=HA).json() if a["device_id"] == dev)

    # happy path: flagged, marked fixed, three clean messages over 20+ seconds -> verified
    msg("infusion-pump-081", ["tls_disabled"], -5)
    a = alert_for("infusion-pump-081"); assert a["remediation"]["status"] == "open"
    assert c.post(f"/api/alerts/{a['id']}/fix", json={"note": "x"}, headers=amara).status_code == 422          # a note is required
    assert c.post(f"/api/alerts/{a['id']}/fix", json={"note": "Re-enabled TLS"}, headers=ben).status_code == 404   # not ben's device
    r = c.post(f"/api/alerts/{a['id']}/fix", json={"note": "Re-enabled TLS on the pump"}, headers=amara).json()
    assert r["remediation"]["status"] == "fixed" and r["remediation"]["fixed"]["by"] == "nurse.amara" and r["ack"]["by"] == "nurse.amara"
    assert c.post(f"/api/alerts/{a['id']}/fix", json={"note": "again please"}, headers=amara).status_code == 409
    msg("infusion-pump-081", [], -10)                       # older than the fix: ignored
    assert alert_for("infusion-pump-081")["remediation"]["clean"] == 0
    msg("infusion-pump-081", [], 5); msg("infusion-pump-081", [], 12)
    assert alert_for("infusion-pump-081")["remediation"]["status"] == "fixed"     # two clean so far
    msg("infusion-pump-081", [], 15)                        # three clean, but only 15 s since the fix
    assert alert_for("infusion-pump-081")["remediation"]["status"] == "fixed"
    msg("infusion-pump-081", [], 25)
    rem = alert_for("infusion-pump-081")["remediation"]
    assert rem["status"] == "verified" and rem["verified_at"] == stamp(25)
    actions = [e["action"] for e in main.auth.audit]
    assert "alert_fixed" in actions and "fix_verified" in actions

    # the fix did not work: the same problem comes back -> reopened, no second alert, audited
    msg("infusion-pump-082", ["tls_disabled"], -5); b = alert_for("infusion-pump-082")
    c.post(f"/api/alerts/{b['id']}/fix", json={"note": "Changed the setting"}, headers=HA)
    before = len([x for x in main.alerts if x["device_id"] == "infusion-pump-082"])
    msg("infusion-pump-082", [], 5)
    msg("infusion-pump-082", ["tls_disabled"], 10)          # clears, then returns before the recheck finishes
    b = alert_for("infusion-pump-082")
    assert b["remediation"]["status"] == "reopened" and b["remediation"]["reopens"] == 1 and b["remediation"]["clean"] == 0
    assert len([x for x in main.alerts if x["device_id"] == "infusion-pump-082"]) == before       # no duplicate alert
    assert any(e["action"] == "fix_failed" and "tls_disabled" in e["detail"] for e in main.auth.audit)
    assert c.post(f"/api/alerts/{b['id']}/fix", json={"note": "Second attempt, replaced cert"}, headers=HA).status_code == 200   # can be fixed again

    # a different problem on the same device does not count as the old one coming back
    msg("infusion-pump-083", ["tls_disabled"], -5); d = alert_for("infusion-pump-083")
    c.post(f"/api/alerts/{d['id']}/fix", json={"note": "Fixed TLS"}, headers=HA)
    msg("infusion-pump-083", ["firmware_outdated"], 5)
    assert next(x for x in c.get("/api/alerts", headers=HA).json() if x["id"] == d["id"])["remediation"]["status"] == "fixed"

    # export carries the lifecycle
    rows = list(_csv.DictReader(_io.StringIO(c.get("/api/alerts/export", headers=HA).text.lstrip("\ufeff"))))
    row = next(x for x in rows if x["device_id"] == "infusion-pump-081")
    assert row["status"] == "verified" and row["fixed_by"] == "nurse.amara" and row["fix_note"] == "Re-enabled TLS on the pump"
print("all dashboard-api checks passed")