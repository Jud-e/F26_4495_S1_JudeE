import os, sys
os.environ["DISABLE_KAFKA"] = "1"
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
print("all dashboard-api checks passed")
