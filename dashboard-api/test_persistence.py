"""
Persistence tests against a real Postgres.

    DATABASE_URL=postgresql://user@host/medguard python3 dashboard-api/test_persistence.py

Skips (exit 0) when DATABASE_URL is not set. Run from the repository root.

The database named in DATABASE_URL must end in "_test" (for example medguard_test). The test
creates it if it is missing and drops and recreates its tables, so it refuses to run against
anything else, which keeps your demo data safe.
"""
import os, sys
URL = os.environ.get("DATABASE_URL")
if not URL:
    print("DATABASE_URL not set: skipping persistence tests"); sys.exit(0)
os.environ["DISABLE_KAFKA"] = "1"
os.environ["DB_CONNECT_ATTEMPTS"] = "1"
sys.path.insert(0, "dashboard-api")
from fastapi.testclient import TestClient
import psycopg
import psycopg.sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import auth, main

# ---- make sure the test database exists, and refuse to touch any other one ----
_info = conninfo_to_dict(URL)
_dbname = _info.get("dbname", "")
if not _dbname.endswith("_test"):
    sys.exit(f"Refusing to run: database '{_dbname}' does not end in _test. "
             "This test drops tables, so point DATABASE_URL at a throwaway database such as medguard_test.")
with psycopg.connect(make_conninfo(**{**_info, "dbname": "postgres"}), autocommit=True) as _admin:
    if not _admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (_dbname,)).fetchone():
        _admin.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(_dbname)))
        print(f"created database {_dbname}")

H = lambda t: {"Authorization": f"Bearer {t}"}
base = {"message_id": "m1", "device_id": "infusion-pump-001", "device_type": "infusion_pump", "patient_id": "P-1",
        "unit": "ICU-3", "timestamp": "2026-10-15T10:00:00Z", "ingested_at": "2026-10-15T10:00:01+00:00",
        "risk_score": 0, "vulnerability_flags": ["auto_lock_disabled"], "remediation_tips": ["tip"]}

def count(table):
    with psycopg.connect(URL) as c:
        return c.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

def forget_everything():
    """What a process restart does to memory."""
    main.devices.clear(); main.alerts.clear(); main._signatures.clear(); main._alert_seq = 0
    auth.audit.clear(); auth._audit_seq = 0; auth.sessions.clear()

with psycopg.connect(URL, autocommit=True) as c:
    c.execute("DROP TABLE IF EXISTS devices, alerts, audit_log")

# ---- run 1: normal use writes through to Postgres ----
main.DATABASE_URL = URL
with TestClient(main.app) as c:
    assert c.get("/health").json()["persistence"] == "postgres"
    admin = c.post("/api/login", json={"username": "admin", "password": "admin123"}).json()["token"]
    c.portal.call(main.process, base)
    first_alert = main.alerts[0]
    assert c.post(f"/api/alerts/{first_alert['id']}/ack", headers=H(admin)).status_code == 200
    c.portal.call(main.process, dict(base, message_id="m2", vulnerability_flags=["auto_lock_disabled", "infusion_rate_mismatch"]))
    second_id = main.alerts[0]["id"]
    assert second_id == first_alert["id"] + 1
    assert count("devices") == 1 and count("alerts") == 2 and count("audit_log") >= 4
    with psycopg.connect(URL) as conn:
        assert conn.execute("SELECT ack_by FROM alerts WHERE id=%s", (first_alert["id"],)).fetchone()[0] == "admin"

# ---- run 2: after a "restart" everything comes back ----
forget_everything()
with TestClient(main.app) as c:
    admin = c.post("/api/login", json={"username": "admin", "password": "admin123"}).json()["token"]
    devs = c.get("/api/devices", headers=H(admin)).json()
    assert [d["device_id"] for d in devs] == ["infusion-pump-001"]
    alerts = c.get("/api/alerts", headers=H(admin)).json()
    assert [a["id"] for a in alerts] == [second_id, first_alert["id"]]            # newest first, order kept
    acked = next(a for a in alerts if a["id"] == first_alert["id"])
    assert acked["ack"]["by"] == "admin" and next(a for a in alerts if a["id"] == second_id)["ack"] is None
    audit = c.get("/api/audit", headers=H(admin)).json()
    actions = [e["action"] for e in audit]
    assert "alert_raised" in actions and "alert_ack" in actions and actions.count("login") == 2
    assert audit[0]["id"] > audit[-1]["id"]                                        # numbering continues, no restart at 1
    # the same problems arriving again do NOT raise a duplicate alert...
    c.portal.call(main.process, dict(base, message_id="m3", vulnerability_flags=["auto_lock_disabled", "infusion_rate_mismatch"]))
    assert len(main.alerts) == 2
    # ...but a change does, and the id continues from where it was
    c.portal.call(main.process, dict(base, message_id="m4", vulnerability_flags=["tls_disabled"]))
    assert main.alerts[0]["id"] == second_id + 1
    # nurses still only see their own devices after a restore
    nurse = c.post("/api/login", json={"username": "nurse.ben", "password": "nurse123"}).json()["token"]
    assert c.get("/api/devices", headers=H(nurse)).json() == []

# ---- a database failure mid-run must not stop monitoring ----
with TestClient(main.app) as c:
    main.db.close()
    c.portal.call(main.process, dict(base, message_id="m5", vulnerability_flags=["firmware_outdated"]))
    assert main.devices["infusion-pump-001"]["flags"] == ["firmware_outdated"]

# ---- database unreachable at startup: run in memory, say so ----
forget_everything()
main.DATABASE_URL = "postgresql://nobody@127.0.0.1:1/none"
main.db = None
with TestClient(main.app) as c:
    h = c.get("/health").json()
    assert h["persistence"] == "memory" and h["database_error"] is None
    c.portal.call(main.process, base)
    assert len(main.devices) == 1
print("all persistence checks passed")