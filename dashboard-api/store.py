"""
Postgres persistence for the dashboard API.

Stores what must survive a restart: each device's latest state, alerts (with
who acknowledged them) and the audit log. Isolation is NOT stored here: the
Kafka topic `iomt.actions` is its single source of truth, and the dashboard
re-reads it on startup (see main.py).

All methods are blocking. main.py calls them through asyncio.to_thread so the
event loop (and the WebSocket pushes) never wait on the database.
"""
import time

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id  TEXT PRIMARY KEY,
    state      JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGINT PRIMARY KEY,
    device_id   TEXT NOT NULL,
    device_type TEXT,
    unit        TEXT,
    reasons     JSONB NOT NULL,
    severity    TEXT NOT NULL,
    risk_score  DOUBLE PRECISION NOT NULL,
    tips        JSONB NOT NULL,
    raised_at   TEXT NOT NULL,
    ack_by      TEXT,
    ack_at      TEXT,
    explanations JSONB NOT NULL DEFAULT '[]'
);
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS explanations JSONB NOT NULL DEFAULT '[]';
CREATE INDEX IF NOT EXISTS alerts_device_idx ON alerts (device_id);
CREATE TABLE IF NOT EXISTS audit_log (
    id        BIGINT PRIMARY KEY,
    logged_at TEXT NOT NULL,
    username  TEXT NOT NULL,
    role      TEXT NOT NULL,
    action    TEXT NOT NULL,
    target    TEXT NOT NULL DEFAULT '',
    detail    TEXT NOT NULL DEFAULT ''
);
"""


def _number(x: float):
    return int(x) if float(x).is_integer() else x


class Store:
    def __init__(self, url: str) -> None:
        self.url = url
        self.pool: ConnectionPool | None = None
        self.last_error: str | None = None

    @property
    def ready(self) -> bool:
        return self.pool is not None

    def connect(self, attempts: int = 15, delay: float = 2.0) -> bool:
        """Open the pool and create the tables. Retries because the database may still be starting."""
        for attempt in range(attempts):
            pool = ConnectionPool(self.url, min_size=1, max_size=4, open=False,
                                  kwargs={"row_factory": dict_row, "autocommit": True})
            try:
                pool.open(wait=True, timeout=5)
                with pool.connection() as conn:
                    conn.execute(SCHEMA)
                self.pool, self.last_error = pool, None
                return True
            except Exception as exc:
                self.last_error = str(exc)
                pool.close()
                print(f"dashboard-api: database not ready ({exc}); attempt {attempt + 1}/{attempts}")
                time.sleep(delay)
        return False

    def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            self.pool = None

    # ---- reading at startup ----
    def load(self, max_alerts: int, max_audit: int) -> dict:
        with self.pool.connection() as conn:
            devices = [r["state"] for r in conn.execute("SELECT state FROM devices")]
            alert_rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT %s", (max_alerts,)).fetchall()
            audit_rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT %s", (max_audit,)).fetchall()
            max_alert = conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM alerts").fetchone()["m"]
            max_audit_id = conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM audit_log").fetchone()["m"]
        alerts = [{
            "id": r["id"], "device_id": r["device_id"], "device_type": r["device_type"], "unit": r["unit"],
            "reasons": r["reasons"], "severity": r["severity"], "risk_score": _number(r["risk_score"]),
            "explanations": r["explanations"], "tips": r["tips"], "at": r["raised_at"],
            "ack": {"by": r["ack_by"], "at": r["ack_at"]} if r["ack_by"] else None,
        } for r in alert_rows]
        audit = [{
            "id": r["id"], "at": r["logged_at"], "user": r["username"], "role": r["role"],
            "action": r["action"], "target": r["target"], "detail": r["detail"],
        } for r in audit_rows]
        return {"devices": devices, "alerts": alerts, "audit": audit, "max_alert_id": max_alert, "max_audit_id": max_audit_id}

    # ---- writing ----
    def save_device(self, state: dict) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO devices (device_id, state, updated_at) VALUES (%s, %s, now()) "
                "ON CONFLICT (device_id) DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
                (state["device_id"], Jsonb(state)))

    def save_alert(self, a: dict) -> None:
        ack = a.get("ack") or {}
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO alerts (id, device_id, device_type, unit, reasons, severity, risk_score, tips, raised_at, ack_by, ack_at, explanations) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                (a["id"], a["device_id"], a.get("device_type"), a.get("unit"), Jsonb(a["reasons"]), a["severity"],
                 a["risk_score"], Jsonb(a.get("tips") or []), a["at"], ack.get("by"), ack.get("at"),
                 Jsonb(a.get("explanations") or [])))

    def ack_alert(self, alert_id: int, by: str, at: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE alerts SET ack_by = %s, ack_at = %s WHERE id = %s", (by, at, alert_id))

    def save_audit(self, e: dict) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO audit_log (id, logged_at, username, role, action, target, detail) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                (e["id"], e["at"], e["user"], e["role"], e["action"], e.get("target", ""), e.get("detail", "")))