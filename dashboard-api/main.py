"""
MedGuard dashboard API.

Reads scored telemetry from Kafka, keeps the latest state of every device in
memory, derives alerts, and pushes both to browsers over a WebSocket.

    iomt.telemetry.scored  ->  [this service]  ->  /login, /admin, /nurse (pages)
                                                   POST /api/login, /api/logout, GET /api/me
                                                   GET  /api/devices, /api/alerts  (filtered by role)
                                                   POST /api/alerts/{id}/ack
                                                   POST /api/devices/{id}/isolate | /restore  (admin; publishes to iomt.actions)
                                                   GET  /api/audit, /api/users                (admin)
                                                   WS   /ws?token=...  (live updates, filtered by role)

The pages are a React app (frontend/), built to static files and served by this service.
Authentication is mocked (see auth.py). Admins see everything; nurses only see
and get notified about the devices assigned to them.

Isolation commands go to the compacted Kafka topic `iomt.actions`. The gateway reads
it to refuse isolated devices, and this service reads it back on startup so the
dashboard and the gateway always agree about who is isolated.

Persistence: when DATABASE_URL is set, device state, alerts (with who acknowledged
them) and the audit log are saved to Postgres (store.py) and reloaded on startup, and
the scored topic is read with a consumer group so a restart resumes where it stopped.
Isolation is not stored in Postgres: the actions topic is its single source of truth.
Without DATABASE_URL the service runs purely in memory and replays the topic on start.
"""
import asyncio
import csv
import io
from datetime import datetime
import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field

import auth
import store as store_module

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SCORED = os.getenv("TOPIC_TELEMETRY_SCORED", "iomt.telemetry.scored")
# Risk score (0-100) at or above which a device raises a high-risk alert.
TOPIC_ACTIONS = os.getenv("TOPIC_ACTIONS", "iomt.actions")
# Remediation recheck: after someone marks an alert fixed, the device must send this many clean messages, spread over
# at least this many seconds, before the fix counts as verified. Any message that still shows the problem reopens it.
RECHECK_CLEAN_MESSAGES = int(os.getenv("RECHECK_CLEAN_MESSAGES", "5"))
RECHECK_MIN_SECONDS = int(os.getenv("RECHECK_MIN_SECONDS", "30"))
RISK_ALERT_THRESHOLD = int(os.getenv("RISK_ALERT_THRESHOLD", "70"))
# Risk history: each device keeps the highest score per HISTORY_BUCKET_S slot for HISTORY_HOURS, so the
# dashboard can draw risk-over-time charts that survive a restart (stored in Postgres when it is on).
HISTORY_BUCKET_S = 10
HISTORY_HOURS = int(os.getenv("HISTORY_HOURS", "24"))
MAX_ALERTS = 200
DATABASE_URL = os.getenv("DATABASE_URL", "")
CONSUMER_GROUP = os.getenv("DASHBOARD_CONSUMER_GROUP", "medguard-dashboard")

VITAL_FIELDS = [
    "infusion_rate_ml_per_hr", "occlusion_pressure_mmHg", "battery_pct",
    "heart_rate_bpm", "pulse_bpm", "spo2_pct", "systolic_bp_mmHg", "diastolic_bp_mmHg",
]
# Flags that are clinically urgent rather than hygiene issues.
HIGH_SEVERITY_REASONS = {"infusion_rate_mismatch", "high_risk_score"}

# All of this is touched only from the event loop, so no locks are needed.
devices: dict[str, dict] = {}
alerts: deque = deque(maxlen=MAX_ALERTS)
clients: dict[WebSocket, dict] = {}   # websocket -> signed-in user
_signatures: dict[str, str] = {}   # last alert reason set per device, to avoid repeats
_alert_seq = 0
isolated: dict[str, bool] = {}     # device_id -> isolated, rebuilt from the actions topic
kafka_connected = False
history: dict[str, dict[int, float]] = {}   # device_id -> {bucket start (epoch s) -> highest score in it}
db: store_module.Store | None = None   # set in the lifespan when DATABASE_URL is configured


async def persist(method: str, *args) -> None:
    """Run one database write off the event loop. A database problem never stops live monitoring."""
    if db is None or not db.ready:
        return
    try:
        await asyncio.to_thread(getattr(db, method), *args)
    except Exception as exc:
        db.last_error = str(exc)
        print(f"dashboard-api: database write failed ({exc})")


def signature_for(state: dict) -> str:
    reasons = list(state["flags"])
    if state["risk_score"] >= RISK_ALERT_THRESHOLD:
        reasons.append("high_risk_score")
    return "|".join(sorted(reasons))


def restore_from_db() -> None:
    """Load saved state so a restart does not lose alerts, acknowledgments or the audit trail."""
    global _alert_seq
    data = db.load(MAX_ALERTS, auth.audit.maxlen)
    for d in data["devices"]:
        d["isolated"] = isolated.get(d["device_id"], False)
        devices[d["device_id"]] = d
        _signatures[d["device_id"]] = signature_for(d)   # so a restart does not re-alert on unchanged problems
    for device_id, bucket, score in db.load_history(int(time.time()) - HISTORY_HOURS * 3600):
        history.setdefault(device_id, {})[bucket] = score
    alerts.clear()
    alerts.extend(data["alerts"])                          # newest first, as stored
    for a in alerts:
        remediation_of(a)
    _alert_seq = max(_alert_seq, data["max_alert_id"])
    auth.restore_audit(data["audit"], data["max_audit_id"])
    print(f"dashboard-api: restored {len(devices)} devices, {len(alerts)} alerts, {len(data['audit'])} audit entries")


def summarize(rec: dict) -> dict:
    """Pick out what the dashboard needs from one scored message."""
    return {
        "device_id": rec.get("device_id"),
        "device_type": rec.get("device_type"),
        "patient_id": rec.get("patient_id"),
        "unit": rec.get("unit"),
        "risk_score": rec.get("risk_score", 0),
        "risk_reasons": rec.get("risk_reasons", []),   # which readings made the model score this high
        "model_version": rec.get("model_version"),
        "flags": rec.get("vulnerability_flags", []),
        "tips": rec.get("remediation_tips", []),
        "last_seen": rec.get("ingested_at") or rec.get("timestamp"),
        "request_rate_per_min": rec.get("request_rate_per_min"),
        "packet_size_bytes": rec.get("packet_size_bytes"),
        "vitals": {k: rec[k] for k in VITAL_FIELDS if k in rec},
    }


def _epoch(iso) -> float | None:
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def record_history(device_id: str, last_seen, score: float) -> tuple[int, float] | None:
    """Fold one score into the device's history. Returns (bucket, score) when the stored value changed, else None.

    Slots come from the message's own timestamp, so replayed old messages land where they belong.
    """
    t = _epoch(last_seen)
    if t is None:
        return None
    bucket = int(t // HISTORY_BUCKET_S) * HISTORY_BUCKET_S
    slots = history.setdefault(device_id, {})
    if bucket in slots and slots[bucket] >= score:
        return None
    slots[bucket] = score
    cutoff = bucket - HISTORY_HOURS * 3600
    for old in [b for b in slots if b < cutoff]:
        del slots[old]
    return bucket, score


def history_points(device_id: str, minutes: int) -> dict:
    """Points for one device over the last `minutes`, thinned to at most ~120 by keeping the highest score per slot."""
    now = time.time()
    start = now - minutes * 60
    width = max(HISTORY_BUCKET_S, (minutes * 60 // 120 // HISTORY_BUCKET_S) * HISTORY_BUCKET_S)
    merged: dict[int, float] = {}
    for bucket, score in history.get(device_id, {}).items():
        if bucket >= start - HISTORY_BUCKET_S:
            slot = int(bucket // width) * width
            merged[slot] = max(merged.get(slot, 0), score)
    return {"device_id": device_id, "minutes": minutes, "bucket_s": width, "from": int(start), "to": int(now),
            "points": [{"t": b, "score": merged[b]} for b in sorted(merged)]}


def new_remediation() -> dict:
    """Lifecycle of the problem behind an alert: open -> fixed (waiting for the recheck) -> verified, or back to reopened."""
    return {"status": "open", "fixed": None, "verified_at": None, "clean": 0, "reopens": 0}


def remediation_of(alert: dict) -> dict:
    """The alert's remediation record, filled in for alerts saved before this feature existed."""
    rem = alert.setdefault("remediation", {})
    for k, v in new_remediation().items():
        rem.setdefault(k, v)
    return rem


def reasons_of(state: dict) -> set[str]:
    return set(signature_for(state).split("|")) - {""}


async def recheck(state: dict) -> list[dict]:
    """Test every alert of this device that is marked fixed against the newest message. Returns the alerts it reopened."""
    reopened = []
    t = _epoch(state["last_seen"])
    if t is None:
        return reopened
    for alert in alerts:
        rem = remediation_of(alert)
        if alert["device_id"] != state["device_id"] or rem["status"] != "fixed":
            continue
        fixed_at = _epoch(rem["fixed"]["at"])
        if fixed_at is None or t <= fixed_at:
            continue                                   # a message from before the fix says nothing about it
        still = sorted(set(alert["reasons"]) & reasons_of(state))
        if still:
            rem["status"], rem["clean"] = "reopened", 0
            rem["reopens"] += 1
            reopened.append(alert)
            await persist("save_remediation", alert["id"], rem)
            await broadcast({"type": "alert_update", "alert": alert, "reopened": True}, device_id=alert["device_id"])
            await log("system", "system", "fix_failed", alert["device_id"],
                      f'alert #{alert["id"]}: problem still present after the fix ({", ".join(still)}); reopened, '
                      f'notified {", ".join(auth.nurses_for(alert["device_id"])) or "no assigned nurse"}')
            continue
        rem["clean"] += 1
        if rem["clean"] >= RECHECK_CLEAN_MESSAGES and t - fixed_at >= RECHECK_MIN_SECONDS:
            rem["status"], rem["verified_at"] = "verified", state["last_seen"]
            await persist("save_remediation", alert["id"], rem)
            await broadcast({"type": "alert_update", "alert": alert}, device_id=alert["device_id"])
            await log("system", "system", "fix_verified", alert["device_id"],
                      f'alert #{alert["id"]}: {rem["clean"]} clean messages over {int(t - fixed_at)} s after the fix')
        else:
            await persist("save_remediation", alert["id"], rem)
            await broadcast({"type": "alert_update", "alert": alert}, device_id=alert["device_id"])
    return reopened


def maybe_alert(state: dict) -> dict | None:
    """Create an alert when a device's set of problems changes.

    A device that keeps reporting the same flags does not create a new alert
    on every message. If its problems clear and later return, it alerts again.
    """
    global _alert_seq
    reasons = list(state["flags"])
    if state["risk_score"] >= RISK_ALERT_THRESHOLD:
        reasons.append("high_risk_score")
    signature = "|".join(sorted(reasons))
    device_id = state["device_id"]
    if signature == _signatures.get(device_id, ""):
        return None
    _signatures[device_id] = signature
    if not reasons:
        return None
    _alert_seq += 1
    return {
        "id": _alert_seq,
        "device_id": device_id,
        "device_type": state["device_type"],
        "unit": state["unit"],
        "reasons": reasons,
        "severity": "high" if HIGH_SEVERITY_REASONS & set(reasons) else "medium",
        "risk_score": state["risk_score"],
        "explanations": state["risk_reasons"] if state["risk_score"] >= RISK_ALERT_THRESHOLD else [],
        "tips": state["tips"],
        "at": state["last_seen"] or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ack": None,   # becomes {"by": username, "at": time} when acknowledged
        "remediation": new_remediation(),
    }


async def broadcast(event: dict, device_id: str | None = None, admin_only: bool = False) -> None:
    """Send an event to every connected user who is allowed to see it."""
    dead = []
    for ws, user in list(clients.items()):
        if admin_only and user["role"] != "admin":
            continue
        if device_id is not None and not auth.can_see(user, device_id):
            continue
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)


async def log(user: str, role: str, action: str, target: str = "", detail: str = "") -> None:
    """Write an audit entry and push it to connected admins."""
    entry = auth.record(user, role, action, target, detail)
    await persist("save_audit", entry)
    await broadcast({"type": "audit", "entry": entry}, admin_only=True)


async def process(rec: dict) -> None:
    """Apply one scored message to the in-memory state and tell the browsers."""
    if not rec.get("device_id"):
        return
    state = summarize(rec)
    state["isolated"] = isolated.get(state["device_id"], False)
    devices[state["device_id"]] = state
    await persist("save_device", state)
    changed = record_history(state["device_id"], state["last_seen"], state["risk_score"])
    if changed:
        await persist("save_history", state["device_id"], *changed)
    await broadcast({"type": "device", "device": state}, device_id=state["device_id"])
    if await recheck(state):                 # a failed fix reopens its alert instead of raising a second one
        _signatures[state["device_id"]] = signature_for(state)
        alert = None
    else:
        alert = maybe_alert(state)
    if alert:
        alerts.appendleft(alert)
        await persist("save_alert", alert)
        await broadcast({"type": "alert", "alert": alert}, device_id=state["device_id"])
        notified = auth.nurses_for(state["device_id"])
        await log("system", "system", "alert_raised", state["device_id"],
                  f'{alert["severity"]}: {", ".join(alert["reasons"])}; '
                  f'notified {", ".join(notified) if notified else "no assigned nurse"}')


async def apply_action(event: dict) -> None:
    """Apply one isolate/restore command (read back from the actions topic) and tell the browsers."""
    device_id, flag = event.get("device_id"), event.get("isolated")
    if not isinstance(device_id, str) or not device_id or not isinstance(flag, bool):
        return
    isolated[device_id] = flag
    device = devices.get(device_id)
    if device is not None:
        device["isolated"] = flag
        await broadcast({"type": "device", "device": device}, device_id=device_id)


_producer: KafkaProducer | None = None


def publish_action(event: dict) -> None:
    """Send one command to the actions topic and wait for Kafka to confirm. Raises if it cannot."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    _producer.send(TOPIC_ACTIONS, key=event["device_id"], value=event).get(timeout=5)


async def send_action(event: dict) -> None:
    """Publish a command. The dashboard state changes when the command is read back from the topic
    (or immediately when Kafka is disabled in tests), so the UI only ever shows what the gateway will enforce."""
    if os.getenv("DISABLE_KAFKA") == "1":
        await apply_action(event)
    else:
        await asyncio.to_thread(publish_action, event)


def consume_actions(loop: asyncio.AbstractEventLoop) -> None:
    """Background thread: replay the actions topic from the start, then follow it."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_ACTIONS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None,
            )
            print(f"dashboard-api: reading '{TOPIC_ACTIONS}' from {KAFKA_BOOTSTRAP}")
            for message in consumer:
                asyncio.run_coroutine_threadsafe(apply_action(message.value), loop)
        except Exception as exc:
            print(f"dashboard-api: actions topic unavailable ({exc}); retrying in 3s")
            time.sleep(3)


def consume_forever(loop: asyncio.AbstractEventLoop) -> None:
    """Background thread: read the scored topic and hand each message to the loop.

    Without a database there is no consumer group: every start replays the topic from the
    beginning to rebuild the in-memory state. With a database it uses a consumer group and
    resumes from the last committed offset. Retries until Kafka is reachable.
    """
    global kafka_connected
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_SCORED,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                # With a database, remember our place so a restart resumes instead of replaying.
                enable_auto_commit=db is not None,
                group_id=CONSUMER_GROUP if db is not None else None,
            )
            kafka_connected = True
            print(f"dashboard-api: reading '{TOPIC_SCORED}' from {KAFKA_BOOTSTRAP}")
            for message in consumer:
                asyncio.run_coroutine_threadsafe(process(message.value), loop)
        except Exception as exc:
            kafka_connected = False
            print(f"dashboard-api: Kafka unavailable ({exc}); retrying in 3s")
            time.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    if DATABASE_URL:
        candidate = store_module.Store(DATABASE_URL)
        if await asyncio.to_thread(candidate.connect, int(os.getenv("DB_CONNECT_ATTEMPTS", "15"))):
            db = candidate
            await asyncio.to_thread(restore_from_db)
            await asyncio.to_thread(db.prune_history, int(time.time()) - HISTORY_HOURS * 3600)
        else:
            print("dashboard-api: could not reach the database; running in memory only")
    if os.getenv("DISABLE_KAFKA") != "1":
        loop = asyncio.get_running_loop()
        threading.Thread(target=consume_forever, args=(loop,), daemon=True).start()
        threading.Thread(target=consume_actions, args=(loop,), daemon=True).start()
    yield
    if db is not None:
        db.close()


# Interactive docs and the schema are for development only (ENABLE_DOCS=1); they are not shown to anonymous visitors.
_docs = os.getenv("ENABLE_DOCS") == "1"
app = FastAPI(title="MedGuard Dashboard API", lifespan=lifespan,
              docs_url="/docs" if _docs else None, redoc_url=None, openapi_url="/openapi.json" if _docs else None)

CSP = ("default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
       "connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    """Browser-side protections: no inline scripts (XSS), no framing (clickjacking), no MIME sniffing, no caching of data."""
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = CSP
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response
# The React app is built into WEB_DIR: /srv/web inside the Docker image, or frontend/dist when
# running locally after `npm run build`. It lives outside /app so the dev volume mount cannot hide it.
WEB_DIR = Path(os.getenv("WEB_DIR") or ("/srv/web" if Path("/srv/web").is_dir() else Path(__file__).parent / "frontend" / "dist"))


def sorted_devices(user: dict) -> list[dict]:
    visible = [d for d in devices.values() if auth.can_see(user, d["device_id"])]
    return sorted(visible, key=lambda d: (-d["risk_score"], d["device_id"]))


def visible_alerts(user: dict) -> list[dict]:
    return [a for a in alerts if auth.can_see(user, a["device_id"])]


# ---------- pages (single-page React app) ----------
if (WEB_DIR / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=WEB_DIR / "assets"), name="assets")


def spa():
    index = WEB_DIR / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=503, detail="Frontend not built. Run `npm run build` in dashboard-api/frontend.")
    return FileResponse(index)


@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse("/login")


for _route in ("/login", "/admin", "/nurse"):
    app.add_api_route(_route, spa, methods=["GET"], include_in_schema=False)


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


# ---------- auth ----------
class LoginBody(BaseModel):
    username: str = Field(max_length=64)
    password: str = Field(max_length=128)


@app.post("/api/login")
async def login(body: LoginBody, request: Request):
    username = body.username.strip().lower()
    client = request.client.host if request.client else "unknown"
    if auth.login_blocked(client, username):
        raise HTTPException(status_code=429, detail="Too many failed sign-in attempts. Try again in a few minutes.",
                            headers={"Retry-After": str(auth.LOGIN_WINDOW_S)})
    if not auth.check_password(username, body.password):
        locked = auth.login_failed(client, username)
        await log(username[:40] or "?", "-", "login_failed", "", "wrong username or password")
        if locked:   # one audit line when a lockout starts, instead of one per blocked attempt
            await log(username[:40] or "?", "-", "login_locked", "", f"sign-in blocked for {auth.LOGIN_WINDOW_S // 60} min after repeated failures from {client}")
        raise HTTPException(status_code=401, detail="Wrong username or password")
    auth.login_succeeded(client, username)
    token = auth.new_session(username)
    user = auth.public(username)
    await log(username, user["role"], "login")
    return {"token": token, "user": user}


@app.post("/api/logout")
async def logout(user: dict = Depends(auth.current_user)):
    auth.sessions.pop(user["token"], None)
    await log(user["username"], user["role"], "logout")
    return {"ok": True}


@app.get("/api/me")
def me(user: dict = Depends(auth.current_user)):
    return {k: v for k, v in user.items() if k != "token"}


# ---------- data ----------
@app.get("/health")
def health():
    return {"status": "dashboard-api is up", "kafka_connected": kafka_connected, "devices": len(devices),
            "persistence": ("postgres" if db is not None and db.ready else "memory"),
            "database_error": db.last_error if db is not None else None}


@app.get("/api/devices")
def get_devices(user: dict = Depends(auth.current_user)):
    return sorted_devices(user)


@app.get("/api/alerts")
def get_alerts(user: dict = Depends(auth.current_user)):
    return visible_alerts(user)


@app.post("/api/alerts/{alert_id}/ack")
async def ack_alert(alert_id: int, user: dict = Depends(auth.current_user)):
    alert = next((a for a in alerts if a["id"] == alert_id and auth.can_see(user, a["device_id"])), None)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    if not alert["ack"]:
        alert["ack"] = {"by": user["username"], "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        await persist("ack_alert", alert_id, alert["ack"]["by"], alert["ack"]["at"])
        await broadcast({"type": "alert_update", "alert": alert}, device_id=alert["device_id"])
        await log(user["username"], user["role"], "alert_ack", alert["device_id"], f"alert #{alert_id}")
    return alert


class FixBody(BaseModel):
    note: str = Field(min_length=3, max_length=500)


@app.post("/api/alerts/{alert_id}/fix")
async def mark_fixed(alert_id: int, body: FixBody, user: dict = Depends(auth.current_user)):
    """Say what was done about an alert. The system then rechecks the device's next messages before calling it verified."""
    alert = next((a for a in alerts if a["id"] == alert_id and auth.can_see(user, a["device_id"])), None)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    rem = remediation_of(alert)
    if rem["status"] not in ("open", "reopened"):
        raise HTTPException(status_code=409, detail=f"Alert is already {rem['status']}")
    rem["status"], rem["clean"] = "fixed", 0
    rem["fixed"] = {"by": user["username"], "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "note": body.note.strip()}
    if not alert["ack"]:
        alert["ack"] = {"by": user["username"], "at": rem["fixed"]["at"]}
        await persist("ack_alert", alert_id, alert["ack"]["by"], alert["ack"]["at"])
    await persist("save_remediation", alert_id, rem)
    await broadcast({"type": "alert_update", "alert": alert}, device_id=alert["device_id"])
    await log(user["username"], user["role"], "alert_fixed", alert["device_id"], f"alert #{alert_id}: {rem['fixed']['note']}")
    return alert


async def set_isolation(device_id: str, flag: bool, user: dict) -> dict:
    device = devices.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Unknown device")
    event = {"device_id": device_id, "isolated": flag, "by": user["username"],
             "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        await send_action(event)
    except Exception as exc:   # do not claim a device is isolated if the gateway could not be told
        await log(user["username"], user["role"], "isolation_failed", device_id, f"could not publish command: {exc}")
        raise HTTPException(status_code=503, detail="Could not send the command to the gateway. Nothing was changed.")
    await log(user["username"], user["role"], "device_isolated" if flag else "device_restored", device_id,
              "command published; the gateway now refuses this device's messages" if flag
              else "command published; the gateway accepts this device again")
    return {**device, "isolated": flag}


@app.post("/api/devices/{device_id}/isolate")
async def isolate(device_id: str, user: dict = Depends(auth.require_admin)):
    return await set_isolation(device_id, True, user)


@app.post("/api/devices/{device_id}/restore")
async def restore(device_id: str, user: dict = Depends(auth.require_admin)):
    return await set_isolation(device_id, False, user)


@app.get("/api/audit")
def get_audit(user: dict = Depends(auth.require_admin)):
    return list(auth.audit)


# ---------- exports ----------
def _cell(v) -> str:
    """One CSV cell. Text starting with = + - @ would run as a formula when opened in Excel, so it is prefixed with '."""
    s = "" if v is None else (json.dumps(v) if isinstance(v, (list, dict)) else str(v))
    return ("'" + s) if s[:1] in ("=", "+", "-", "@", "\t", "\r") else s


def export_response(rows: list[dict], columns: list[str], fmt: str, name: str) -> Response:
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="format must be csv or json")
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    headers = {"Content-Disposition": f'attachment; filename="{name}-{stamp}.{fmt}"', "Cache-Control": "no-store"}
    if fmt == "json":
        return Response(json.dumps(rows, indent=2), media_type="application/json", headers=headers)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(columns)
    for r in rows:
        w.writerow([_cell(r.get(c)) for c in columns])
    return Response("\ufeff" + out.getvalue(), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/api/audit/export")
async def export_audit(format: str = "csv", action: str = "", user_filter: str = "", q: str = "", since: str = "",
                       user: dict = Depends(auth.require_admin)):
    """Admin only. Exports the full stored audit log (not just the latest entries), oldest first, and audits the export."""
    if db is not None and db.ready:
        rows = await asyncio.to_thread(db.audit_rows, action, user_filter, q, since)
    else:
        ql = q.lower()
        rows = [e for e in reversed(auth.audit)
                if (not action or e["action"] == action) and (not user_filter or e["user"] == user_filter)
                and (not ql or ql in f'{e["target"]} {e["detail"]} {e["user"]}'.lower()) and (not since or e["at"] >= since)]
    resp = export_response(rows, ["id", "at", "user", "role", "action", "target", "detail"], format, "medguard-audit")
    await log(user["username"], user["role"], "audit_exported", "audit_log", f"{len(rows)} entries as {format}")
    return resp


@app.get("/api/alerts/export")
async def export_alerts(format: str = "csv", user: dict = Depends(auth.current_user)):
    """Alerts the signed-in user is allowed to see (a nurse gets only their devices), newest first."""
    rows = [{**a, "acked_by": (a["ack"] or {}).get("by"), "acked_at": (a["ack"] or {}).get("at"),
             "reasons": "; ".join(a["reasons"]), "explanations": "; ".join(a.get("explanations") or []),
             "status": remediation_of(a)["status"], "fixed_by": (remediation_of(a)["fixed"] or {}).get("by"),
             "fixed_at": (remediation_of(a)["fixed"] or {}).get("at"), "fix_note": (remediation_of(a)["fixed"] or {}).get("note"),
             "verified_at": remediation_of(a)["verified_at"], "reopens": remediation_of(a)["reopens"]}
            for a in visible_alerts(user)]
    resp = export_response(rows, ["id", "at", "device_id", "device_type", "unit", "severity", "risk_score", "reasons",
                                  "explanations", "acked_by", "acked_at", "status", "fixed_by", "fixed_at", "fix_note", "verified_at", "reopens"],
                           format, "medguard-alerts")
    await log(user["username"], user["role"], "alerts_exported", "alerts", f"{len(rows)} alerts as {format}")
    return resp


@app.get("/api/devices/{device_id}/history")
async def device_history(device_id: str, minutes: int = 60, format: str = "json", user: dict = Depends(auth.current_user)):
    """Risk score over time for one device. `minutes` is 5 to the retention limit; format=csv downloads it."""
    if device_id not in devices or not auth.can_see(user, device_id):
        raise HTTPException(status_code=404, detail="Unknown device")
    minutes = max(5, min(minutes, HISTORY_HOURS * 60))
    data = history_points(device_id, minutes)
    if format == "json":
        return data
    rows = [{"device_id": device_id, "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p["t"])), "max_risk_score": p["score"]}
            for p in data["points"]]
    resp = export_response(rows, ["device_id", "time", "max_risk_score"], format, f"medguard-risk-{device_id}")
    await log(user["username"], user["role"], "history_exported", device_id, f"{len(rows)} points, last {minutes} min, as {format}")
    return resp


@app.get("/api/users")
def get_users(user: dict = Depends(auth.require_admin)):
    online = {u["username"] for u in clients.values()}
    return [{**auth.public(n), "online": n in online} for n in auth.USERS]


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, token: str = ""):
    user = auth.user_for_token(token)
    if not user:
        await ws.close(code=4401)
        return
    await ws.accept()
    clients[ws] = user
    try:
        await ws.send_json({"type": "snapshot", "devices": sorted_devices(user), "alerts": visible_alerts(user)})
        while True:
            await ws.receive_text()   # we only push; this detects the disconnect
    except WebSocketDisconnect:
        pass
    finally:
        clients.pop(ws, None)