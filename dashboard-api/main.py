"""
MedGuard dashboard API.

Reads scored telemetry from Kafka, keeps the latest state of every device in
memory, derives alerts, and pushes both to browsers over a WebSocket.

    iomt.telemetry.scored  ->  [this service]  ->  /login, /admin, /nurse (pages)
                                                   POST /api/login, /api/logout, GET /api/me
                                                   GET  /api/devices, /api/alerts  (filtered by role)
                                                   POST /api/alerts/{id}/ack
                                                   POST /api/devices/{id}/isolate | /restore  (admin)
                                                   GET  /api/audit, /api/users                (admin)
                                                   WS   /ws?token=...  (live updates, filtered by role)

The pages are a React app (frontend/), built to static files and served by this service.
Authentication is mocked (see auth.py). Admins see everything; nurses only see
and get notified about the devices assigned to them.

State is in memory on purpose for now: on startup the service replays the
topic from the beginning, which rebuilds the latest state per device.
Postgres (acknowledgments, isolation state, audit log) comes later.
"""
import asyncio
import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer
from pydantic import BaseModel

import auth

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SCORED = os.getenv("TOPIC_TELEMETRY_SCORED", "iomt.telemetry.scored")
# Risk score (0-100) at or above which a device raises a high-risk alert.
RISK_ALERT_THRESHOLD = int(os.getenv("RISK_ALERT_THRESHOLD", "70"))
MAX_ALERTS = 200

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
kafka_connected = False


def summarize(rec: dict) -> dict:
    """Pick out what the dashboard needs from one scored message."""
    return {
        "device_id": rec.get("device_id"),
        "device_type": rec.get("device_type"),
        "patient_id": rec.get("patient_id"),
        "unit": rec.get("unit"),
        "risk_score": rec.get("risk_score", 0),
        "model_version": rec.get("model_version"),
        "flags": rec.get("vulnerability_flags", []),
        "tips": rec.get("remediation_tips", []),
        "last_seen": rec.get("ingested_at") or rec.get("timestamp"),
        "request_rate_per_min": rec.get("request_rate_per_min"),
        "packet_size_bytes": rec.get("packet_size_bytes"),
        "vitals": {k: rec[k] for k in VITAL_FIELDS if k in rec},
    }


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
        "tips": state["tips"],
        "at": state["last_seen"] or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ack": None,   # becomes {"by": username, "at": time} when acknowledged
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
    await broadcast({"type": "audit", "entry": entry}, admin_only=True)


async def process(rec: dict) -> None:
    """Apply one scored message to the in-memory state and tell the browsers."""
    if not rec.get("device_id"):
        return
    state = summarize(rec)
    state["isolated"] = devices.get(state["device_id"], {}).get("isolated", False)
    devices[state["device_id"]] = state
    await broadcast({"type": "device", "device": state}, device_id=state["device_id"])
    alert = maybe_alert(state)
    if alert:
        alerts.appendleft(alert)
        await broadcast({"type": "alert", "alert": alert}, device_id=state["device_id"])
        notified = auth.nurses_for(state["device_id"])
        await log("system", "system", "alert_raised", state["device_id"],
                  f'{alert["severity"]}: {", ".join(alert["reasons"])}; '
                  f'notified {", ".join(notified) if notified else "no assigned nurse"}')


def consume_forever(loop: asyncio.AbstractEventLoop) -> None:
    """Background thread: read the scored topic and hand each message to the loop.

    No consumer group: every start replays the topic from the beginning so the
    in-memory state is rebuilt. Retries until Kafka is reachable.
    """
    global kafka_connected
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_SCORED,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None,
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
    if os.getenv("DISABLE_KAFKA") != "1":
        loop = asyncio.get_running_loop()
        threading.Thread(target=consume_forever, args=(loop,), daemon=True).start()
    yield


app = FastAPI(title="MedGuard Dashboard API", lifespan=lifespan)
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
    username: str
    password: str


@app.post("/api/login")
async def login(body: LoginBody):
    username = body.username.strip().lower()
    if not auth.check_password(username, body.password):
        await log(username[:40] or "?", "-", "login_failed", "", "wrong username or password")
        raise HTTPException(status_code=401, detail="Wrong username or password")
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
    return {"status": "dashboard-api is up", "kafka_connected": kafka_connected, "devices": len(devices)}


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
        await broadcast({"type": "alert_update", "alert": alert}, device_id=alert["device_id"])
        await log(user["username"], user["role"], "alert_ack", alert["device_id"], f"alert #{alert_id}")
    return alert


async def set_isolation(device_id: str, isolated: bool, user: dict) -> dict:
    device = devices.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Unknown device")
    device["isolated"] = isolated
    await broadcast({"type": "device", "device": device}, device_id=device_id)
    await log(user["username"], user["role"], "device_isolated" if isolated else "device_restored", device_id,
              "recorded in the dashboard; gateway enforcement is not connected yet")
    return device


@app.post("/api/devices/{device_id}/isolate")
async def isolate(device_id: str, user: dict = Depends(auth.require_admin)):
    return await set_isolation(device_id, True, user)


@app.post("/api/devices/{device_id}/restore")
async def restore(device_id: str, user: dict = Depends(auth.require_admin)):
    return await set_isolation(device_id, False, user)


@app.get("/api/audit")
def get_audit(user: dict = Depends(auth.require_admin)):
    return list(auth.audit)


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
