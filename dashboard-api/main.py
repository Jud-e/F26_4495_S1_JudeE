"""
MedGuard dashboard API.

Reads scored telemetry from Kafka, keeps the latest state of every device in
memory, derives alerts, and pushes both to browsers over a WebSocket.

    iomt.telemetry.scored  ->  [this service]  ->  GET /api/devices
                                                   GET /api/alerts
                                                   WS  /ws (live updates)
                                                   GET /    (minimal live page)

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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from kafka import KafkaConsumer

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
clients: set[WebSocket] = set()
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
    }


async def broadcast(event: dict) -> None:
    dead = []
    for ws in clients:
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def process(rec: dict) -> None:
    """Apply one scored message to the in-memory state and tell the browsers."""
    if not rec.get("device_id"):
        return
    state = summarize(rec)
    devices[state["device_id"]] = state
    await broadcast({"type": "device", "device": state})
    alert = maybe_alert(state)
    if alert:
        alerts.appendleft(alert)
        await broadcast({"type": "alert", "alert": alert})


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
STATIC = Path(__file__).parent / "static"


def sorted_devices() -> list[dict]:
    return sorted(devices.values(), key=lambda d: (-d["risk_score"], d["device_id"]))


@app.get("/health")
def health():
    return {"status": "dashboard-api is up", "kafka_connected": kafka_connected, "devices": len(devices)}


@app.get("/api/devices")
def get_devices():
    return sorted_devices()


@app.get("/api/alerts")
def get_alerts():
    return list(alerts)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_json({"type": "snapshot", "devices": sorted_devices(), "alerts": list(alerts)})
        while True:
            await ws.receive_text()   # we only push; this detects the disconnect
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")