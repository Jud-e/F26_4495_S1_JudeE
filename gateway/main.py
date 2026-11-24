"""
MedGuard Zero-Trust API Gateway.

Two layers of defense before anything reaches Kafka:
1. verify_token (auth.py)  — rejects requests without a valid bearer token (401)
2. TelemetryPayload (schemas.py) — rejects structurally invalid/tampered
   payloads via Pydantic, enforced automatically by FastAPI (422)

A third gate runs after those two: a device an administrator has isolated is
refused with 403 (see isolation.py), and while the isolation list is still
loading after a restart the gateway answers 503.

Only messages that pass all of these reach publish() -> Kafka. Two independent
flag checks then run against the now-trusted payload — device config
vulnerabilities and patient prescription mismatches — and get attached
to the message for downstream (nurse-channel) consumption.
"""
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

import isolation
import limits
from auth import verify_token
from kafka_producer import publish
from patient_registry import get_patient
from prescription_checks import PRESCRIPTION_REMEDIATION_TIPS, check_prescription_mismatch
from schemas import TelemetryPayload
from vulnerability_rules import REMEDIATION_TIPS, check_config_vulnerabilities

@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("DISABLE_KAFKA") == "1":          # tests: no broker, nothing to wait for
        isolation.registry.mark_ready()
    else:
        threading.Thread(target=isolation.run_forever, daemon=True).start()
    yield


# The interactive docs and schema are for development only; they hand an attacker the exact payload shapes.
_docs = os.getenv("ENABLE_DOCS") == "1"
app = FastAPI(title="MedGuard Zero-Trust Gateway", lifespan=lifespan,
              docs_url="/docs" if _docs else None, redoc_url=None, openapi_url="/openapi.json" if _docs else None)

TOPIC_RAW = os.getenv("TOPIC_TELEMETRY_RAW", "iomt.telemetry.raw")
TOPIC_REJECTED = os.getenv("TOPIC_TELEMETRY_REJECTED", "iomt.telemetry.rejected")   # dead-letter topic
MAX_BODY_BYTES = int(os.getenv("MAX_BODY_BYTES", "16384"))
MAX_CLOCK_SKEW_S = int(os.getenv("MAX_CLOCK_SKEW_S", "300"))


def reset_state() -> None:
    limits.reset()


@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    """Refuse oversized or unsized request bodies before anything reads them into memory."""
    if request.method in ("POST", "PUT", "PATCH"):
        length = request.headers.get("content-length")
        if length is None:
            return JSONResponse(status_code=411, content={"detail": "Content-Length required"})
        if not length.isdigit() or int(length) > MAX_BODY_BYTES:
            return JSONResponse(status_code=413, content={"detail": f"Request body larger than {MAX_BODY_BYTES} bytes"})
    return await call_next(request)


def reject(request: Request, reason: str, detail, body: bytes = b"") -> None:
    """Dead-letter record for an authenticated message that was refused. Best effort: never breaks the request."""
    try:
        publish(TOPIC_REJECTED, key=None, value={
            "reason": reason, "detail": detail,
            "client": request.client.host if request.client else None,
            "at": datetime.now(timezone.utc).isoformat(),
            "payload_excerpt": body[:1024].decode("utf-8", "replace"),
        })
    except Exception as exc:
        print(f"dead-letter publish failed: {exc}")


@app.exception_handler(RequestValidationError)
async def validation_failed(request: Request, exc: RequestValidationError):
    errors = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
    if getattr(request.state, "authenticated", False):      # only credentialed senders can add to the dead-letter topic
        reject(request, "schema_validation", errors[:10], await request.body())
    return JSONResponse(status_code=422, content={"detail": errors})


@app.get("/health")
def health():
    reg = isolation.registry
    return {
        "status": "gateway is up",
        "isolation_ready": reg.ready,
        "isolated_devices": reg.count(),
        "blocked_messages": sum(reg.blocked.values()),
    }


@app.post("/ingest", status_code=status.HTTP_201_CREATED)
def ingest(request: Request, payload: TelemetryPayload, _authorized: bool = Depends(verify_token)):
    if not limits.device_rate.allow(payload.device_id):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded for this device",
                            headers={"Retry-After": "5"})
    reg = isolation.registry
    if not reg.ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Isolation list is still loading; retry shortly",
        )
    if reg.is_isolated(payload.device_id):
        reg.blocked[payload.device_id] += 1
        print(f"BLOCKED: {payload.device_id} is isolated (message {payload.message_id})")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Device is isolated by an administrator",
        )

    ts = payload.timestamp if payload.timestamp.tzinfo else payload.timestamp.replace(tzinfo=timezone.utc)
    if abs((datetime.now(timezone.utc) - ts).total_seconds()) > MAX_CLOCK_SKEW_S:
        reject(request, "stale_or_future_timestamp", str(payload.timestamp), payload.model_dump_json().encode())
        raise HTTPException(status_code=422, detail=f"timestamp must be within {MAX_CLOCK_SKEW_S} seconds of the gateway clock")
    replay_key = (payload.device_id, payload.message_id)
    if limits.replays.is_replay(replay_key):
        reject(request, "replayed_message_id", payload.message_id, b"")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Duplicate message_id (replay refused)")

    message = payload.model_dump(mode="json")

    all_flags = []
    all_tips = []

    config_flags = check_config_vulnerabilities(payload.config)
    if config_flags:
        all_flags.extend(config_flags)
        all_tips.extend(REMEDIATION_TIPS[f] for f in config_flags)

    patient = get_patient(payload.patient_id)
    prescription_flags = check_prescription_mismatch(patient, payload.device_type, message)
    if prescription_flags:
        all_flags.extend(prescription_flags)
        all_tips.extend(PRESCRIPTION_REMEDIATION_TIPS[f] for f in prescription_flags)

    if all_flags:
        message["vulnerability_flags"] = all_flags
        message["remediation_tips"] = all_tips

    message["ingested_at"] = datetime.now(timezone.utc).isoformat()

    try:
        publish(TOPIC_RAW, key=payload.device_id, value=message)
    except Exception as exc:        # log the real cause here; the caller only learns that it should retry
        print(f"ERROR: could not publish to Kafka: {exc}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline temporarily unavailable; retry shortly")
    limits.replays.remember(replay_key)

    return {
        "message_id": payload.message_id,
        "status": "accepted",
        "vulnerability_flags": all_flags,
    }