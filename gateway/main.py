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

from fastapi import Depends, FastAPI, HTTPException, status

import isolation
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


app = FastAPI(title="MedGuard Zero-Trust Gateway", lifespan=lifespan)

TOPIC_RAW = os.getenv("TOPIC_TELEMETRY_RAW", "iomt.telemetry.raw")


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
def ingest(payload: TelemetryPayload, _authorized: bool = Depends(verify_token)):
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

    publish(TOPIC_RAW, key=payload.device_id, value=message)

    return {
        "message_id": payload.message_id,
        "status": "accepted",
        "vulnerability_flags": all_flags,
    }