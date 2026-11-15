"""
Simulates IoMT device telemetry and POSTs it to the gateway's /ingest
endpoint — not directly to Kafka. This is deliberate: posting through the
gateway is what actually exercises the zero-trust auth + schema validation
layer, rather than bypassing it.

Run:
    python simulate_devices.py
    python simulate_devices.py --rate 2 --anomaly-chance 0.15 --count 200

Requires: pip install -r requirements.txt
Requires the gateway to be running (docker compose up -d) and reachable at
GATEWAY_URL (default http://localhost:8000).
"""
import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

GATEWAY_URL = os.getenv("GATEWAY_URL", "https://localhost:8000")
GATEWAY_API_TOKEN = os.getenv("GATEWAY_API_TOKEN", "dev-shared-secret-change-me")
GROUND_TRUTH_LOG = Path(__file__).parent / "ground_truth_log.csv"
PATIENTS_PATH = Path(__file__).parent.parent / "data" / "patients.json"

# A small fleet of simulated devices. `config` is each device's current
# reported security posture — some start deliberately vulnerable so the
# gateway's vulnerability_rules.py has something real to flag. A real
# device wouldn't announce its own config like this over the wire in
# practice it's polled/registered separately, but folding it into the
# telemetry payload keeps this project's scope manageable.
DEVICES = [
    {"device_id": "infusion-pump-001", "device_type": "infusion_pump", "unit": "ICU-3",
     "config": {"auto_lock_enabled": False, "default_credentials_changed": False,
                "firmware_up_to_date": True, "tls_enabled": True}},
    {"device_id": "infusion-pump-002", "device_type": "infusion_pump", "unit": "ICU-3",
     "config": {"auto_lock_enabled": True, "default_credentials_changed": True,
                "firmware_up_to_date": True, "tls_enabled": True}},
    {"device_id": "cardiac-monitor-001", "device_type": "cardiac_monitor", "unit": "ICU-1",
     "config": {"auto_lock_enabled": True, "default_credentials_changed": True,
                "firmware_up_to_date": True, "tls_enabled": False}},
    {"device_id": "cardiac-monitor-002", "device_type": "cardiac_monitor", "unit": "ICU-1",
     "config": {"auto_lock_enabled": True, "default_credentials_changed": True,
                "firmware_up_to_date": True, "tls_enabled": True}},
    {"device_id": "pulse-ox-001", "device_type": "pulse_oximeter", "unit": "WARD-2",
     "config": {"auto_lock_enabled": True, "default_credentials_changed": True,
                "firmware_up_to_date": False, "tls_enabled": True}},
]


def load_patient_map() -> dict:
    """device_id -> patient_id, from data/patients.json (see Option 2 in the
    project discussion: device readings now carry patient context so the
    gateway can cross-check against actual prescriptions, not just ranges)."""
    with open(PATIENTS_PATH) as f:
        data = json.load(f)
    return {p["assigned_device_id"]: p["patient_id"] for p in data["patients"]}


PATIENT_BY_DEVICE = load_patient_map()
for _device in DEVICES:
    _device["patient_id"] = PATIENT_BY_DEVICE[_device["device_id"]]


def normal_reading(device: dict) -> dict:
    """Plausible in-range reading for this device type."""
    if device["device_type"] == "infusion_pump":
        metrics = {
            "infusion_rate_ml_per_hr": round(random.uniform(20, 150), 1),
            "occlusion_pressure_mmHg": round(random.uniform(2, 8), 1),
            "battery_pct": random.randint(40, 100),
        }
    elif device["device_type"] == "cardiac_monitor":
        metrics = {
            "heart_rate_bpm": random.randint(60, 100),
            "spo2_pct": random.randint(95, 100),
            "systolic_bp_mmHg": random.randint(100, 130),
            "diastolic_bp_mmHg": random.randint(65, 85),
        }
    else:  # pulse_oximeter
        metrics = {
            "spo2_pct": random.randint(95, 100),
            "pulse_bpm": random.randint(60, 100),
        }

    return {
        "packet_size_bytes": random.randint(120, 400),
        "request_rate_per_min": round(random.uniform(1, 6), 1),
        **metrics,
    }


def anomalous_reading(device: dict):
    """
    Returns (metrics, attack_type, is_malformed).

    - out_of_range_vital / traffic_flood / prescription_mismatch: structurally
      valid but behaviorally/clinically anomalous -> should PASS the
      gateway's schema check and flow to Kafka (prescription_mismatch is
      additionally caught by the gateway's prescription check specifically).
    - payload_tamper: injects an undeclared field -> should be REJECTED by
      the gateway's extra="forbid" schema validation (422).
    """
    possible_attacks = ["out_of_range_vital", "traffic_flood", "payload_tamper"]
    if device["device_type"] == "infusion_pump":
        possible_attacks.append("prescription_mismatch")

    attack_type = random.choice(possible_attacks)
    base = normal_reading(device)
    is_malformed = False

    if attack_type == "out_of_range_vital":
        if device["device_type"] == "cardiac_monitor":
            base["heart_rate_bpm"] = random.choice([random.randint(0, 20), random.randint(220, 300)])
            base["spo2_pct"] = random.randint(40, 70)
        elif device["device_type"] == "pulse_oximeter":
            base["pulse_bpm"] = random.choice([random.randint(0, 20), random.randint(220, 300)])
            base["spo2_pct"] = random.randint(40, 70)
        else:  # infusion_pump
            base["occlusion_pressure_mmHg"] = round(random.uniform(50, 150), 1)

    elif attack_type == "traffic_flood":
        base["request_rate_per_min"] = round(random.uniform(200, 1000), 1)
        base["packet_size_bytes"] = random.randint(4000, 20000)

    elif attack_type == "prescription_mismatch":
        # Structurally valid rate, but deliberately far from what this
        # patient is actually prescribed (see gateway/prescription_checks.py)
        base["infusion_rate_ml_per_hr"] = round(random.uniform(250, 400), 1)

    else:  # payload_tamper
        base["_tampered_field"] = "unexpected_field_injected"
        is_malformed = True

    return base, attack_type, is_malformed


def build_message(device: dict, is_anomaly: bool):
    if is_anomaly:
        metrics, attack_type, is_malformed = anomalous_reading(device)
    else:
        metrics, attack_type, is_malformed = normal_reading(device), None, False

    message_id = str(uuid.uuid4())
    payload = {
        "message_id": message_id,
        "device_id": device["device_id"],
        "patient_id": device["patient_id"],
        "device_type": device["device_type"],
        "unit": device["unit"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": device["config"],
        **metrics,
    }
    ground_truth = {
        "message_id": message_id,
        "label": "anomalous" if is_anomaly else "normal",
        "attack_type": attack_type or "",
        "expected_rejected": is_malformed,
    }
    return payload, ground_truth


def log_ground_truth(row: dict):
    """
    Ground truth (label, attack type) is NOT sent to the gateway — a real
    device would never announce whether its own reading is malicious. It's
    logged locally instead, keyed by message_id, so David's evaluation step
    (Week 8) can join it against what the ML model actually predicted.
    """
    file_exists = GROUND_TRUTH_LOG.exists()
    with open(GROUND_TRUTH_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["message_id", "label", "attack_type", "expected_rejected"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def send_to_gateway(payload: dict):
    headers = {"Authorization": f"Bearer {GATEWAY_API_TOKEN}"}
    try:
        resp = requests.post(f"{GATEWAY_URL}/ingest", json=payload, headers=headers, timeout=5)
        body = resp.json() if resp.content else {}
        return resp.status_code, body
    except requests.exceptions.RequestException as e:
        return None, {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Simulate IoMT device telemetry through the gateway.")
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per second (default: 1.0)")
    parser.add_argument("--anomaly-chance", type=float, default=0.05, help="Probability a message is anomalous (default: 0.05)")
    parser.add_argument("--count", type=int, default=None, help="Stop after N messages (default: run until Ctrl+C)")
    args = parser.parse_args()

    print(f"Posting to {GATEWAY_URL}/ingest (anomaly chance: {args.anomaly_chance:.0%}). "
          f"Ground truth logged to {GROUND_TRUTH_LOG.name}. Ctrl+C to stop.\n")

    sent = 0
    try:
        while args.count is None or sent < args.count:
            device = random.choice(DEVICES)
            is_anomaly = random.random() < args.anomaly_chance
            payload, ground_truth = build_message(device, is_anomaly)

            status_code, resp_body = send_to_gateway(payload)
            log_ground_truth(ground_truth)
            sent += 1

            tag = "ANOMALY" if is_anomaly else "normal "
            if status_code == 201:
                outcome = "ACCEPTED"
            elif status_code in (401, 422):
                outcome = f"REJECTED({status_code})"
            elif status_code == 403:
                outcome = "BLOCKED(403)"      # the device was isolated by an administrator
            elif status_code is None:
                outcome = "CONN-ERROR"
            else:
                outcome = f"HTTP {status_code}"

            vuln = resp_body.get("vulnerability_flags") if isinstance(resp_body, dict) else None
            vuln_note = f" | vuln={vuln}" if vuln else ""
            attack_note = ground_truth["attack_type"] or "-"

            print(f"[{sent:>4}] {tag} | {outcome:<12} | {device['device_id']:<20} | {attack_note:<20}{vuln_note}")

            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print(f"\nStopped. Sent {sent} messages.")


if __name__ == "__main__":
    main()