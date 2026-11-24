"""
Gateway tests. Run from this folder:   pip install -r requirements-dev.txt && pytest

No Kafka broker is needed: publish() is replaced with a recorder and the isolation
follower is tested against a fake consumer.
"""
import os
from pathlib import Path
from types import SimpleNamespace

os.environ["DISABLE_KAFKA"] = "1"
os.environ.setdefault("GATEWAY_API_TOKEN", "test-token-0123456789abcdef")
os.environ.setdefault("PATIENT_REGISTRY_PATH", str(Path(__file__).resolve().parent.parent / "data" / "patients.json"))

import pytest
from fastapi.testclient import TestClient
from kafka import TopicPartition

import isolation
import main

import json
import uuid
from datetime import datetime, timedelta, timezone

import auth
import limits

AUTH = {"Authorization": "Bearer " + auth.device_token("infusion-pump-001")}   # the default test device's own token


def token_for(device_id):
    return {"Authorization": "Bearer " + auth.device_token(device_id)}


def pump(**over):
    base = {
        "message_id": "m-" + uuid.uuid4().hex[:12], "device_id": "infusion-pump-001", "device_type": "infusion_pump",
        "patient_id": "P-1001", "unit": "ICU-3", "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "packet_size_bytes": 300, "request_rate_per_min": 3.0,
        "infusion_rate_ml_per_hr": 75.0, "occlusion_pressure_mmHg": 4.0, "battery_pct": 80,
    }
    base.update(over)
    return base


@pytest.fixture()
def published(monkeypatch):
    """Client with a clean isolation registry and a recorder instead of Kafka."""
    sent = []
    monkeypatch.setattr(main, "publish", lambda topic, key, value: sent.append((topic, key, value)))
    limits.reset()
    fresh = isolation.IsolationRegistry()
    fresh.mark_ready()
    monkeypatch.setattr(isolation, "registry", fresh)
    with TestClient(main.app) as client:
        client.sent, client.registry = sent, fresh
        yield client


# ---------- authentication and schema ----------
def test_missing_token_is_401(published):
    assert published.post("/ingest", json=pump()).status_code == 401
    assert published.sent == []


def test_wrong_token_is_401(published):
    r = published.post("/ingest", json=pump(), headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401 and published.sent == []


def test_extra_field_is_422(published):
    r = published.post("/ingest", json=pump(injected="x"), headers=AUTH)
    assert r.status_code == 422 and [t for t, *_ in published.sent] == ["iomt.telemetry.rejected"]   # nothing reaches the raw topic


def test_out_of_bounds_value_is_422(published):
    r = published.post("/ingest", json=pump(battery_pct=500), headers=AUTH)
    assert r.status_code == 422 and all(t != "iomt.telemetry.raw" for t, *_ in published.sent)


def test_valid_message_is_accepted_and_keyed_by_device(published):
    r = published.post("/ingest", json=pump(), headers=AUTH)
    assert r.status_code == 201 and r.json()["status"] == "accepted"
    topic, key, value = published.sent[0]
    assert key == "infusion-pump-001" and "ingested_at" in value and "vulnerability_flags" not in value


# ---------- hardening ----------
def test_a_device_token_only_works_for_that_device(published):
    r = published.post("/ingest", json=pump(device_id="infusion-pump-002"), headers=AUTH)   # pump-001's token, claiming pump-002
    assert r.status_code == 401 and published.sent == []
    assert published.post("/ingest", json=pump(device_id="infusion-pump-002"), headers=token_for("infusion-pump-002")).status_code == 201


def test_weak_or_default_master_secret_stops_startup(monkeypatch):
    import importlib
    for bad in ("", "dev-shared-secret-change-me", "short", "change-me-generate-a-long-random-value"):
        monkeypatch.setenv("GATEWAY_API_TOKEN", bad)
        with pytest.raises(RuntimeError):
            auth._load_master()


def test_isolation_cannot_be_dodged_with_lookalike_ids(published):
    published.registry.apply({"device_id": "infusion-pump-001", "isolated": True})
    for twin in ("INFUSION-PUMP-001", "infusion-pump-001 ", "infusion-pump-001\u200b"):
        r = published.post("/ingest", json=pump(device_id=twin), headers=token_for(twin))
        assert r.status_code == 422, twin
    assert published.sent == [] or all(t != "iomt.telemetry.raw" for t, *_ in published.sent)


def test_replayed_message_is_refused_but_a_retry_after_failure_is_not(published, monkeypatch):
    body = pump()
    assert published.post("/ingest", json=body, headers=AUTH).status_code == 201
    assert published.post("/ingest", json=body, headers=AUTH).status_code == 409
    flaky = pump()
    monkeypatch.setattr(main, "publish", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
    assert published.post("/ingest", json=flaky, headers=AUTH).status_code == 503
    monkeypatch.setattr(main, "publish", lambda t, key, value: published.sent.append((t, key, value)))
    assert published.post("/ingest", json=flaky, headers=AUTH).status_code == 201   # not marked as seen when it failed


def test_stale_and_future_timestamps_are_refused(published):
    for delta in (-3600, 3600):
        ts = (datetime.now(timezone.utc) + timedelta(seconds=delta)).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert published.post("/ingest", json=pump(timestamp=ts), headers=AUTH).status_code == 422


def test_oversized_body_is_refused_before_parsing(published):
    r = published.post("/ingest", content=json.dumps(pump(unit="A" * 100_000)), headers={**AUTH, "Content-Type": "application/json"})
    assert r.status_code == 413


def test_non_finite_and_huge_numbers_are_refused(published):
    text = json.dumps(pump()).replace('"request_rate_per_min": 3.0', '"request_rate_per_min": Infinity')
    assert published.post("/ingest", content=text, headers={**AUTH, "Content-Type": "application/json"}).status_code == 422
    assert published.post("/ingest", json=pump(request_rate_per_min=1e15), headers=AUTH).status_code == 422


def test_burst_from_one_device_is_throttled_but_others_are_not(published):
    codes = [published.post("/ingest", json=pump(), headers=AUTH).status_code for _ in range(80)]
    assert codes.count(201) == limits.device_rate.burst and 429 in codes
    assert published.post("/ingest", json=pump(device_id="infusion-pump-002"), headers=token_for("infusion-pump-002")).status_code == 201


def test_token_guessing_is_locked_out(published):
    codes = [published.post("/ingest", json=pump(), headers={"Authorization": f"Bearer g{i}"}).status_code for i in range(25)]
    assert codes[:10] == [401] * 10 and set(codes[10:]) == {429}


def test_kafka_outage_gives_503_without_leaking_details(published, monkeypatch):
    monkeypatch.setattr(main, "publish", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("broker kafka:9092 down")))
    r = published.post("/ingest", json=pump(), headers=AUTH)
    assert r.status_code == 503 and "kafka:9092" not in r.text


def test_rejected_messages_go_to_the_dead_letter_topic_only_when_credentialed(published):
    published.post("/ingest", json=pump(battery_pct=999))                       # no token: not recorded
    assert published.sent == []
    published.post("/ingest", json=pump(battery_pct=999), headers=AUTH)          # credentialed but invalid: recorded
    topic, _, record = published.sent[0]
    assert topic == "iomt.telemetry.rejected" and record["reason"] == "schema_validation" and "battery_pct" in json.dumps(record["detail"])


def test_docs_are_not_exposed(published):
    assert [published.get(p).status_code for p in ("/docs", "/redoc", "/openapi.json")] == [404, 404, 404]


# ---------- checks that attach flags ----------
def test_config_vulnerabilities_are_flagged(published):
    cfg = {"auto_lock_enabled": False, "default_credentials_changed": True, "firmware_up_to_date": True, "tls_enabled": True}
    r = published.post("/ingest", json=pump(config=cfg), headers=AUTH)
    assert r.status_code == 201 and r.json()["vulnerability_flags"] == ["auto_lock_disabled"]
    assert published.sent[0][2]["remediation_tips"]


def test_prescription_mismatch_is_flagged(published):
    r = published.post("/ingest", json=pump(infusion_rate_ml_per_hr=300.0), headers=AUTH)
    assert "infusion_rate_mismatch" in r.json()["vulnerability_flags"]


# ---------- isolation enforcement ----------
def test_isolated_device_is_refused_and_nothing_is_published(published):
    published.registry.apply({"device_id": "infusion-pump-001", "isolated": True})
    r = published.post("/ingest", json=pump(), headers=AUTH)
    assert r.status_code == 403 and published.sent == []
    assert published.get("/health").json()["blocked_messages"] == 1


def test_isolation_only_affects_that_device(published):
    published.registry.apply({"device_id": "infusion-pump-002", "isolated": True})
    assert published.post("/ingest", json=pump(), headers=AUTH).status_code == 201


def test_restore_lets_the_device_through_again(published):
    published.registry.apply({"device_id": "infusion-pump-001", "isolated": True})
    published.registry.apply({"device_id": "infusion-pump-001", "isolated": False})
    assert published.post("/ingest", json=pump(), headers=AUTH).status_code == 201


def test_isolated_device_still_needs_a_valid_token(published):
    published.registry.apply({"device_id": "infusion-pump-001", "isolated": True})
    assert published.post("/ingest", json=pump()).status_code == 401


def test_gateway_refuses_with_503_until_isolation_list_is_loaded(monkeypatch, published):
    not_ready = isolation.IsolationRegistry()
    monkeypatch.setattr(isolation, "registry", not_ready)
    r = published.post("/ingest", json=pump(), headers=AUTH)
    assert r.status_code == 503 and published.sent == []
    assert published.get("/health").json()["isolation_ready"] is False


# ---------- registry and follower ----------
def test_registry_ignores_malformed_commands():
    reg = isolation.IsolationRegistry()
    for bad in [None, {}, {"device_id": "x"}, {"device_id": "", "isolated": True},
                {"device_id": "x", "isolated": "yes"}, {"device_id": 5, "isolated": True}, "text"]:
        reg.apply(bad)
    assert reg.count() == 0


class FakeConsumer:
    """Just enough of KafkaConsumer for isolation.follow(): one partition holding `events`."""

    def __init__(self, events, topic_exists=True):
        self.events, self.pos, self.topic_exists = events, 0, topic_exists
        self.assigned = None

    def partitions_for_topic(self, topic):
        return {0} if self.topic_exists else None

    def assign(self, tps): self.assigned = tps
    def seek_to_beginning(self, *tps): self.pos = 0
    def end_offsets(self, tps): return {tp: len(self.events) for tp in tps}
    def position(self, tp): return self.pos

    def poll(self, timeout_ms=0):
        if self.pos >= len(self.events):
            return {}
        batch = self.events[self.pos:self.pos + 2]       # deliver two at a time
        self.pos += len(batch)
        tp = TopicPartition("iomt.actions", 0)
        return {tp: [SimpleNamespace(value=e) for e in batch]}


def run_follow(consumer, reg, polls):
    import threading
    stop, n = threading.Event(), {"n": 0}
    real_poll = consumer.poll

    def limited(timeout_ms=0):
        n["n"] += 1
        if n["n"] > polls:
            stop.set()
        return real_poll(timeout_ms)

    consumer.poll = limited
    isolation.follow(consumer, reg, "iomt.actions", stop)


def test_follower_replays_history_last_event_wins_then_marks_ready():
    events = [
        {"device_id": "a", "isolated": True},
        {"device_id": "b", "isolated": True},
        {"device_id": "a", "isolated": False},   # a restored later
        {"device_id": "c", "isolated": True},
    ]
    reg = isolation.IsolationRegistry()
    run_follow(FakeConsumer(events), reg, polls=5)
    assert reg.ready
    assert reg.is_isolated("b") and reg.is_isolated("c") and not reg.is_isolated("a")


def test_follower_is_ready_immediately_on_an_empty_topic():
    reg = isolation.IsolationRegistry()
    run_follow(FakeConsumer([]), reg, polls=2)
    assert reg.ready and reg.count() == 0


def test_follower_is_not_ready_before_it_has_caught_up():
    reg = isolation.IsolationRegistry()
    events = [{"device_id": str(i), "isolated": True} for i in range(10)]   # 2 per poll -> 5 polls
    run_follow(FakeConsumer(events), reg, polls=2)
    assert not reg.ready