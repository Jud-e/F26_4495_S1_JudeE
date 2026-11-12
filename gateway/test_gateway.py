"""
Gateway tests. Run from this folder:   pip install -r requirements-dev.txt && pytest

No Kafka broker is needed: publish() is replaced with a recorder and the isolation
follower is tested against a fake consumer.
"""
import os
from pathlib import Path
from types import SimpleNamespace

os.environ["DISABLE_KAFKA"] = "1"
os.environ.setdefault("GATEWAY_API_TOKEN", "test-token")
os.environ.setdefault("PATIENT_REGISTRY_PATH", str(Path(__file__).resolve().parent.parent / "data" / "patients.json"))

import pytest
from fastapi.testclient import TestClient
from kafka import TopicPartition

import isolation
import main

AUTH = {"Authorization": "Bearer " + os.environ["GATEWAY_API_TOKEN"]}


def pump(**over):
    base = {
        "message_id": "m-1", "device_id": "infusion-pump-001", "device_type": "infusion_pump",
        "patient_id": "P-1001", "unit": "ICU-3", "timestamp": "2026-10-30T10:00:00Z",
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
    assert r.status_code == 422 and published.sent == []


def test_out_of_bounds_value_is_422(published):
    r = published.post("/ingest", json=pump(battery_pct=500), headers=AUTH)
    assert r.status_code == 422 and published.sent == []


def test_valid_message_is_accepted_and_keyed_by_device(published):
    r = published.post("/ingest", json=pump(), headers=AUTH)
    assert r.status_code == 201 and r.json()["status"] == "accepted"
    topic, key, value = published.sent[0]
    assert key == "infusion-pump-001" and "ingested_at" in value and "vulnerability_flags" not in value


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