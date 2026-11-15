"""
End-to-end check of the stream consumer with the real trained model and a fake Kafka.

    cd stream && pytest test_consumer.py

Needs ml/models/isolation_forest.joblib (train it with the steps in consumer.py); skipped if absent.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "ml"))
import consumer  # noqa: E402

MODEL = HERE.parent / "ml" / "models" / "isolation_forest.joblib"
pytestmark = pytest.mark.skipif(not MODEL.exists(), reason="no trained model; see consumer.py docstring")


def pump(**over):
    r = {"message_id": "m", "device_id": "infusion-pump-001", "device_type": "infusion_pump", "unit": "ICU-3",
         "timestamp": "2026-11-14T10:00:00Z", "packet_size_bytes": 250, "request_rate_per_min": 3.5,
         "infusion_rate_ml_per_hr": 80.0, "occlusion_pressure_mmHg": 5.0, "battery_pct": 70,
         "vulnerability_flags": ["tls_disabled"]}
    r.update(over)
    return r


def run(monkeypatch, records, model_path=MODEL):
    sent = []
    monkeypatch.setattr(consumer, "MODEL_PATH", model_path)
    monkeypatch.setattr(consumer, "KafkaConsumer", lambda *a, **k: [SimpleNamespace(value=r) for r in records])
    monkeypatch.setattr(consumer, "KafkaProducer",
                        lambda *a, **k: SimpleNamespace(send=lambda topic, value: sent.append((topic, value))))
    consumer.main()
    return [v for _, v in sent]


def test_normal_message_scores_low_and_keeps_the_gateways_flags(monkeypatch):
    (out,) = run(monkeypatch, [pump()])
    assert out["risk_score"] < 70 and out["risk_reasons"] == []
    assert out["vulnerability_flags"] == ["tls_disabled"]            # the original record is carried through
    assert out["features"]["infusion_rate_ml_per_hr"] == 80.0


def test_a_flood_scores_high_and_says_why(monkeypatch):
    (out,) = run(monkeypatch, [pump(request_rate_per_min=600.0, packet_size_bytes=9000)])
    assert out["risk_score"] >= 70
    assert any(r.startswith("request_rate_per_min = 600") for r in out["risk_reasons"])


def test_a_prescription_style_overdose_rate_scores_high(monkeypatch):
    (out,) = run(monkeypatch, [pump(infusion_rate_ml_per_hr=350.0)])
    assert out["risk_score"] >= 70 and "infusion_rate_ml_per_hr" in out["risk_reasons"][0]


def test_an_unusable_record_is_skipped_not_fatal(monkeypatch):
    out = run(monkeypatch, [pump(device_type="toaster"), pump(message_id="after")])
    assert [o["message_id"] for o in out] == ["after"]


def test_without_a_model_every_score_is_zero(monkeypatch, tmp_path):
    (out,) = run(monkeypatch, [pump(infusion_rate_ml_per_hr=350.0)], model_path=tmp_path / "missing.joblib")
    assert out["risk_score"] == 0 and out["risk_reasons"] == []
