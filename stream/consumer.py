"""
Consumes raw telemetry from Kafka, preprocesses it, scores it, and publishes the result.

Run locally (outside Docker) against the host-exposed Kafka port:
    python stream/consumer.py

Scoring needs a trained model. Create one first:
    python ml/train_isolation_forest.py --input <normal telemetry .jsonl>
Without a model file the consumer still runs, but every risk_score is 0
(so a 0 on the dashboard can mean "no model", not "safe").
"""
import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from preprocessing import FEATURE_COLUMNS, PreprocessingError, preprocess

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ml"))  # so we can import risk_scoring

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_IN = os.getenv("TOPIC_TELEMETRY_RAW", "iomt.telemetry.raw")
TOPIC_OUT = os.getenv("TOPIC_TELEMETRY_SCORED", "iomt.telemetry.scored")
MODEL_PATH = Path(os.getenv("MODEL_PATH", ROOT / "ml" / "models" / "isolation_forest.joblib"))


def load_scorer():
    """Load the trained model, or return None (risk 0) if there isn't one yet."""
    if not MODEL_PATH.exists():
        print(f"WARNING: no model at {MODEL_PATH} -- every risk_score will be 0.")
        print("         Train one with: python ml/train_isolation_forest.py --input <normal .jsonl>")
        return None

    from risk_scoring import RiskScorer  # imported here so the consumer runs without scikit-learn

    scorer = RiskScorer.load(MODEL_PATH)
    # The model learned "column N means X". If the order ever changed after
    # training, scores would be silently wrong -- so refuse to start.
    if list(scorer.columns) != list(FEATURE_COLUMNS):
        sys.exit("Model was trained on different feature columns than preprocessing.py now produces. Retrain it.")
    print(f"Loaded model from {MODEL_PATH}")
    return scorer


def main():
    scorer = load_scorer()

    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="stream-preprocessing",
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Listening on '{TOPIC_IN}', publishing to '{TOPIC_OUT}'...")

    for message in consumer:
        record = message.value

        # Preprocessing: raw message -> (identifying metadata, numeric vector).
        # A record we can't vectorize is skipped, not allowed to crash the stream.
        try:
            meta, features = preprocess(record)
        except PreprocessingError as exc:
            print(f"Skipping unusable record: {exc}")
            continue

        # Model inference: feature vector -> risk score 0-100.
        risk_score = scorer.score_one(features) if scorer else 0

        # Keep the original record (vulnerability flags, remediation tips...) and attach what we computed.
        scored = dict(record)
        scored["features"] = dict(zip(FEATURE_COLUMNS, features))
        scored["risk_score"] = risk_score
        producer.send(TOPIC_OUT, scored)
        print(f"Scored {meta['device_id']} ({meta['device_type']}): risk_score={risk_score}")


if __name__ == "__main__":
    main()
