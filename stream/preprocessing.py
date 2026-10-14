"""
Stream preprocessing: turns one raw telemetry message into a fixed-length numeric feature vector the ML models can consume.

Kept free of kafka on purpose, so the exact same function runs in 3 places
1. stream/consumer.py (live scoring)
2. ml/train_*.py (building the training set)
3. tests (docker wouldn't be needed)

Use on function for training and live scoring to guarantee thee model sees indetically-shaped
data in both. if these ever drift apart, the model silently scores garbages.

TODO (David):
- Preprocessing: normalize/clean the HL7/FHIR-shaped payload fields
- Feed cleaned records into the ML model (see ml/train_isolation_forest.py)
- Publish the result (record + risk score) to TOPIC_TELEMETRY_SCORED
"""

from typing import Any

# Standardize field names
# -- different device types report the same physical quantity different names. (Map them onto 'ONE' canonical name so they share a column).

FIELD_ALIASES = {
    "pulse_bpm": "heart_rate_bpm", # pulse oximeter - same as a cardiac monitor
}

# Feature Vector Layout
# -- order matters so every record must produce columns in an exact order.

DEVICE_TYPES = ["infusion_pump", "cardiac_monitor", "pulse_oximeter"]

NUMERIC_FEATURES = [
    # shared by every device
    "packet_size_bytes",
    "request_rate_per_min",

    # infusion pump
    "infusion_rate_ml_per_hr",
    "occlusion_pressure_mmHg",
    "battery_pct",

    # cardiac monitor / pulse oximeter (spo2 and heart rate are shared)
    "heart_rate_bpm",
    "spo2_pct",
    "systolic_bp_mmHg",
    "diastolic_bp_mmHg",
]

FEATURE_COLUMNS = NUMERIC_FEATURES + [f"is_{d}" for d in DEVICE_TYPES]


# Fields that identify/trace a record but would not be fed to the model
META_FIELDS = [
    "message_id", "device_id", "device_type", "unit", "timestamp", "ingested_at"
]

class PreprocessingError(ValueError):
    """Raised for a record we cannot turn into a feature vector."""

def _canonicalize(record: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of the record with aliased field names renamed."""
    return {FIELD_ALIASES.get(k, k): v for k, v in record.items()}

def _to_float(value: Any, field: str) -> float:
    """Coerce a value to float; a missing/None value becomes 0.0"""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PreprocessingError(f"field '{field}' is not numeric: {value!r}") from exc

def preprocess(record: dict[str, Any]) -> tuple[dict[str, Any],list[float]]:
    """
    Convert one raw message into (meta, features).
    meta -> identify fields, carried alongside the vector
    features -> lists of floats, length == len(FEATURE_COLUMNS), fixed order
    """
    device_type = record.get("device_type")
    if device_type not in DEVICE_TYPES:
        raise PreprocessingError(f"unknown device_type: {device_type!r}")

    clean = _canonicalize(record)

    # Numeric features
    numeric = [_to_float(clean.get(name), name) for name in NUMERIC_FEATURES]

    # Categorical -> numeric: one-hot encode the device type because a model cannot read the string "cardiac_monitor"
    one_hot = [1.0 if device_type == d else 0.0 for d in DEVICE_TYPES]

    meta = {name: clean.get(name) for name in META_FIELDS}
    return meta, numeric + one_hot

def records_to_dataframe(records: list[dict[str, Any]]):
    """
    Batch version for training: list of raw messages -> pandas DataFrame with one row per record and columns == FEATURE_COLUMNS.
    Records that fail preprocessing are skipped (and counted) rather than crashing the run.
    """
    import pandas as pd

    rows, skipped = [], 0
    for rec in records:
        try:
            _, features = preprocess(rec)
            rows.append(features)
        except PreprocessingError:
            skipped += 1
    if skipped:
        print(f"records_to_dataframe: skipped {skipped} unusable record(s)")
    return pd.DataFrame(rows, columns=FEATURE_COLUMNS)