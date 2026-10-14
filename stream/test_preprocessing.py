"""
Run with: python stream/test_preprocessing.py
No Kafka or Docker needed: this feeds fake messages straight into preprocess().
"""

from preprocessing import (
    FEATURE_COLUMNS,
    PreprocessingError,
    preprocess,
    records_to_dataframe,
)

COMMON = {
    "message_id": "t-1",
    "device_id": "dev-1",
    "unit": "ICU-3",
    "timestamp": "2026-09-29T03:30:00Z",
    "ingested_at": "2026-09-29T03:30:01+00:00",
    "packet_size_bytes": 512,
    "request_rate_per_min": 12.5,
}

CARDIAC = {
    **COMMON, "device_type": "cardiac_monitor", "heart_rate_bpm": 78,
    "spo2_pct": 98, "systolic_bp_mmHg": 120, "diastolic_bp_mmHg": 80
}
PUMP = {
    **COMMON, "device_type": "infusion_pump", "infusion_rate_ml_hr": 125.0,
    "occlusion_pressure_mmHg": 40.0, "battery_pct": 90
}
OXIMETER = {
    **COMMON, "device_type": "pulse_oximeter", "spo2_pct": 97, "pulse_bpm": 72
}

def as_dict(features):
    return dict(zip(FEATURE_COLUMNS, features))

def test_fixed_length_for_every_device_type():
    for rec in (CARDIAC, PUMP, OXIMETER):
        _, feats = preprocess(rec)
        assert len(feats) == len(FEATURE_COLUMNS), rec["device_type"]

def test_cardiac_values_land_in_right_columns():
    _, feats = preprocess(CARDIAC)
    f = as_dict(feats)
    assert f["heart_rate_bpm"] == 78.0
    assert f["systolic_bp_mmHg"] == 120.0
    assert f["is_cardiac_monitor"] == 1.0
    assert f["is_infusion_pump"] == 0.0
    assert f["infusion_rate_ml_per_hr"] == 0.0 #not a pump -> 0.0

def test_pulse_bpm_is_renamed_to_heart_rate():
    _, feats = preprocess(OXIMETER)
    f = as_dict(feats)
    assert f["heart_rate_bpm"] == 72.0
    assert f["is_pulse_oximeter"] == 1.0

def test_metadata_kept_but_not_in_feature():
    meta, feats = preprocess(CARDIAC)
    assert meta["device_id"] == "dev-1"
    assert meta["message_id"] == "t-1"
    assert "device_id" not in FEATURE_COLUMNS
    assert all(isinstance(x, float) for x in feats)

def test_unknown_device_type_rejected():
    try:
        preprocess({**COMMON, "device_type": "toaster"})
    except PreprocessingError:
        return
    raise AssertionError("expected PreprocessingError")

def test_non_numeric_value_rejected():
    try:
        preprocess({**CARDIAC, "heart_rate_bpm": "fast"})
    except PreprocessingError:
        return
    raise AssertionError("expected PreprocessingError")

def test_dataframe_shape_and_skips_bad_rows():
    df = records_to_dataframe([CARDIAC, PUMP, OXIMETER, {**COMMON, "device_type": "toaster"}])
    assert df.shape == (3, len(FEATURE_COLUMNS))
    assert list(df.columns) == FEATURE_COLUMNS

if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS {t.__name__}")
    print(f"\n{len(tests)} tests passed")
    print("\nExample vector for the cardiac monitor test payload:")
    _, feats = preprocess(CARDIAC)
    for name, value in zip(FEATURE_COLUMNS, feats):
        print(f" {name:28s} {value}")