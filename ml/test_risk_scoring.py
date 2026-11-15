"""
Run with:  python ml/test_risk_scoring.py
Kafka or Docker not needed. Trains a small model on stand-in data (a few seconds).
"""
import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "stream"))
sys.path.insert(0, str(HERE))

from make_synthetic_data import generate  # noqa: E402
from preprocessing import FEATURE_COLUMNS, preprocess  # noqa: E402
from risk_scoring import (  # noqa: E402
    ALERT_THRESHOLD, RiskScorer, fit_calibration, fit_envelope, score_to_risk, scores_to_risk,
)


def _small_model():
    train, test = generate(n_normal=4000, n_test_normal=200, n_test_attack=100, seed=1)
    X = pd.DataFrame([preprocess(r)[1] for r in train], columns=FEATURE_COLUMNS)
    model = IsolationForest(n_estimators=100, random_state=1).fit(X.iloc[:3200])
    cal = fit_calibration(-model.score_samples(X.iloc[3200:]))
    return model, cal, test


def test_risk_is_between_0_and_100_and_monotonic():
    cal = fit_calibration(np.random.default_rng(0).normal(0.5, 0.05, 2000))
    risk = scores_to_risk(np.linspace(0.0, 1.5, 300), cal)
    assert risk.min() >= 0.0 and risk.max() <= 100.0
    assert (np.diff(risk) >= -1e-9).all(), "risk must never go down as the anomaly score goes up"


def test_anchor_points_map_where_documented():
    scores = np.random.default_rng(0).normal(0.5, 0.05, 20000)
    cal = fit_calibration(scores)
    assert score_to_risk(np.percentile(scores, 50), cal) < 1
    assert abs(score_to_risk(np.percentile(scores, 99), cal) - ALERT_THRESHOLD) < 1
    assert score_to_risk(np.percentile(scores, 99.9), cal) > 99
    assert score_to_risk(10.0, cal) == 100.0
    assert score_to_risk(-10.0, cal) == 0.0


def test_about_one_percent_of_unseen_normal_triggers_an_alert():
    model, cal, test = _small_model()
    normal = [r for r in test if r["_label"] == 0]
    X = pd.DataFrame([preprocess(r)[1] for r in normal], columns=FEATURE_COLUMNS)
    rate = (scores_to_risk(-model.score_samples(X), cal) >= ALERT_THRESHOLD).mean()
    assert rate < 0.05, f"false alarm rate {rate:.2%} is far above the ~1% design target"


def test_attacks_score_much_higher_than_normal():
    model, cal, test = _small_model()
    X = pd.DataFrame([preprocess(r)[1] for r in test], columns=FEATURE_COLUMNS)
    y = np.array([r["_label"] for r in test])
    risk = scores_to_risk(-model.score_samples(X), cal)
    assert risk[y == 1].mean() > risk[y == 0].mean() + 30


def test_risk_scorer_scores_one_vector_as_an_int():
    model, cal, test = _small_model()
    scorer = RiskScorer({"model": model, "feature_columns": FEATURE_COLUMNS, "calibration": cal})
    value = scorer.score_one(preprocess(test[0])[1])
    assert isinstance(value, int) and 0 <= value <= 100


# ---------- envelope: readings far outside their normal range ----------
def _scorer_with_envelope():
    train, _ = generate(n_normal=4000, n_test_normal=10, n_test_attack=3, seed=1)
    X = pd.DataFrame([preprocess(r)[1] for r in train], columns=FEATURE_COLUMNS)
    model = IsolationForest(n_estimators=100, random_state=1).fit(X.iloc[:3200])
    cal = fit_calibration(-model.score_samples(X.iloc[3200:]))
    return RiskScorer({"model": model, "feature_columns": FEATURE_COLUMNS, "calibration": cal,
                       "envelope": fit_envelope(X)}), train


def _pump(train, **over):
    r = dict(next(t for t in train if t["device_type"] == "infusion_pump"))
    r.update(over)
    return preprocess(r)[1]


def test_envelope_flags_a_reading_far_outside_its_range_and_says_which():
    scorer, train = _scorer_with_envelope()
    detail = scorer.score_detail(_pump(train, infusion_rate_ml_per_hr=900.0))
    assert detail["risk"] >= ALERT_THRESHOLD
    assert detail["reasons"] and detail["reasons"][0].startswith("infusion_rate_ml_per_hr = 900")
    assert "normal" in detail["reasons"][0]


def test_envelope_risk_grows_with_distance_and_caps_at_100():
    scorer, train = _scorer_with_envelope()
    near = scorer.score_detail(_pump(train, infusion_rate_ml_per_hr=160.0))["envelope_risk"]
    far = scorer.score_detail(_pump(train, infusion_rate_ml_per_hr=400.0))["envelope_risk"]
    huge = scorer.score_detail(_pump(train, infusion_rate_ml_per_hr=1e6))["envelope_risk"]
    assert ALERT_THRESHOLD <= near < far <= huge == 100.0


def test_envelope_ignores_a_reading_inside_its_range():
    scorer, train = _scorer_with_envelope()
    detail = scorer.score_detail(_pump(train, infusion_rate_ml_per_hr=100.0, occlusion_pressure_mmHg=40.0,
                                       battery_pct=60.0, packet_size_bytes=400, request_rate_per_min=12))
    assert detail["envelope_risk"] == 0.0


def test_each_device_type_has_its_own_range():
    # 150 mL/hr is nothing for a pump but it is not a field a cardiac monitor reports at all
    scorer, train = _scorer_with_envelope()
    monitor = dict(next(t for t in train if t["device_type"] == "cardiac_monitor"))
    assert "infusion_rate_ml_per_hr" not in scorer.envelope["is_cardiac_monitor"]
    assert scorer.score_detail(preprocess(monitor)[1])["envelope_risk"] == 0.0


def test_a_model_file_without_an_envelope_still_works():
    model, cal, test = _small_model()
    scorer = RiskScorer({"model": model, "feature_columns": FEATURE_COLUMNS, "calibration": cal})
    detail = scorer.score_detail(preprocess(test[0])[1])
    assert detail["envelope_risk"] == 0.0 and 0 <= detail["risk"] <= 100


if __name__ == "__main__":
    random.seed(0)
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS  {t.__name__}")
    print(f"\n{len(tests)} tests passed")