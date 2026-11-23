"""
Run with:  python ml/test_autoencoder.py
No Kafka or Docker needed. Trains a small autoencoder on stand-in data (a few seconds).
"""
import random
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "stream"))
sys.path.insert(0, str(HERE))

from autoencoder import AutoencoderScorer, fit_autoencoder  # noqa: E402
from make_synthetic_data import generate  # noqa: E402
from preprocessing import FEATURE_COLUMNS, preprocess  # noqa: E402
from risk_scoring import ALERT_THRESHOLD, fit_calibration, fit_envelope, scores_to_risk  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402


def _build(with_envelope=True):
    train, test = generate(n_normal=4000, n_test_normal=300, n_test_attack=150, seed=2)
    X = pd.DataFrame([preprocess(r)[1] for r in train], columns=FEATURE_COLUMNS)
    fit, cal = X.iloc[:3200], X.iloc[3200:]
    scaler, model = fit_autoencoder(fit, seed=1)
    bundle = {"scaler": scaler, "model": model, "feature_columns": FEATURE_COLUMNS,
              "calibration": {"values": [0, 1, 2, 3], "risks": [0, 30, 70, 100]}}
    bundle["calibration"] = fit_calibration(AutoencoderScorer(bundle).anomaly_scores(cal))
    if with_envelope:
        bundle["envelope"] = fit_envelope(X)
    return AutoencoderScorer(bundle), train, test


def _test_matrix(test):
    X = pd.DataFrame([preprocess(r)[1] for r in test], columns=FEATURE_COLUMNS)
    return X, np.array([r["_label"] for r in test])


def test_attacks_are_rebuilt_worse_than_normal_messages():
    ae, _, test = _build()
    X, y = _test_matrix(test)
    assert roc_auc_score(y, ae.anomaly_scores(X)) > 0.9


def test_about_one_percent_of_unseen_normal_triggers_an_alert():
    ae, _, test = _build()
    X, y = _test_matrix(test)
    rate = (ae.risk_scores(X[y == 0]) >= ALERT_THRESHOLD).mean()
    assert rate < 0.06, f"false alarm rate {rate:.2%} is far above the ~1% design target"


def test_risk_is_between_0_and_100():
    ae, _, test = _build()
    X, _ = _test_matrix(test)
    risk = ae.risk_scores(X)
    assert risk.min() >= 0 and risk.max() <= 100


def test_save_and_load_gives_identical_scores():
    ae, _, test = _build()
    X, _ = _test_matrix(test)
    import joblib
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "ae.joblib"
        joblib.dump({"scaler": ae.scaler, "model": ae.model, "feature_columns": ae.columns,
                     "calibration": ae.calibration, "envelope": ae.envelope}, path)
        again = AutoencoderScorer.load(path)
    assert np.allclose(ae.anomaly_scores(X), again.anomaly_scores(X))


def test_score_one_returns_an_int_and_extreme_reading_names_its_reason():
    ae, train, _ = _build()
    pump = dict(next(t for t in train if t["device_type"] == "infusion_pump"))
    normal = ae.score_detail(preprocess(pump)[1])
    assert isinstance(ae.score_one(preprocess(pump)[1]), int) and 0 <= normal["risk"] <= 100
    pump["infusion_rate_ml_per_hr"] = 900.0
    bad = ae.score_detail(preprocess(pump)[1])
    assert bad["risk"] >= ALERT_THRESHOLD and bad["reasons"][0].startswith("infusion_rate_ml_per_hr = 900")


def test_a_model_without_an_envelope_still_works():
    ae, train, _ = _build(with_envelope=False)
    detail = ae.score_detail(preprocess(train[0])[1])
    assert detail["envelope_risk"] == 0.0 and 0 <= detail["risk"] <= 100


if __name__ == "__main__":
    random.seed(0)
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS  {t.__name__}")
    print(f"\n{len(tests)} tests passed")
