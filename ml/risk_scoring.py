"""
Turns the Isolation Forest's raw anomaly score into a 0-100 risk score.

The raw score has no natural meaning, so we calibrate it against HELD-OUT NORMAL
data using percentiles. Each anchor is (percentile of normal data, risk score):

    p50   -> 0     an ordinary message
    p90   -> 30    normal data looks this odd 1 time in 10
    p99   -> 70    ALERT_THRESHOLD: ~1% of normal messages reach this
    p99.9 -> 100   far outside anything seen in normal data

Between anchors the mapping is a straight line, so risk never decreases as the
anomaly score increases.
"""
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ANCHORS = [(50.0, 0.0), (90.0, 30.0), (99.0, 70.0), (99.9, 100.0)]
ALERT_THRESHOLD = 70


def fit_calibration(normal_scores) -> dict:
    """Learn the anchor positions from anomaly scores of held-out normal data."""
    pcts = [a[0] for a in ANCHORS]
    values = np.percentile(np.asarray(normal_scores, dtype=float), pcts).astype(float)
    for i in range(1, len(values)):          # np.interp needs strictly increasing x
        if values[i] <= values[i - 1]:
            values[i] = values[i - 1] + 1e-9
    return {"values": values.tolist(), "risks": [a[1] for a in ANCHORS]}


def scores_to_risk(anomaly_scores, calibration: dict) -> np.ndarray:
    """Vectorised: array of anomaly scores -> array of risk scores in [0, 100]."""
    return np.interp(np.asarray(anomaly_scores, dtype=float),
                     calibration["values"], calibration["risks"])


def score_to_risk(anomaly_score: float, calibration: dict) -> float:
    return float(scores_to_risk([anomaly_score], calibration)[0])


class RiskScorer:
    """A trained model + its column list + its calibration, loaded from one file."""

    def __init__(self, bundle: dict):
        self.model = bundle["model"]
        self.columns = list(bundle["feature_columns"])
        self.calibration = bundle["calibration"]

    @classmethod
    def load(cls, path) -> "RiskScorer":
        return cls(joblib.load(Path(path)))

    def anomaly_scores(self, X) -> np.ndarray:
        """Higher = more anomalous. (scikit-learn's score_samples is the reverse.)"""
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X, columns=self.columns)
        return -self.model.score_samples(X[self.columns])

    def risk_scores(self, X) -> np.ndarray:
        return scores_to_risk(self.anomaly_scores(X), self.calibration)

    def score_one(self, features) -> int:
        """One feature vector (list of floats) -> integer risk 0-100."""
        X = pd.DataFrame([list(features)], columns=self.columns)
        return int(round(float(self.risk_scores(X)[0])))
