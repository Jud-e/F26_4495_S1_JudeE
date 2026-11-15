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

A second signal, the "envelope", is combined with the forest's. An Isolation Forest only
splits inside the range of its normal training data, so a reading far OUTSIDE that range
(a pump at 400 mL/hr when normal tops out near 150) looks almost as ordinary to it as one at
the edge. The envelope measures exactly that: for each device type it records the range each
reading takes in normal data (p0.1 to p99.9). A reading outside the range scores at least
ALERT_THRESHOLD, rising to 100 when it is a full normal-range-width outside. Final risk is
the larger of the two signals, and the envelope also says WHICH reading was out of range.
The forest keeps covering odd combinations of readings that are each individually normal.
"""
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ANCHORS = [(50.0, 0.0), (90.0, 30.0), (99.0, 70.0), (99.9, 100.0)]
ALERT_THRESHOLD = 70
ENVELOPE_PCTS = (0.1, 99.9)     # the "normal range" of a reading: these percentiles of normal data
MAX_REASONS = 3


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


def fit_envelope(X: pd.DataFrame) -> dict:
    """Learn, for each device type, the normal range of every reading it reports.

    X holds NORMAL data only: numeric columns plus one-hot "is_<type>" columns.
    Returns {"is_<type>": {column: [low, high]}}. Readings a device type never reports
    (constant in its rows, e.g. battery on a cardiac monitor) get no range.
    """
    type_cols = [c for c in X.columns if c.startswith("is_")]
    numeric = [c for c in X.columns if c not in type_cols]
    envelope = {}
    for type_col in type_cols:
        rows = X[X[type_col] == 1.0]
        if len(rows) < 100:
            continue                      # not enough data to say what normal looks like
        envelope[type_col] = {
            c: [float(v) for v in np.percentile(rows[c], ENVELOPE_PCTS)]
            for c in numeric if rows[c].nunique() > 1
        }
    return envelope


def envelope_findings(row: dict, envelope: dict) -> list:
    """Readings outside their device type's normal range, worst first.

    row: {column: value} for one message. Each finding has the feature, its value, the normal
    range and a risk in [ALERT_THRESHOLD, 100] that grows with the distance outside the range.
    """
    type_col = next((c for c, v in row.items() if c.startswith("is_") and v == 1.0), None)
    findings = []
    for column, (low, high) in envelope.get(type_col, {}).items():
        value = float(row[column])
        span = max(high - low, 1e-9)
        outside = (low - value) / span if value < low else (value - high) / span if value > high else 0.0
        if outside > 0:
            risk = ALERT_THRESHOLD + (100 - ALERT_THRESHOLD) * min(outside, 1.0)
            findings.append({"feature": column, "value": value, "normal_low": low,
                             "normal_high": high, "risk": risk})
    return sorted(findings, key=lambda f: -f["risk"])


def describe(finding: dict) -> str:
    return (f"{finding['feature']} = {finding['value']:g} "
            f"(normal {finding['normal_low']:g} to {finding['normal_high']:g})")


class RiskScorer:
    """A trained model + its column list + its calibration (+ envelope), loaded from one file."""

    def __init__(self, bundle: dict):
        self.model = bundle["model"]
        self.columns = list(bundle["feature_columns"])
        self.calibration = bundle["calibration"]
        self.envelope = bundle.get("envelope")      # absent in model files saved before the envelope existed

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

    def score_detail(self, features) -> dict:
        """One feature vector -> {"risk": 0-100, "forest_risk", "envelope_risk", "reasons": [str]}.

        reasons says why a message scored high: the readings outside their normal range, or,
        if the forest alone raised it, that the combination of readings is unusual.
        """
        X = pd.DataFrame([list(features)], columns=self.columns)
        forest = float(self.risk_scores(X)[0])
        findings = envelope_findings(X.iloc[0].to_dict(), self.envelope) if self.envelope else []
        envelope = max((f["risk"] for f in findings), default=0.0)
        reasons = [describe(f) for f in findings[:MAX_REASONS]]
        if not reasons and forest >= ALERT_THRESHOLD:
            reasons = ["unusual combination of readings (anomaly model)"]
        return {"risk": int(round(max(forest, envelope))), "forest_risk": forest,
                "envelope_risk": envelope, "reasons": reasons}

    def score_one(self, features) -> int:
        """One feature vector (list of floats) -> integer risk 0-100."""
        return self.score_detail(features)["risk"]