from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler

from risk_scoring import ALERT_THRESHOLD, MAX_REASONS, describe, envelope_findings, scores_to_risk

HIDDEN_LAYERS = (8, 4, 8)


def fit_autoencoder(X, seed: int = 42, hidden=HIDDEN_LAYERS, max_iter: int = 500):
    """Train on NORMAL rows only. Returns (scaler, model)."""
    arr = np.asarray(X, dtype=float)
    scaler = StandardScaler().fit(arr)
    scaled = scaler.transform(arr)
    model = MLPRegressor(
        hidden_layer_sizes=hidden, activation="relu", solver="adam", max_iter=max_iter,
        early_stopping=True, validation_fraction=0.1, n_iter_no_change=20, random_state=seed,
    )
    model.fit(scaled, scaled)          # the target is the input itself
    return scaler, model


class AutoencoderScorer:
    """A trained autoencoder + its scaler, column list, calibration (+ envelope), from one file."""

    def __init__(self, bundle: dict):
        self.scaler = bundle["scaler"]
        self.model = bundle["model"]
        self.columns = list(bundle["feature_columns"])
        self.calibration = bundle["calibration"]
        self.envelope = bundle.get("envelope")

    @classmethod
    def load(cls, path) -> "AutoencoderScorer":
        return cls(joblib.load(Path(path)))

    def _scaled(self, X) -> np.ndarray:
        arr = X[self.columns].to_numpy(dtype=float) if isinstance(X, pd.DataFrame) else np.asarray(X, dtype=float)
        return self.scaler.transform(arr)

    def _errors(self, scaled: np.ndarray) -> np.ndarray:
        """Squared rebuild error per message per column."""
        return (scaled - self.model.predict(scaled)) ** 2

    def anomaly_scores(self, X) -> np.ndarray:
        """Higher = rebuilt worse = more unusual."""
        return self._errors(self._scaled(X)).mean(axis=1)

    def risk_scores(self, X) -> np.ndarray:
        return scores_to_risk(self.anomaly_scores(X), self.calibration)

    def score_detail(self, features) -> dict:
        """One feature vector -> {"risk", "autoencoder_risk", "envelope_risk", "reasons"}."""
        X = pd.DataFrame([list(features)], columns=self.columns)
        err = self._errors(self._scaled(X))[0]
        ae = float(scores_to_risk([err.mean()], self.calibration)[0])
        findings = envelope_findings(X.iloc[0].to_dict(), self.envelope) if self.envelope else []
        envelope = max((f["risk"] for f in findings), default=0.0)
        reasons = [describe(f) for f in findings[:MAX_REASONS]]
        if not reasons and ae >= ALERT_THRESHOLD:
            worst = self.columns[int(np.argmax(err))]
            reasons = [f"unusual combination of readings; biggest mismatch in {worst} (autoencoder)"]
        return {"risk": int(round(max(ae, envelope))), "autoencoder_risk": ae,
                "envelope_risk": envelope, "reasons": reasons}

    def score_one(self, features) -> int:
        return self.score_detail(features)["risk"]
