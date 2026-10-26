"""
Train the telemetry Isolation Forest on NORMAL messages only.

    python ml/train_isolation_forest.py --input ml/data/synthetic/train_normal.jsonl \
                                        --test  ml/data/synthetic/test.jsonl

--input  .jsonl of normal telemetry (one raw gateway message per line)
--test   optional .jsonl with _label (0 normal / 1 attack) and _attack, for evaluation

Saves ml/models/isolation_forest.joblib = {model, feature_columns, calibration}.
Uses the SAME preprocess() as the live consumer, so training and scoring match.
"""
import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "stream"))
sys.path.insert(0, str(HERE))

from preprocessing import FEATURE_COLUMNS, PreprocessingError, preprocess  # noqa: E402
from risk_scoring import ALERT_THRESHOLD, fit_calibration, scores_to_risk  # noqa: E402

DEFAULT_OUT = HERE / "models" / "isolation_forest.joblib"


def load_vectors(path):
    """Read a .jsonl and return (feature DataFrame, the raw records that survived)."""
    rows, kept, skipped = [], [], 0
    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            try:
                rows.append(preprocess(rec)[1]); kept.append(rec)
            except PreprocessingError:
                skipped += 1
    if skipped:
        print(f"  skipped {skipped} unusable record(s) in {path}")
    return pd.DataFrame(rows, columns=FEATURE_COLUMNS), kept


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--test", type=Path)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--n-estimators", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    X, _ = load_vectors(args.input)
    if len(X) < 500:
        print(f"WARNING: only {len(X)} training rows; percentiles will be rough.")
    X = X.sample(frac=1.0, random_state=args.seed).reset_index(drop=True)
    n_fit = int(len(X) * 0.8)
    fit, cal = X.iloc[:n_fit], X.iloc[n_fit:]        # calibrate on rows the model never saw
    print(f"Fit on {len(fit):,} rows, calibrate on {len(cal):,} held-out rows")

    model = IsolationForest(n_estimators=args.n_estimators, contamination="auto",
                            random_state=args.seed, n_jobs=-1).fit(fit)
    calibration = fit_calibration(-model.score_samples(cal))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "feature_columns": FEATURE_COLUMNS, "calibration": calibration}, args.out)
    print(f"Saved model to {args.out}")

    if args.test:
        Xt, recs = load_vectors(args.test)
        y = np.array([r["_label"] for r in recs])
        attack = np.array([r.get("_attack", "?") for r in recs])
        anomaly = -model.score_samples(Xt)
        flagged = scores_to_risk(anomaly, calibration) >= ALERT_THRESHOLD
        p, r_, f1, _ = precision_recall_fscore_support(y, flagged, average="binary", zero_division=0)
        fp, tn = int((flagged & (y == 0)).sum()), int((~flagged & (y == 0)).sum())
        print(f"\n=== Evaluation (alert threshold: risk >= {ALERT_THRESHOLD}) ===")
        print(f"ROC AUC                    {roc_auc_score(y, anomaly):.3f}")
        print(f"Precision / Recall / F1    {p:.3f} / {r_:.3f} / {f1:.3f}")
        print(f"False-alarm rate           {fp / max(fp + tn, 1):.2%}")
        print("Recall by attack type:")
        for a in sorted(set(attack[y == 1])):
            print(f"  {a:10s} {flagged[(attack == a) & (y == 1)].mean():.3f}")
        print("Recall by device type is worth checking too: a shared model is weakest on cardiac monitors.")


if __name__ == "__main__":
    main()
