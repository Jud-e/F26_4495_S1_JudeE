"""
Train and evaluate an Isolation Forest on CICIDS2017 network-flow data.

This is a SEPARATE model from the telemetry one (train_isolation_forest.py):
CICIDS2017 has 78 network-flow columns, not the 12 telemetry features the live
pipeline produces, so a model trained here cannot score a live telemetry message.
It exists to benchmark the algorithm against REAL labelled attacks for the
evaluation section of the report. It saves to its own file so the two can't be mixed up.

Uses Jude's ml/load_dataset.py (load_cicids2017 + split_train_eval).

How the data is split (and why)
-------------------------------
  benign rows (split_train_eval's train_df), shuffled, then:
      60%  fit the forest
      20%  calibrate the 0-100 risk mapping
      20%  HELD-OUT benign test rows -> honest false-alarm rate
  attack rows (eval_df where is_attack == 1) -> test
split_train_eval's eval_df contains the SAME benign rows the model trained on, and a
model scores data it has seen as slightly more normal, which would flatter the
false-alarm rate. So we evaluate on benign rows the model never saw.

Usage (from the project root):
    python ml/train_cicids2017.py
    python ml/train_cicids2017.py --max-fit-rows 50000 --seed 7
"""
import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from load_dataset import load_cicids2017, split_train_eval  # noqa: E402
from risk_scoring import ALERT_THRESHOLD, fit_calibration, scores_to_risk  # noqa: E402

DEFAULT_MODEL_PATH = HERE / "models" / "isolation_forest_cicids2017.joblib"


def to_clean_float32(df: pd.DataFrame, feature_cols, name: str):
    """Force feature columns to float32 and drop any row with inf/NaN.

    load_cicids2017() already drops bad rows in two columns, but its
    replace(inf -> NA) leaves the columns it touches as `object` dtype, and an
    inf in any OTHER column would slip through and make scikit-learn raise.
    """
    X = df[feature_cols].apply(pd.to_numeric, errors="coerce")
    X = X.replace([np.inf, -np.inf], np.nan)
    ok = X.notna().all(axis=1)
    dropped = int((~ok).sum())
    if dropped:
        print(f"  {name}: dropped {dropped:,} rows with inf/NaN/non-numeric values")
    return X.loc[ok].astype("float32"), ok.to_numpy()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=DEFAULT_MODEL_PATH)
    ap.add_argument("--n-estimators", type=int, default=200)
    ap.add_argument("--max-fit-rows", type=int, default=100_000,
                    help="cap on rows used to fit (each tree only sees 256 rows by default, "
                         "so more rows cost memory but add little)")
    ap.add_argument("--max-benign-test-rows", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = load_cicids2017()
    train_df, eval_df = split_train_eval(df)

    feature_cols = list(train_df.columns)   # Label already removed
    print(f"\nUsing {len(feature_cols)} flow features.")

    print("Cleaning:")
    benign, _ = to_clean_float32(train_df, feature_cols, "benign")
    if len(benign) < 1000:
        sys.exit("Almost no usable benign rows after cleaning. If the CSVs have text columns "
                 "(Flow ID, IP addresses, Timestamp), you downloaded the 'TrafficLabelling' version. "
                 "Use MachineLearningCSV.zip instead -- it is purely numeric.")
    attacks_raw = eval_df[eval_df["is_attack"] == 1]
    attacks, ok_mask = to_clean_float32(attacks_raw, feature_cols, "attacks")
    attack_labels = attacks_raw["Label"].to_numpy()[ok_mask]
    del df, train_df, eval_df, attacks_raw   # free memory; the CSVs are large

    benign = benign.sample(frac=1.0, random_state=args.seed).reset_index(drop=True)
    n = len(benign)
    n_fit, n_cal = int(n * 0.6), int(n * 0.2)
    fit = benign.iloc[:n_fit].head(args.max_fit_rows)
    cal = benign.iloc[n_fit:n_fit + n_cal]
    test_benign = benign.iloc[n_fit + n_cal:].head(args.max_benign_test_rows)
    print(f"\nBenign split: fit={len(fit):,}  calibrate={len(cal):,}  held-out test={len(test_benign):,}")
    print(f"Attack test rows: {len(attacks):,}")

    # BENIGN only: the big attack classes aren't rare, so training on them would teach it they're normal.
    model = IsolationForest(n_estimators=args.n_estimators, contamination="auto",
                            random_state=args.seed, n_jobs=-1).fit(fit)

    calibration = fit_calibration(-model.score_samples(cal))

    X_test = pd.concat([test_benign, attacks], ignore_index=True)
    y = np.r_[np.zeros(len(test_benign), dtype=int), np.ones(len(attacks), dtype=int)]
    labels = np.r_[np.array(["BENIGN"] * len(test_benign)), attack_labels]
    anomaly = -model.score_samples(X_test)
    flagged = scores_to_risk(anomaly, calibration) >= ALERT_THRESHOLD

    auc = roc_auc_score(y, anomaly)
    p, r, f1, _ = precision_recall_fscore_support(y, flagged, average="binary", zero_division=0)
    tp, fp = int((flagged & (y == 1)).sum()), int((flagged & (y == 0)).sum())
    fn, tn = int((~flagged & (y == 1)).sum()), int((~flagged & (y == 0)).sum())

    print(f"\n=== Evaluation (alert threshold: risk >= {ALERT_THRESHOLD}) ===")
    print(f"ROC AUC                      {auc:.3f}")
    print(f"Precision / Recall / F1      {p:.3f} / {r:.3f} / {f1:.3f}")
    print(f"False-alarm rate (benign)    {fp / max(fp + tn, 1):.2%}")
    print(f"Confusion  TP={tp:,}  FP={fp:,}  FN={fn:,}  TN={tn:,}")
    print("\nRecall by attack type (share flagged as risk >= %d):" % ALERT_THRESHOLD)
    rows = []
    for lab in sorted(set(attack_labels)):
        m = labels == lab
        sel = m | (y == 0)   # this attack vs held-out benign: ranks separation even below the threshold
        rows.append((lab, int(m.sum()), flagged[m].mean(), roc_auc_score(y[sel], anomaly[sel])))
    print(f"  {'attack':34s} {'rows':>9s} {'recall':>8s} {'AUC':>7s}")
    for lab, cnt, rec, a in sorted(rows, key=lambda t: -t[2]):
        print(f"  {lab:34s} {cnt:9,d} {rec:8.3f} {a:7.3f}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "feature_columns": feature_cols, "calibration": calibration}, args.out)
    print(f"\nSaved model to {args.out}")


if __name__ == "__main__":
    main()
