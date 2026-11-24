"""
Train the Autoencoder on NORMAL telemetry and compare it with the Isolation Forest.

    python ml/make_simulator_data.py
    python ml/train_isolation_forest.py --input ml/data/simulator/train_normal.jsonl
    python ml/train_autoencoder.py --input ml/data/simulator/train_normal.jsonl --test ml/data/simulator/test.jsonl

--input   .jsonl of normal messages (same file the forest trained on)
--test    .jsonl with _label (0/1) and _attack, to evaluate on
--forest-model  the trained forest, so both can be scored on the SAME test set (skipped if missing)

Saves ml/models/autoencoder.joblib and, with --test, writes into ml/results/:
    comparison.md      the table for the report
    roc_curves.png     ROC curves for every detector (needs matplotlib; skipped if missing)

Detectors compared (alert threshold: risk >= 70):
    Isolation Forest | Isolation Forest + range check (the live one)
    Autoencoder      | Autoencoder + range check
"""
import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, roc_curve

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "stream"))
sys.path.insert(0, str(HERE))

from autoencoder import AutoencoderScorer, fit_autoencoder  # noqa: E402
from preprocessing import FEATURE_COLUMNS  # noqa: E402
from risk_scoring import ALERT_THRESHOLD, RiskScorer, envelope_findings, fit_calibration, fit_envelope, scores_to_risk  # noqa: E402
from train_isolation_forest import load_vectors  # noqa: E402

DEFAULT_OUT = HERE / "models" / "autoencoder.joblib"
DEFAULT_FOREST = HERE / "models" / "isolation_forest.joblib"
FPR_POINTS = (0.01, 0.05, 0.10)


def envelope_risks(X: pd.DataFrame, envelope) -> np.ndarray:
    out = []
    for row in X.to_dict("records"):
        findings = envelope_findings(row, envelope)
        out.append(max((f["risk"] for f in findings), default=0.0))
    return np.array(out)


def evaluate(name, score, risk, y, attack):
    """score = continuous anomaly measure (for ROC); risk = 0-100 used for the alert line."""
    flagged = risk >= ALERT_THRESHOLD
    p, r, f1, _ = precision_recall_fscore_support(y, flagged, average="binary", zero_division=0)
    fpr, tpr, _ = roc_curve(y, score)
    tp, fp = int((flagged & (y == 1)).sum()), int((flagged & (y == 0)).sum())
    fn, tn = int((~flagged & (y == 1)).sum()), int((~flagged & (y == 0)).sum())
    return {
        "name": name, "auc": roc_auc_score(y, score), "precision": p, "recall": r, "f1": f1,
        "far": fp / max(fp + tn, 1), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "tpr_at": {f: float(np.interp(f, fpr, tpr)) for f in FPR_POINTS},
        "by_attack": {a: float(flagged[(attack == a) & (y == 1)].mean()) for a in sorted(set(attack[y == 1]))},
        "roc": (fpr, tpr),
    }


def write_report(results, out_dir: Path, n_norm, n_att):
    out_dir.mkdir(parents=True, exist_ok=True)
    attacks = sorted({a for r in results for a in r["by_attack"]})
    lines = [f"Test set: {n_norm:,} normal and {n_att:,} attack messages. Alert threshold: risk >= {ALERT_THRESHOLD}.", "",
             "| Detector | AUC | Precision | Recall | F1 | False-alarm rate | Recall at 1% FPR | at 5% FPR | at 10% FPR |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in results:
        t = r["tpr_at"]
        lines.append(f"| {r['name']} | {r['auc']:.3f} | {r['precision']:.3f} | {r['recall']:.3f} | {r['f1']:.3f} | "
                     f"{r['far']:.2%} | {t[0.01]:.3f} | {t[0.05]:.3f} | {t[0.10]:.3f} |")
    lines += ["", "Recall by attack type at the alert line:", "",
              "| Detector | " + " | ".join(attacks) + " |", "| --- |" + " --- |" * len(attacks)]
    for r in results:
        lines.append(f"| {r['name']} | " + " | ".join(f"{r['by_attack'][a]:.3f}" for a in attacks) + " |")
    lines += ["", "Confusion matrices (TP / FP / FN / TN):", ""]
    for r in results:
        lines.append(f"- {r['name']}: {r['tp']} / {r['fp']} / {r['fn']} / {r['tn']}")
    (out_dir / "comparison.md").write_text("\n".join(lines) + "\n")
    print(f"Wrote {out_dir / 'comparison.md'}")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, so no ROC picture. Install it with: uv pip install matplotlib")
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6))
    for ax, xmax, title in ((axes[0], 1.0, "ROC curves"), (axes[1], 0.1, "Zoom: false-alarm rate up to 10%")):
        for r in results:
            ax.plot(*r["roc"], label=f"{r['name']} (AUC {r['auc']:.3f})", linewidth=1.8)
        ax.plot([0, 1], [0, 1], "--", color="grey", linewidth=1)
        ax.set_xlim(0, xmax); ax.set_ylim(0, 1.02)
        ax.set_xlabel("False-alarm rate (normal messages flagged)"); ax.set_ylabel("Recall (attacks caught)")
        ax.set_title(title); ax.grid(alpha=0.3)
    axes[0].legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "roc_curves.png", dpi=150)
    print(f"Wrote {out_dir / 'roc_curves.png'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--test", type=Path)
    ap.add_argument("--forest-model", type=Path, default=DEFAULT_FOREST)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--results-dir", type=Path, default=HERE / "results")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    X, _ = load_vectors(args.input)
    if len(X) < 500:
        print(f"WARNING: only {len(X)} training rows; results will be rough.")
    Xs = X.sample(frac=1.0, random_state=args.seed).reset_index(drop=True)
    n_fit = int(len(Xs) * 0.8)
    fit, cal = Xs.iloc[:n_fit], Xs.iloc[n_fit:]
    print(f"Fit on {len(fit):,} rows, calibrate on {len(cal):,} held-out rows")

    scaler, model = fit_autoencoder(fit, seed=args.seed)
    print(f"Autoencoder trained: {model.n_iter_} passes over the data, final training loss {model.loss_:.4f}")
    probe = AutoencoderScorer({"scaler": scaler, "model": model, "feature_columns": FEATURE_COLUMNS,
                               "calibration": {"values": [0, 1, 2, 3], "risks": [0, 30, 70, 100]}})
    calibration = fit_calibration(probe.anomaly_scores(cal))
    envelope = fit_envelope(X)
    bundle = {"scaler": scaler, "model": model, "feature_columns": FEATURE_COLUMNS,
              "calibration": calibration, "envelope": envelope}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, args.out)
    print(f"Saved model to {args.out}")
    if not args.test:
        return

    ae = AutoencoderScorer(bundle)
    Xt, recs = load_vectors(args.test)
    y = np.array([r["_label"] for r in recs])
    attack = np.array([r.get("_attack", "?") for r in recs])
    ae_score = ae.anomaly_scores(Xt)
    ae_risk = scores_to_risk(ae_score, calibration)
    env_risk = envelope_risks(Xt, envelope)

    results = []
    if args.forest_model.exists():
        forest = RiskScorer.load(args.forest_model)
        f_score = forest.anomaly_scores(Xt)
        f_risk = forest.risk_scores(Xt)
        f_env = envelope_risks(Xt, forest.envelope) if forest.envelope else np.zeros(len(Xt))
        results += [evaluate("Isolation Forest", f_score, f_risk, y, attack),
                    evaluate("Isolation Forest + range check", np.maximum(f_risk, f_env), np.maximum(f_risk, f_env), y, attack)]
    else:
        print(f"(no forest model at {args.forest_model}; comparing the autoencoder alone)")
    results += [evaluate("Autoencoder", ae_score, ae_risk, y, attack),
                evaluate("Autoencoder + range check", np.maximum(ae_risk, env_risk), np.maximum(ae_risk, env_risk), y, attack)]

    print(f"\n=== Evaluation on {int((y == 0).sum()):,} normal and {int((y == 1).sum()):,} attack messages (alert: risk >= {ALERT_THRESHOLD}) ===")
    for r in results:
        t = r["tpr_at"]
        print(f"\n[{r['name']}]")
        print(f"  AUC {r['auc']:.3f}   Precision/Recall/F1 {r['precision']:.3f} / {r['recall']:.3f} / {r['f1']:.3f}   False-alarm {r['far']:.2%}")
        print(f"  Recall at 1% / 5% / 10% false alarms: {t[0.01]:.3f} / {t[0.05]:.3f} / {t[0.10]:.3f}")
        print("  Recall by attack type: " + ", ".join(f"{a} {v:.3f}" for a, v in r["by_attack"].items()))
        print(f"  Confusion  TP={r['tp']}  FP={r['fp']}  FN={r['fn']}  TN={r['tn']}")
    print()
    write_report(results, args.results_dir, int((y == 0).sum()), int((y == 1).sum()))


if __name__ == "__main__":
    main()
