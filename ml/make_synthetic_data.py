"""
STAND-IN data generator. NOT real data: it exists so the training code can be
tested before simulator output is available.

    python ml/make_synthetic_data.py
writes ml/data/synthetic/train_normal.jsonl and test.jsonl
(test rows carry _label 0/1 and _attack = flood | tamper | subtle).
"""
import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _base(rng, i, device_type):
    return {
        "message_id": f"syn-{i}", "device_id": f"{device_type}-{rng.randint(1, 5):02d}",
        "device_type": device_type, "unit": rng.choice(["ICU-3", "ER-1", "WARD-2"]),
        "timestamp": "2026-01-01T00:00:00Z",
        "packet_size_bytes": max(40, int(rng.gauss(400, 60))),
        "request_rate_per_min": max(1, int(rng.gauss(12, 3))),
    }


def _normal(rng, i):
    t = rng.choice(["infusion_pump", "cardiac_monitor", "pulse_oximeter"])
    r = _base(rng, i, t)
    if t == "infusion_pump":
        r.update(infusion_rate_ml_per_hr=rng.gauss(100, 15), occlusion_pressure_mmHg=rng.gauss(40, 8),
                 battery_pct=rng.uniform(30, 100))
    elif t == "cardiac_monitor":
        r.update(heart_rate_bpm=rng.gauss(75, 8), spo2_pct=min(100, rng.gauss(97, 1)),
                 systolic_bp_mmHg=rng.gauss(120, 8), diastolic_bp_mmHg=rng.gauss(80, 5))
    else:
        r.update(spo2_pct=min(100, rng.gauss(97, 1)), pulse_bpm=rng.gauss(75, 8))
    return r


def _attack(rng, i, kind):
    r = _normal(rng, i)
    if kind == "flood":
        r["packet_size_bytes"] = rng.randint(3000, 9000)
        r["request_rate_per_min"] = rng.randint(120, 600)
    elif kind == "tamper":
        if r["device_type"] == "infusion_pump":
            r["infusion_rate_ml_per_hr"] = rng.uniform(300, 600)
        elif r["device_type"] == "cardiac_monitor":
            r["heart_rate_bpm"] = rng.uniform(170, 220); r["spo2_pct"] = rng.uniform(60, 80)
        else:
            r["pulse_bpm"] = rng.uniform(170, 220); r["spo2_pct"] = rng.uniform(60, 80)
    else:  # subtle: small shifts that hide inside normal ranges
        r["request_rate_per_min"] = rng.randint(22, 35)
        if "heart_rate_bpm" in r: r["heart_rate_bpm"] += 25
        if "pulse_bpm" in r: r["pulse_bpm"] += 25
        if "infusion_rate_ml_per_hr" in r: r["infusion_rate_ml_per_hr"] += 60
    return r


def generate(n_normal=20000, n_test_normal=2000, n_test_attack=1000, seed=0):
    rng = random.Random(seed)
    train = [_normal(rng, i) for i in range(n_normal)]
    test = []
    for i in range(n_test_normal):
        r = _normal(rng, 10**6 + i); r["_label"] = 0; r["_attack"] = "none"; test.append(r)
    kinds = ["flood", "tamper", "subtle"]
    for i in range(n_test_attack):
        k = kinds[i % 3]
        r = _attack(rng, 2 * 10**6 + i, k); r["_label"] = 1; r["_attack"] = k; test.append(r)
    rng.shuffle(test)
    return train, test


if __name__ == "__main__":
    out = HERE / "data" / "synthetic"
    out.mkdir(parents=True, exist_ok=True)
    train, test = generate()
    for name, rows in (("train_normal.jsonl", train), ("test.jsonl", test)):
        with open(out / name, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(train)} normal and {len(test)} test rows to {out}")
