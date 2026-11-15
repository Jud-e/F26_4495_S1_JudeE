# Detection evaluation (simulator data)

Reproduce (from the repo root; use Python 3.12 with the pins in stream/requirements.txt so the saved model loads in Docker):

    python ml/make_simulator_data.py
    python ml/train_isolation_forest.py --input ml/data/simulator/train_normal.jsonl --test ml/data/simulator/test.jsonl

Data: 20,000 normal messages for training; a test set of 3,000 normal and 1,500 attack messages (500 each of
out_of_range_vital, traffic_flood, prescription_mismatch), all produced by `ingestion/simulate_devices.py`.
`payload_tamper` is excluded because the gateway rejects it (HTTP 422) before it reaches the model.
Alert threshold: risk >= 70.

| Detector | Precision | Recall | False-alarm rate | out_of_range_vital | traffic_flood | prescription_mismatch |
|---|---|---|---|---|---|---|
| Isolation Forest only | 0.870 | 0.170 | 1.27% | 0.096 | 0.308 | 0.106 |
| Forest + envelope (live) | 0.973 | 1.000 | 1.40% | 1.000 | 1.000 | 1.000 |

Forest ROC AUC: 0.866.

## What this shows

- An Isolation Forest only splits inside the range of its normal training data, so a reading far *outside* that
  range is barely easier to isolate than one at the edge. On this simulator (independent, uniformly distributed
  readings) it catches few attacks even though they are extreme.
- The envelope adds a per-device-type "normal range" for each reading (p0.1 to p99.9 of normal data). Anything
  outside scores at least 70 and rises to 100 at one full normal-range-width outside. It also reports *which*
  reading was out of range, which the dashboard shows as the reason.
- The forest is kept for odd *combinations* of individually normal readings. The simulator's readings are
  independent, so it has no such combinations to find here, and it contributes mostly false alarms (about 1% by
  design, from calibrating p99 of normal to 70). On correlated real data (see the CICIDS2017 benchmark) it is the
  stronger signal.

## Limits

- Evaluation is on simulated attacks that are all large departures. Subtle attacks that stay inside normal ranges
  are not tested and would be missed by the envelope.
- The simulator's normal ranges define "normal" here. Real devices would need a model trained on their own data.
