Test set: 3,000 normal and 1,500 attack messages. Alert threshold: risk >= 70.

| Detector | AUC | Precision | Recall | F1 | False-alarm rate | Recall at 1% FPR | at 5% FPR | at 10% FPR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Isolation Forest | 0.866 | 0.870 | 0.170 | 0.284 | 1.27% | 0.138 | 0.372 | 0.540 |
| Isolation Forest + range check | 0.999 | 0.973 | 1.000 | 0.987 | 1.37% | 1.000 | 1.000 | 1.000 |
| Autoencoder | 0.926 | 0.972 | 0.741 | 0.841 | 1.07% | 0.741 | 0.770 | 0.795 |
| Autoencoder + range check | 1.000 | 0.977 | 1.000 | 0.988 | 1.17% | 1.000 | 1.000 | 1.000 |

Recall by attack type at the alert line:

| Detector | out_of_range_vital | prescription_mismatch | traffic_flood |
| --- | --- | --- | --- |
| Isolation Forest | 0.096 | 0.106 | 0.308 |
| Isolation Forest + range check | 1.000 | 1.000 | 1.000 |
| Autoencoder | 1.000 | 0.222 | 1.000 |
| Autoencoder + range check | 1.000 | 1.000 | 1.000 |

Confusion matrices (TP / FP / FN / TN):

- Isolation Forest: 255 / 38 / 1245 / 2962
- Isolation Forest + range check: 1500 / 41 / 0 / 2959
- Autoencoder: 1111 / 32 / 389 / 2968
- Autoencoder + range check: 1500 / 35 / 0 / 2965
