# MedGuard

A zero-trust monitoring pipeline for connected medical devices (IoMT). Simulated devices send telemetry to a hardened gateway, the data flows through Kafka for preprocessing and anomaly scoring, and a React dashboard shows alerts to two kinds of users: an admin (all devices, audit log, isolation) and nurses (only the devices assigned to them, with desktop notifications).

Course project for CSIS 4495 (Douglas College). Team: Jude Enuanwa (Team Lead) and David [LAST NAME].

> This is a prototype that uses simulated devices and synthetic patient data only. Sign-in is mocked and no real health data is handled.

## How it works

```
simulated devices
      |  HTTPS + bearer token
      v
gateway (FastAPI): auth, schema validation, config and prescription checks
      |
      v
Kafka: iomt.telemetry.raw
      |
      v
stream consumer: preprocessing, risk score  ->  Kafka: iomt.telemetry.scored
                                                      |
                                                      v
                                   dashboard-api (FastAPI + WebSocket)
                                   mock sign-in, role filtering, alerts, audit log
                                                      |
                                                      v
                                   React app: admin console / nurse station
```

### What is built

- **Gateway:** bearer-token authentication, Pydantic schema validation per device type, configuration checks (auto-lock off, default credentials, outdated firmware, TLS off), prescription-mismatch check for infusion pumps, TLS.
- **Simulator:** five devices with deliberate weaknesses and injected attacks, plus a ground-truth log for evaluating the model.
- **Stream:** preprocessing into a fixed numeric feature vector, published to a scored topic.
- **Dashboard API:** live device state and alerts over a WebSocket, de-duplicated alerts, mocked sign-in with admin and nurse roles enforced on the server, acknowledgement, isolate and restore buttons, and an audit log.
- **React frontend:** login page, admin console (overview, devices, alerts, audit log, access) and nurse station (assigned devices, plain-language alerts, desktop notifications).

### Not built yet

- The Isolation Forest and Autoencoder scores (the risk score is a placeholder until the model is integrated).
- Per-device authentication (the gateway uses one shared token and trusts `device_id`).
- Gateway enforcement of isolation (isolating a device is recorded and audited but not yet blocked).
- Postgres persistence (alerts and the audit log are held in memory and reset on restart).
- Real user accounts and single sign-on.

## Quick start (Windows)

### Prerequisites

| Tool | Notes |
|---|---|
| Docker Desktop | Must be **running** before you start. Check with `docker ps`. |
| Python 3.12+ | Runs the device simulator. |
| mkcert | Creates the local HTTPS certificate. Install: `winget install FiloSottile.mkcert`, then reopen the terminal. |
| Git | To clone the repo. |
| Node 20+ | Only needed if you edit the frontend or use `-Frontend`. |

### 1. Clone

```powershell
git clone https://github.com/Jud-e/F26_4495_S1_JudeE.git
cd F26_4495_S1_JudeE
```

### 2. Create the certificate (once per machine)

```powershell
mkcert -install
cd gateway
mkcert -key-file key.pem -cert-file cert.pem localhost 127.0.0.1
cd ..
```

`key.pem` and `cert.pem` are git-ignored, so each team member creates their own.

### 3. Install the simulator dependencies (once)

```powershell
pip install -r ingestion/requirements.txt
```

### 4. Run everything

```powershell
.\run-demo.ps1
```

The script checks mkcert, starts the Docker stack, waits for the gateway to be healthy, opens the dashboard and runs the simulator. Options:

| Option | Effect |
|---|---|
| `-Count 200` | Number of simulated messages (default 60) |
| `-AnomalyChance 0.3` | Fraction of messages that are injected attacks (default 0.2) |
| `-Clean` | Wipes Kafka and Postgres data first, for fresh alerts and a fresh audit log |
| `-Frontend` | Rebuilds the React app on your machine first (needs Node) |

The first run builds the Docker images, which takes a few minutes.

### 5. Open the dashboard

http://localhost:8100 and sign in with a demo account:

| Username | Password | Role | Sees |
|---|---|---|---|
| `admin` | `admin123` | Admin | All devices, alerts, audit log, access list, Isolate/Restore |
| `nurse.amara` | `nurse123` | Nurse | Infusion pumps and pulse oximeters |
| `nurse.ben` | `nurse123` | Nurse | Cardiac monitors |

The demo accounts are hard-coded in `dashboard-api/auth.py` (mocked authentication).

### What to try

1. Sign in as `admin` in one window and as a nurse in a private window. In the nurse window click **Enable notifications**.
2. Let the simulator run. New alerts appear for the admin, and only for the matching nurse, with a toast and a desktop notification.
3. As the nurse, **Acknowledge** an alert. As the admin, open **Audit log** and see the sign-ins, alert and acknowledgement.
4. As the admin, open **Devices** and click **Isolate** on a device. The action is recorded and audited (gateway enforcement is not connected yet).

## Services and ports

| Service | URL | Purpose |
|---|---|---|
| Dashboard | http://localhost:8100 | React app and dashboard API |
| Gateway | https://localhost:8000 | Ingestion API (docs at `/docs`) |
| Kafka UI | http://localhost:8085 | Browse topics and messages |
| Kafka (from your machine) | localhost:29092 | Used by the stream consumer when run outside Docker |

## Project structure

```
gateway/            FastAPI gateway (auth, validation, checks, TLS)
ingestion/          Device simulator and ground-truth log
stream/             Preprocessing and the Kafka consumer
ml/                 Model code (Isolation Forest in progress)
dashboard-api/      Dashboard API (main.py, auth.py) and React app
  frontend/         Vite + React + TypeScript source
data/               Patient registry (synthetic)
docker-compose.yml  The whole stack
run-demo.ps1        One-command start
```

## Running the steps manually

```powershell
docker compose up --build -d
$env:REQUESTS_CA_BUNDLE = "$(mkcert -CAROOT)\rootCA.pem"
python ingestion/simulate_devices.py --count 60 --anomaly-chance 0.2
```

`REQUESTS_CA_BUNDLE` makes Python trust the mkcert certificate. It is set per terminal, or permanently with `setx`.

## Resetting data

```powershell
docker compose down -v
docker compose up --build -d
```

`-v` deletes the Kafka and Postgres volumes. The dashboard replays the scored topic from the start each time it launches, so old messages from earlier runs come back as alerts unless you reset.

## Developing the frontend

```powershell
cd dashboard-api\frontend
npm install
npm run dev          # http://localhost:5173, forwards /api and /ws to localhost:8100
npm run typecheck    # TypeScript check
npm run build        # writes dist/, normally built by the Docker image instead
```

Keep the Docker stack running while you use `npm run dev`. After frontend changes, `docker compose up --build -d dashboard-api` rebuilds the image.

## Tests

The dashboard API has an automated test file. From the folder that contains `dashboard-api/`:

```powershell
pip install -r dashboard-api/requirements.txt httpx
python test_dash.py
```

It checks sign-in, token rejection, nurse versus admin filtering, alert de-duplication, acknowledgement permissions, isolation and the audit log. Preprocessing has its own unit tests under `stream/`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `failed to connect to the docker API ... dockerDesktopLinuxEngine` | Docker Desktop is not running. Start it and wait for the engine. |
| Simulator prints `CONN-ERROR` | `REQUESTS_CA_BUNDLE` is not set in this terminal, or the stack is not up yet. |
| `mkcert` is not recognised | Install it with winget and open a new terminal. |
| Dashboard says "Frontend not built" | Rebuild the image with `docker compose up --build -d dashboard-api`, or run `npm run build` in `dashboard-api/frontend` and restart the container. |
| Nurse page shows no devices | The device patterns in `dashboard-api/auth.py` must match the simulator's device IDs. |
| Old alerts on first load | Kafka kept earlier messages. Reset with `.\run-demo.ps1 -Clean`. |

## TLS

The gateway runs uvicorn with `--ssl-keyfile=/app/key.pem --ssl-certfile=/app/cert.pem`, using certificates made by mkcert (step 2). If a merge ever drops these flags from the `command:` line of the gateway service in `docker-compose.yml`, the gateway silently falls back to plain HTTP. See issue #4.

## Security notes and limitations

- Sign-in is mocked: accounts and passwords are hard-coded and sessions live in memory.
- The gateway uses one shared bearer token and trusts the `device_id` a client reports, so it cannot yet prove which device sent a message. Per-device identity (tokens or mTLS) is planned.
- Isolation is recorded and audited but not yet enforced at the gateway.
- Alerts and the audit log are in memory and reset when the dashboard service restarts.
- Only synthetic data is used. Handling real health data would add privacy requirements (PIPEDA, BC PIPA) that this prototype does not address.

## Documents

Proposal and progress reports are in `DocumentsAndReports/`.