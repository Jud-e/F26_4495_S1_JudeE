# Starts the whole stack, opens the dashboard, and sends simulator traffic.
# Run from the project root:  .\run-demo.ps1   (or .\run-demo.ps1 -Count 200)
#   -Clean     wipe Kafka/Postgres data first (fresh alerts, fresh audit log)
#   -Frontend  rebuild the React app on this machine first (needs Node)
param([int]$Count = 60, [double]$AnomalyChance = 0.2, [switch]$Clean, [switch]$Frontend)
$ErrorActionPreference = "Stop"

if (-not (Get-Command mkcert -ErrorAction SilentlyContinue)) {
    throw "mkcert not found. See the TLS setup section of the README."
}
$env:REQUESTS_CA_BUNDLE = Join-Path (mkcert -CAROOT) "rootCA.pem"

if ($Frontend) {
    Push-Location dashboard-api\frontend
    npm install --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { Pop-Location; throw "npm install failed" }
    npm run build
    if ($LASTEXITCODE -ne 0) { Pop-Location; throw "npm run build failed" }
    Pop-Location
}

if ($Clean) {
    docker compose down -v
    if ($LASTEXITCODE -ne 0) { throw "docker compose down failed. Is Docker Desktop running?" }
}

docker compose up --build -d
if ($LASTEXITCODE -ne 0) { throw "docker compose failed. Is Docker Desktop running?" }

Write-Host "Waiting for the gateway..."
$ready = $false
for ($i = 0; $i -lt 60 -and -not $ready; $i++) {
    try { Invoke-WebRequest https://localhost:8000/health -UseBasicParsing | Out-Null; $ready = $true }
    catch { Start-Sleep -Seconds 2 }
}
if (-not $ready) { throw "Gateway did not come up. Check: docker compose logs gateway" }

Start-Process "http://localhost:8100"
python ingestion/simulate_devices.py --count $Count --anomaly-chance $AnomalyChance