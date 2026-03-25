param(
  [string]$BrokerAddr = "127.0.0.1:9090",
  [int]$StartupTimeoutSeconds = 25
)

$ErrorActionPreference = "Stop"
$env:BROKER_ADDR = $BrokerAddr
$env:SMOKE_STARTUP_TIMEOUT_SECS = "$StartupTimeoutSeconds"

cargo run -p broker --example smoke_all
if ($LASTEXITCODE -ne 0) {
  throw "smoke workflow failed with exit code $LASTEXITCODE"
}
