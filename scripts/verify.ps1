[CmdletBinding()]
param(
    [string]$DemoDir = ".demo",
    [string]$Summary = "artifacts/demo-summary.json",
    [string]$Report = "artifacts/rebuild-report.json"
)

$ErrorActionPreference = "Stop"

Write-Host "==> go version"
go version

Write-Host "==> go test ./... -count=1"
go test ./... -count=1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> pure-Go Windows build"
$env:CGO_ENABLED = "0"
go build -o sampleengine.exe .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> deterministic demo"
go run . demo --dir $DemoDir --summary $Summary --report $Report
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Expected summary_digest: f1580c2da67d48acaf675c6b444c315d394f80b1579a35bc7e19ee05c69ce9d7"
