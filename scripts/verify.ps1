$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$work = $env:VPNBENCH_WORK_DIR
if ([string]::IsNullOrWhiteSpace($work)) {
    $work = Join-Path $env:TEMP ("vpnbench-" + [guid]::NewGuid().ToString("N"))
}
New-Item -ItemType Directory -Force -Path $work | Out-Null
New-Item -ItemType Directory -Force -Path .\validation | Out-Null

$env:CGO_ENABLED = "0"
go build -trimpath -o (Join-Path $work "vpnbench.exe") .\cmd\vpnbench
$binary = Join-Path $work "vpnbench.exe"

& $binary generate --out .\samples
& $binary run `
    --config .\samples\configs\workbench.json `
    --db (Join-Path $work "workbench.db") `
    --report .\validation\workbench-report.json
& $binary verify `
    --samples .\samples `
    --work (Join-Path $work "verify") `
    --out .\validation\acceptance-report.json

Write-Output "reports: validation\workbench-report.json validation\acceptance-report.json"
