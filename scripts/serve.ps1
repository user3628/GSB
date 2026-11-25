[CmdletBinding()]
param(
    [string]$Addr = "127.0.0.1:18761"
)

$ErrorActionPreference = "Stop"
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    go build -o "bin\recallab.exe" ".\cmd\recallab"
    Write-Host "Health: http://$Addr/healthz"
    Write-Host "Run endpoint: POST http://$Addr/v1/runs"
    & ".\bin\recallab.exe" serve --addr $Addr
}
finally {
    Pop-Location
}
