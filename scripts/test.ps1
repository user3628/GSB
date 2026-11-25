$ErrorActionPreference = "Stop"
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    go test ./... -count=1
    New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    go build -o "bin\recallab.exe" ".\cmd\recallab"
    Write-Host "All tests and build completed."
}
finally {
    Pop-Location
}
