$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$env:GOPROXY = "off"
$env:GOSUMDB = "off"

go run .\cmd\driftctl demo --out artifacts\demo
go run .\cmd\driftctl verify --out artifacts\verification.json
go run .\cmd\driftctl stress --count 20000 --fanout 6 --exact-limit 5000
