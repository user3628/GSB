$ErrorActionPreference = "Stop"
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    go test ./... -count=1
    New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    go build -o "bin\recallab.exe" ".\cmd\recallab"
    $verifyDir = Join-Path "tmp" ("verify-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force -Path $verifyDir | Out-Null
    & ".\bin\recallab.exe" generate --out (Join-Path $verifyDir "data") --seed 99 --items 200 --queries 40 --vector-dim 16
    & ".\bin\recallab.exe" run --data (Join-Path $verifyDir "data") --db (Join-Path $verifyDir "process.db") --out (Join-Path $verifyDir "process.json") --workers 3 --mode process
    & ".\bin\recallab.exe" run --data (Join-Path $verifyDir "data") --db (Join-Path $verifyDir "inline.db") --out (Join-Path $verifyDir "inline.json") --workers 1 --mode inline
    $processHash = (Get-FileHash (Join-Path $verifyDir "process.json") -Algorithm SHA256).Hash
    $inlineHash = (Get-FileHash (Join-Path $verifyDir "inline.json") -Algorithm SHA256).Hash
    if ($processHash -ne $inlineHash) { throw "process/inline output mismatch" }
    Write-Host "Verification passed; process and inline output SHA256=$processHash"
}
finally {
    Pop-Location
}
