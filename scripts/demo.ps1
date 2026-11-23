$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("blocksnap-e2e-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Push-Location $Root
    go build -o (Join-Path $Tmp "blocksnapd.exe") ./cmd/blocksnapd
    if ($LASTEXITCODE -ne 0) { throw "blocksnapd build failed" }
    go build -o (Join-Path $Tmp "blocksnap-demo.exe") ./cmd/demo
    if ($LASTEXITCODE -ne 0) { throw "demo build failed" }
    & (Join-Path $Tmp "blocksnap-demo.exe") -server-bin (Join-Path $Tmp "blocksnapd.exe") -workdir (Join-Path $Tmp "data")
    if ($LASTEXITCODE -ne 0) { throw "demo exited with $LASTEXITCODE" }
}
finally {
    Pop-Location
    Remove-Item -Recurse -Force $Tmp
}
