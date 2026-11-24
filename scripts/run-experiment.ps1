$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root
$Out = if ($env:OUT) { $env:OUT } else { "artifacts/run" }
$Bin = if ($env:BIN) { $env:BIN } else { "bin/authlab.exe" }

go build -trimpath -o $Bin ./cmd/authlab
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $Bin experiment --out $Out @args
exit $LASTEXITCODE
