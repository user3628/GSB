$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if ($args.Count -eq 0) {
    $Work = Join-Path $Root "artifacts\experiment"
    go run ./cmd/experiment -workdir $Work -clean
} else {
    go run ./cmd/experiment @args
}
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
