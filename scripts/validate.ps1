$ErrorActionPreference = "Stop"
if (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root

$env:GOPATH = Join-Path $root ".gopath"
$env:GOMODCACHE = Join-Path $root ".gomodcache"

Write-Host "== Go version =="
go version

Write-Host "== Download modules (first run only) =="
go mod download

Write-Host "== go test ./... =="
go test ./... -count=1
if ($LASTEXITCODE -ne 0) { throw "go test failed" }

Write-Host "== go vet ./... =="
go vet ./...
if ($LASTEXITCODE -ne 0) { throw "go vet failed" }

$runDir = Join-Path ([System.IO.Path]::GetTempPath()) ("openplatform-validate-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $runDir | Out-Null
try {
    $binaryName = if ($IsWindows -or $env:OS -eq "Windows_NT") { "openplatform.exe" } else { "openplatform" }
    $binary = Join-Path $runDir $binaryName

    Write-Host "== build CLI =="
    go build -o $binary ./cmd/openplatform
    if ($LASTEXITCODE -ne 0) { throw "go build failed" }

    Write-Host "== deterministic demo =="
    $demoA = Join-Path $runDir "demo-a.json"
    $demoB = Join-Path $runDir "demo-b.json"
    & $binary demo --db (Join-Path $runDir "demo-a.db") --reset | Set-Content -NoNewline $demoA
    if ($LASTEXITCODE -ne 0) { throw "demo A failed" }
    & $binary demo --db (Join-Path $runDir "demo-b.db") --reset | Set-Content -NoNewline $demoB
    if ($LASTEXITCODE -ne 0) { throw "demo B failed" }

    $bytesA = [System.IO.File]::ReadAllBytes($demoA)
    $bytesB = [System.IO.File]::ReadAllBytes($demoB)
    if ([Convert]::ToBase64String($bytesA) -ne [Convert]::ToBase64String($bytesB)) {
        throw "deterministic demo outputs differ"
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = [BitConverter]::ToString($sha.ComputeHash($bytesA)).Replace("-", "").ToLowerInvariant()
    } finally {
        $sha.Dispose()
    }
    Write-Host "demo byte-identical sha256=$hash"

    $expectedCodes = [ordered]@{
        "auth_code_expired"      = "E_INVALID_GRANT"
        "pkce_mismatch"          = "E_PKCE_MISMATCH"
        "refresh_token_replay"   = "E_TOKEN_REUSE"
        "precision_overflow"     = "E_PRECISION_OVERFLOW"
        "snapshot_version_jump"  = "E_SNAPSHOT_VERSION"
        "quota_exhausted"        = "E_QUOTA_EXHAUSTED"
    }

    Write-Host "== fault injection =="
    foreach ($fault in $expectedCodes.Keys) {
        $output = & $binary fault-demo --db (Join-Path $runDir "fault-$fault.db") --reset --fault $fault 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0) { throw "fault-demo $fault unexpectedly succeeded" }
        $expected = $expectedCodes[$fault]
        if ($output -notmatch [regex]::Escape($expected)) {
            throw "fault-demo $fault did not emit $expected`n$output"
        }
        Write-Host ("{0,-24} {1}" -f $fault, $expected)
    }

    Write-Host "VALIDATION_OK"
} finally {
    Remove-Item -Recurse -Force $runDir -ErrorAction SilentlyContinue
}
