param(
    [string]$DbPath = "data\kill-test.db",
    [int]$Port = 18085,
    [string]$SampleDir = "samples",
    [int]$PauseSeconds = 120
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$exe = Join-Path $root "bin\meshpolicy.exe"
$logDir = Join-Path $root "data\logs"

if ([System.IO.Path]::IsPathRooted($DbPath)) { $dbFull = [System.IO.Path]::GetFullPath($DbPath) } else { $dbFull = [System.IO.Path]::GetFullPath((Join-Path $root $DbPath)) }
$dbParent = [System.IO.Path]::GetDirectoryName($dbFull)
New-Item -ItemType Directory -Force -Path $dbParent, (Split-Path -Parent $exe), $logDir | Out-Null
Get-ChildItem -Path $dbParent -Filter (([System.IO.Path]::GetFileName($dbFull)) + "*") -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
$marker = "$dbFull.marker"
$base = "http://127.0.0.1:$Port"
if ([System.IO.Path]::IsPathRooted($SampleDir)) { $sampleFull = $SampleDir } else { $sampleFull = Join-Path $root $SampleDir }
$legacy = [System.IO.Path]::GetFullPath((Join-Path $sampleFull "v1-legacy.bin"))
$current = [System.IO.Path]::GetFullPath((Join-Path $sampleFull "v2-current.jsonl"))

Write-Host "Building meshpolicy.exe ..."
& go build -o $exe .\cmd\meshpolicy
if ($LASTEXITCODE -ne 0) { throw "go build failed" }

$env:MESHPOLICY_ENABLE_TEST_HOOKS = "1"
$env:MESHPOLICY_TEST_MARKER = $marker
$env:MESHPOLICY_TEST_PAUSE_SECONDS = "$PauseSeconds"

function Start-LocalServer([string]$suffix) {
    $out = Join-Path $logDir "kill-test-$suffix.out.log"
    $err = Join-Path $logDir "kill-test-$suffix.err.log"
    $serverArgs = 'serve -addr "127.0.0.1:{0}" -db "{1}"' -f $Port, $dbFull
    return Start-Process -FilePath $exe -ArgumentList $serverArgs -PassThru -RedirectStandardOutput $out -RedirectStandardError $err
}

function Wait-Health {
    for ($i = 0; $i -lt 120; $i++) {
        try {
            Invoke-RestMethod -Uri "$base/healthz" -TimeoutSec 1 | Out-Null
            return
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    throw "server at $base did not become healthy"
}

function Invoke-Import([string]$requestId, [string]$path) {
    $client = New-Object System.Net.Http.HttpClient
    try {
        $client.Timeout = [TimeSpan]::FromSeconds(30)
        $bytes = [System.IO.File]::ReadAllBytes($path)
        $content = New-Object System.Net.Http.ByteArrayContent -ArgumentList (,$bytes)
        $content.Headers.ContentType = New-Object System.Net.Http.Headers.MediaTypeHeaderValue -ArgumentList "application/octet-stream"
        $request = New-Object System.Net.Http.HttpRequestMessage -ArgumentList ([System.Net.Http.HttpMethod]::Post), "$base/v1/import?truncation=floor"
        $request.Headers.Add("X-Request-ID", $requestId)
        $request.Content = $content
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        $status = [int]$response.StatusCode
        $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        $response.Dispose()
        $request.Dispose()
        $content.Dispose()
        return @{ Status = $status; Body = $body; Json = ($body | ConvertFrom-Json) }
    } finally {
        $client.Dispose()
    }
}

$server = $null
$pauseJob = $null
try {
    $server = Start-LocalServer "first"
    Wait-Health
    $r1 = Invoke-Import "kill-v1" $legacy
    if ($r1.Status -ne 201) { throw "v1 import expected 201, got $($r1.Status): $($r1.Body)" }
    $before = Invoke-RestMethod -Uri "$base/v1/versions"
    if ([int]$before.activeVersion -ne 1) { throw "expected active version 1 before crash" }

    $pauseJob = Start-Job -ScriptBlock {
        param($uri, $requestId, $path)
        Add-Type -AssemblyName System.Net.Http
        $bytes = [System.IO.File]::ReadAllBytes($path)
        try {
            $client = New-Object System.Net.Http.HttpClient
            $client.Timeout = [TimeSpan]::FromSeconds(180)
            $content = New-Object System.Net.Http.ByteArrayContent -ArgumentList (,$bytes)
            $content.Headers.ContentType = New-Object System.Net.Http.Headers.MediaTypeHeaderValue -ArgumentList "application/octet-stream"
            $request = New-Object System.Net.Http.HttpRequestMessage -ArgumentList ([System.Net.Http.HttpMethod]::Post), $uri
            $request.Headers.Add("X-Request-ID", $requestId)
            $request.Headers.Add("X-Test-Pause-Mid-Rebuild", "1")
            $request.Content = $content
            $null = $client.SendAsync($request).GetAwaiter().GetResult()
        } catch {
            # Expected when the server is forcibly terminated.
        }
    } -ArgumentList "$base/v1/import?truncation=floor", "kill-v2", $current

    for ($i = 0; $i -lt 120; $i++) {
        if (Test-Path $marker) { break }
        Start-Sleep -Milliseconds 250
    }
    if (-not (Test-Path $marker)) { throw "mid-rebuild marker was not written" }
    Write-Host "Mid-rebuild transaction reached; forcibly terminating PID $($server.Id)"
    Stop-Process -Id $server.Id -Force
    Wait-Process -Id $server.Id -ErrorAction SilentlyContinue
    $server = $null
    if ($pauseJob) {
        Stop-Job -Job $pauseJob -ErrorAction SilentlyContinue
        Remove-Job -Job $pauseJob -Force -ErrorAction SilentlyContinue
        $pauseJob = $null
    }

    $server = Start-LocalServer "recovered"
    Wait-Health

    $versions = Invoke-RestMethod -Uri "$base/v1/versions"
    $recovery = Invoke-RestMethod -Uri "$base/v1/recovery-report"
    if ([int]$versions.activeVersion -ne 1) { throw "active version changed after crash: $($versions.activeVersion)" }
    if (@($versions.versions).Count -ne 1) { throw "uncommitted snapshot row survived crash" }
    if ([int]$recovery.activeSnapshotVersion -ne 1) { throw "recovery report active version is not 1" }
    if (-not $recovery.activeSnapshotIntegrity.complete) { throw "recovery report stale sense: active snapshot incomplete" }
    if ([int]$recovery.pendingIntentCount -lt 1) { throw "recovery report did not record the pending intent" }
    if ($recovery.factsSource -ne "sqlite_committed_rows_only") { throw "recovery report facts source is not committed SQLite rows" }
    if (@($recovery.abortedBatches).Count -lt 1) { throw "recovery report has no aborted batch" }

    $derived = Invoke-RestMethod -Uri "$base/v1/derived"
    if ([int]$derived.snapshotVersion -ne 1) { throw "derived structures are not bound to version 1" }

    $replay = Invoke-Import "kill-v2" $current
    if ($replay.Status -ne 503) { throw "aborted request replay expected 503, got $($replay.Status)" }
    if ($replay.Json.error.code -ne "E_IMPORT_ABORTED") { throw "aborted replay error code: $($replay.Json.error.code)" }

    Write-Host "PASS forced-termination recovery"
    Write-Host "activeVersion=1, versions=1, abortedBatch=1, derivedVersion=1, replay=E_IMPORT_ABORTED"
    $recovery | ConvertTo-Json -Depth 12
} finally {
    if ($pauseJob) {
        Stop-Job -Job $pauseJob -ErrorAction SilentlyContinue
        Remove-Job -Job $pauseJob -Force -ErrorAction SilentlyContinue
    }
    if ($server -and -not $server.HasExited) {
        Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    }
}
