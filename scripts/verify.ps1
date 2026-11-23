param(
    [int]$Port = 0,
    [switch]$KeepData,
    [string]$Binary = ""
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).ProviderPath
Set-Location $Root
if ($Port -eq 0) {
    $probe = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $probe.Start()
    $Port = ([System.Net.IPEndPoint]$probe.LocalEndpoint).Port
    $probe.Stop()
}
$Base = "http://127.0.0.1:$Port"
$runId = [Guid]::NewGuid().ToString("N")
$buildDir = Join-Path $env:TEMP "cdnlab-build-$runId"
$exe = if ($Binary -ne "") { $Binary } else { Join-Path $buildDir "cdnlab.exe" }
$db = Join-Path $env:TEMP "cdnlab-verify-$runId.db"
$configPath = Join-Path $Root "samples\config.json"
$policyPath = Join-Path $Root "samples\policies\policy-v1.json"
$bodyFile = Join-Path $buildDir "app-v1.js"
$http = [System.Net.Http.HttpClient]::new()
$http.Timeout = [TimeSpan]::FromSeconds(15)
$script:proc = $null
$script:restart = 0

New-Item -ItemType Directory -Force -Path $buildDir | Out-Null

function Invoke-Api {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Body = "",
        [hashtable]$Headers = @{}
    )
    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::new($Method), $Url)
    foreach ($key in $Headers.Keys) {
        [void]$request.Headers.TryAddWithoutValidation($key, $Headers[$key])
    }
    if ($Body -ne "") {
        $request.Content = [System.Net.Http.StringContent]::new($Body, [System.Text.Encoding]::UTF8, "application/json")
    }
    try {
        $response = $http.SendAsync($request).GetAwaiter().GetResult()
        $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        $responseHeaders = @{}
        foreach ($header in $response.Headers) {
            $responseHeaders[$header.Key] = ($header.Value -join ",")
        }
        $result = [pscustomobject]@{
            Status  = [int]$response.StatusCode
            Body    = $text
            Headers = $responseHeaders
        }
        $response.Dispose()
        return $result
    }
    catch {
        return [pscustomobject]@{
            Status  = 0
            Body    = $_.Exception.ToString()
            Headers = @{}
        }
    }
    finally {
        $request.Dispose()
    }
}

function Start-Cdnlab {
    $script:restart++
    $outLog = Join-Path $buildDir "server-$($script:restart).out.log"
    $errLog = Join-Path $buildDir "server-$($script:restart).err.log"
    $arguments = @(
        "serve",
        "--config", $configPath,
        "--listen", "127.0.0.1:$Port",
        "--db", $db
    )
    $script:proc = Start-Process -FilePath $exe -ArgumentList $arguments -WorkingDirectory $env:TEMP `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog -PassThru -WindowStyle Hidden
    for ($i = 0; $i -lt 200; $i++) {
        if ($script:proc.HasExited) {
            $stderr = if (Test-Path $errLog) { Get-Content $errLog -Raw } else { "" }
            throw "cdnlab exited before becoming healthy: $stderr"
        }
        $health = Invoke-Api -Method GET -Url "$Base/healthz"
        if ($health.Status -eq 200) {
            return
        }
        Start-Sleep -Milliseconds 50
    }
    throw "cdnlab did not become healthy"
}

try {
    if ($Binary -eq "") {
        Write-Host "== build"
        & go build -o $exe ./cmd/cdnlab
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    } else {
        if (-not (Test-Path $exe)) { throw "binary not found: $exe" }
        Write-Host "== use prebuilt binary $exe"
    }
    Set-Content -NoNewline -Path $bodyFile -Value 'console.log("app-v1");'
    & $exe publish --config $configPath --path /static/app.js --file $bodyFile | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "publish failed" }

    Write-Host "== start and prime"
    Start-Cdnlab
    $prime = Invoke-Api -Method GET -Url "$Base/edge/edge-a/static/app.js"
    if ($prime.Status -ne 200) { throw "prime failed: $($prime.Status) $($prime.Body)" }
    Write-Host "prime cache-status=$($prime.Headers['X-Cache-Status']) etag=$($prime.Headers['ETag'])"

    Write-Host "== persist E_ORIGIN_TIMEOUT"
    $timeout = Invoke-Api -Method GET -Url "$Base/edge/edge-a/static/style.css?case=t&__fault=origin_timeout&__delay_ms=1000" `
        -Headers @{ "X-Request-ID" = "verify-origin-timeout" }
    if ($timeout.Status -ne 504) { throw "timeout fault status=$($timeout.Status) body=$($timeout.Body)" }

    Write-Host "== persist E_STALE_SNAPSHOT"
    $policyJson = Get-Content $policyPath -Raw
    $stale = Invoke-Api -Method POST -Url "$Base/admin/policies" -Body $policyJson `
        -Headers @{ "X-Request-ID" = "verify-stale-policy" }
    if ($stale.Status -ne 409) { throw "stale policy status=$($stale.Status) body=$($stale.Body)" }

    Write-Host "== purge interruption must be all-or-nothing"
    $purgeBody = '{"kind":"path","path":"/static/app.js","interrupt_nodes":["edge-b"]}'
    $purge = Invoke-Api -Method POST -Url "$Base/admin/purge" -Body $purgeBody `
        -Headers @{ "X-Request-ID" = "verify-purge-incomplete" }
    if ($purge.Status -ne 409) { throw "purge status=$($purge.Status) body=$($purge.Body)" }
    if ($purge.Body -notmatch "E_PURGE_INCOMPLETE" -or $purge.Body -notmatch "edge-b") {
        throw "purge response missing incomplete details: $($purge.Body)"
    }

    Write-Host "== force termination with pending intent"
    $null = Invoke-Api -Method POST -Url "$Base/admin/debug/crash?node=edge-a&path=/static/app.js" `
        -Headers @{ "X-Request-ID" = "verify-crash" }
    if (-not $script:proc.WaitForExit(5000)) {
        throw "server did not terminate after debug crash"
    }
    $script:proc = $null

    Write-Host "== restart and recover"
    Start-Cdnlab
    $recovery = Invoke-Api -Method GET -Url "$Base/admin/recovery"
    $entriesResponse = Invoke-Api -Method GET -Url "$Base/admin/entries?path=/static/app.js"
    if ($recovery.Status -ne 200 -or $entriesResponse.Status -ne 200) {
        throw "recovery inspection failed: $($recovery.Status) $($entriesResponse.Status)"
    }
    $report = $recovery.Body | ConvertFrom-Json
    $entries = ($entriesResponse.Body | ConvertFrom-Json).entries
    if ($report.facts_only -ne $true) { throw "recovery report is not facts-only" }
    if ([int]$report.scanned_intents -lt 1) { throw "no pending intent was scanned" }
    if ([int]$report.rolled_back_rows -lt 1) { throw "no entry rows were rolled back" }
    $codes = @($report.business_codes | ForEach-Object { $_.code })
    foreach ($required in @("E_PURGE_INCOMPLETE", "E_ORIGIN_TIMEOUT", "E_STALE_SNAPSHOT")) {
        if ($codes -notcontains $required) {
            throw "recovery report missing $required; codes=$($codes -join ',')"
        }
    }
    if (@($entries).Count -ne 4) { throw "expected 4 restored entries, got $(@($entries).Count)" }
    foreach ($entry in $entries) {
        if ($entry.state -ne "FRESH") { throw "entry $($entry.node_id) was not restored: $($entry.state)" }
    }

    $summary = [pscustomobject]@{
        recovery_report = $report
        restored_entries = @($entries | ForEach-Object {
            [pscustomobject]@{ node = $_.node_id; state = $_.state; etag = $_.etag }
        })
    }
    $summary | ConvertTo-Json -Depth 12
    Write-Host "VERIFY_OK"
}
finally {
    if ($script:proc -ne $null -and -not $script:proc.HasExited) {
        Stop-Process -Id $script:proc.Id -Force
    }
    $http.Dispose()
    if (-not $KeepData) {
        foreach ($suffix in @("", "-wal", "-shm")) {
            Remove-Item -Force -ErrorAction SilentlyContinue -Path "$db$suffix"
        }
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue -Path $buildDir
    }
}
