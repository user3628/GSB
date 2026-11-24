[CmdletBinding()]
param(
    [int]$Port = 18080,
    [string]$WorkDir = ""
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($WorkDir)) {
    $WorkDir = Join-Path $root ".demo-run"
}
if (Test-Path $WorkDir) {
    Remove-Item -Recurse -Force $WorkDir
}
New-Item -ItemType Directory -Path $WorkDir | Out-Null
$db = Join-Path $WorkDir "columnar.db"
$exe = Join-Path $WorkDir "columnar-server.exe"
$baseUrl = "http://127.0.0.1:$Port"

Push-Location $root
try {
    & go build -o $exe ./cmd/server
    if ($LASTEXITCODE -ne 0) { throw "go build failed" }
} finally {
    Pop-Location
}

$env:COLUMNAR_ALLOW_CRASH = "1"

function Start-DemoServer([string]$suffix) {
    $stdout = Join-Path $WorkDir "server-$suffix.out.log"
    $stderr = Join-Path $WorkDir "server-$suffix.err.log"
    $arguments = @(
        "-listen", "127.0.0.1:$Port",
        "-db", "`"$db`"",
        "-auto-seed=false",
        "-allow-crash"
    )
    $process = Start-Process -FilePath $exe -ArgumentList $arguments -PassThru `
        -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    for ($i = 0; $i -lt 100; $i++) {
        Start-Sleep -Milliseconds 100
        if ($process.HasExited) {
            throw "server exited during startup; see $stderr"
        }
        try {
            Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/healthz" -TimeoutSec 1 | Out-Null
            return $process
        } catch {
            continue
        }
    }
    throw "server did not become healthy"
}

function Stop-DemoServer($process) {
    if ($null -ne $process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
        $process.WaitForExit()
    }
}

function Invoke-Json([string]$method, [string]$path, [string]$json) {
    $params = @{
        UseBasicParsing = $true
        Method = $method
        Uri = "$baseUrl$path"
        TimeoutSec = 30
    }
    if (-not [string]::IsNullOrWhiteSpace($json)) {
        $params["Body"] = $json
        $params["ContentType"] = "application/json"
    }
    return Invoke-RestMethod @params
}

function Invoke-ExpectedFailure([string]$path, [string]$json) {
    $failed = $false
    $body = ""
    try {
        Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$baseUrl$path" `
            -ContentType "application/json" -Body $json -TimeoutSec 30 | Out-Null
    } catch {
        $failed = $true
        $body = $_.ErrorDetails.Message
        if ([string]::IsNullOrWhiteSpace($body) -and $null -ne $_.Exception.Response) {
            $stream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            $body = $reader.ReadToEnd()
        }
    }
    if (-not $failed) { throw "expected an HTTP failure from $path" }
    return ($body | ConvertFrom-Json)
}

$server = $null
$crashedServer = $null
try {
    $server = Start-DemoServer "first"

    $snapshotRequest = @"
{"version":1,"rows":257,"seed":20240517}
"@
    $snapshot = Invoke-Json "POST" "/api/v1/snapshots/generate" $snapshotRequest

    $chainJson = @"
{
  "id":"demo-sales",
  "version":1,
  "base_snapshot_version":1,
  "operators":[
    {"kind":"scan","scan":{"columns":["id","quantity","price","category"]}},
    {"kind":"filter","filter":{"predicate":{"column":"quantity","op":"ge","value_int":20}}},
    {"kind":"project","project":{"columns":["id","quantity","price","category"]}},
    {"kind":"aggregate","aggregate":{"specs":[
      {"name":"row_count","func":"count"},
      {"name":"quantity_count","func":"count_non_null","column":"quantity"},
      {"name":"quantity_sum","func":"sum","column":"quantity"},
      {"name":"quantity_avg","func":"avg","column":"quantity"},
      {"name":"price_sum","func":"sum","column":"price"},
      {"name":"price_avg","func":"avg","column":"price"},
      {"name":"category_distinct","func":"count_distinct","column":"category"}
    ]}}
  ]
}
"@
    Invoke-Json "POST" "/api/v1/chains" $chainJson | Out-Null

    $smallJson = @"
{"execution_id":"demo-small","chain_version":1,"bitmap_form":"all","batch_size":8,"large_vector_threshold":128}
"@
    $largeJson = @"
{"execution_id":"demo-large","chain_version":1,"bitmap_form":"all","batch_size":256,"large_vector_threshold":128}
"@
    $small = Invoke-Json "POST" "/api/v1/chains/demo-sales/execute" $smallJson
    $large = Invoke-Json "POST" "/api/v1/chains/demo-sales/execute" $largeJson
    $comparison = Invoke-Json "GET" "/api/v1/executions/demo-small/compare?right=demo-large&tolerance_ppm=1" ""
    Write-Host ("consistency: small_path={0} batches={1}; large_path={2} batches={3}; rows_equal={4}; aggregates_equal={5}" -f `
        $small.path.path, $small.path.batch_count, $large.path.path, $large.path.batch_count, `
        $comparison.rows_consistent, $comparison.aggregates_consistent)

    $faults = @(
        @{ kind = "bitmap_mismatch"; code = "E_BITMAP_MISMATCH" },
        @{ kind = "fixed_overflow"; code = "E_NUMERIC_OVERFLOW" },
        @{ kind = "dict_index"; code = "E_DICT_INDEX" },
        @{ kind = "divide_by_zero"; code = "E_DIVIDE_BY_ZERO" },
        @{ kind = "exec_aborted"; code = "E_EXEC_ABORTED" }
    )
    foreach ($fault in $faults) {
        $executionId = "fault-" + $fault.kind
        $faultJson = @"
{"execution_id":"$executionId","chain_version":1,"bitmap_form":"all","batch_size":8,"large_vector_threshold":128,"injections":[{"kind":"$($fault.kind)","batch":2}]}
"@
        $failure = Invoke-ExpectedFailure "/api/v1/chains/demo-sales/execute" $faultJson
        if ($failure.error.code -ne $fault.code) {
            throw "fault $($fault.kind) returned $($failure.error.code), expected $($fault.code)"
        }
        $failedView = Invoke-Json "GET" "/api/v1/executions/$executionId" ""
        Write-Host ("fault: kind={0} code={1} status={2} published={3} batches={4} aggregates={5}" -f `
            $fault.kind, $failure.error.code, $failedView.status, $failedView.published, `
            @($failedView.batches).Count, @($failedView.aggregates).Count)
    }

    $crashJson = @"
{"execution_id":"crash-demo","chain_version":1,"bitmap_form":"all","batch_size":8,"large_vector_threshold":128,"crash_after_batch":2}
"@
    try {
        Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$baseUrl/api/v1/chains/demo-sales/execute" `
            -ContentType "application/json" -Body $crashJson -TimeoutSec 30 | Out-Null
    } catch {
        # The server exits with code 91 while serving this request.
    }
    $crashedServer = $server
    $server = $null
    $crashedServer.WaitForExit(10000) | Out-Null
    if (-not $crashedServer.HasExited) { throw "server did not terminate at crash point" }
    Write-Host ("forced termination: process_exit_code={0}" -f $crashedServer.ExitCode)

    $server = Start-DemoServer "recovered"
    $recoveredView = Invoke-Json "GET" "/api/v1/executions/crash-demo" ""
    $reports = Invoke-Json "GET" "/api/v1/recovery/reports" ""
    $report = @($reports.reports)[-1]
    Write-Host ("recovery: execution={0} status={1} published={2} rolled_back_batches=[{3}] discarded_aggregates=[{4}] intents={5}" -f `
        $recoveredView.execution_id, $recoveredView.status, $recoveredView.published, `
        (@($report.rolled_back_batches) -join ","), (@($report.discarded_aggregate_batches) -join ","), `
        @($report.intents).Count)
    Write-Host ("recovery artifact: db={0} report={1}" -f $db, $report.id)
} finally {
    Stop-DemoServer $server
    Stop-DemoServer $crashedServer
}
