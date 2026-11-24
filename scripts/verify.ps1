param(
    [string]$ServerExe = ""
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Artifacts = Join-Path $Root "artifacts\verify"
$ExpectedDir = Join-Path $Root "samples\expected"
$SamplesDir = Join-Path $Root "samples"

function Fail([string]$Message) {
    throw "FAIL: $Message"
}

function Assert-Equal($Actual, $Expected, [string]$Label) {
    if ($Actual -ne $Expected) {
        Fail "$Label expected=[$Expected] actual=[$Actual]"
    }
}

function Write-Utf8NoBom([string]$Path, [string]$Text) {
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Text, $encoding)
}

function Read-Text([string]$Path) {
    return [System.IO.File]::ReadAllText($Path)
}

function Find-ItemById($Items, [string]$Id, [string]$Field) {
    foreach ($item in $Items) {
        if ($item.$Field -eq $Id) { return $item }
    }
    Fail "item $Id not found in field $Field"
}

$Port = $null
$ServerProcess = $null
$Http = New-Object System.Net.Http.HttpClient
$Http.Timeout = [TimeSpan]::FromSeconds(40)
$Http.DefaultRequestHeaders.ConnectionClose = $true

function Get-FreePort {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $p = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    $listener.Stop()
    return $p
}

function Invoke-Ledger([string]$Method, [string]$Path, [string]$BodyText = "") {
    $uri = "http://127.0.0.1:$Port$Path"
    if ($Method -eq "POST") {
        $content = New-Object System.Net.Http.StringContent($BodyText, [System.Text.Encoding]::UTF8, "application/json")
        $response = $Http.PostAsync($uri, $content).GetAwaiter().GetResult()
    } else {
        $response = $Http.GetAsync($uri).GetAwaiter().GetResult()
    }
    $status = [int]$response.StatusCode
    $text = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    $response.Dispose()
    $parsed = $null
    if (-not [string]::IsNullOrWhiteSpace($text)) {
        $parsed = $text | ConvertFrom-Json
    }
    return [pscustomobject]@{
        Status = $status
        Text = $text
        Json = $parsed
    }
}

function Wait-Health([int]$TimeoutSeconds = 20) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ($ServerProcess.HasExited) {
            $err = ""
            if (Test-Path $ServerErr) { $err = Read-Text $ServerErr }
            Fail "server exited during startup: $err"
        }
        try {
            $r = Invoke-Ledger "GET" "/healthz"
            if ($r.Status -eq 200) { return }
        } catch {
            Start-Sleep -Milliseconds 200
        }
    }
    Fail "server health check timed out"
}

function Start-Ledger([string]$DbPath, [string]$StdoutPath, [string]$StderrPath) {
    $script:ServerErr = $StderrPath
    $serverArgs = @("-db", $DbPath, "-addr", "127.0.0.1:$Port", "-enable-crash-endpoint")
    $script:ServerProcess = Start-Process -FilePath $ServerExe -ArgumentList $serverArgs -WorkingDirectory $Artifacts -RedirectStandardOutput $StdoutPath -RedirectStandardError $StderrPath -PassThru
    Wait-Health
}

function Stop-Ledger {
    if ($null -ne $script:ServerProcess -and -not $script:ServerProcess.HasExited) {
        Stop-Process -Id $script:ServerProcess.Id -Force
        $script:ServerProcess.WaitForExit(5000) | Out-Null
    }
}

function Print-SegmentDetails($Report, [int64]$BaseMinute) {
    Write-Host "Segmented interest by tenant, lot, rate tier and minute (offsets from sample T0):"
    foreach ($tenant in $Report.tenant_segments) {
        Write-Host ("  tenant={0} principal_milli={1} interest_micros={2} rounding_nanos={3}" -f $tenant.tenant_id, $tenant.principal_outstanding_milli, $tenant.unsettled_interest_micros, $tenant.cumulative_rounding_error_nanos)
        foreach ($seg in $tenant.rate_segments) {
            $start = [int64]$seg.start_minute - $BaseMinute
            $end = [int64]$seg.end_minute_exclusive - $BaseMinute
            Write-Host ("    segment tier={0} rate_ppm/min={1} minutes=[{2},{3}) exact_nanos={4} interest_micros={5} rounding_nanos={6}" -f $seg.rate_tier, $seg.rate_ppm_per_minute, $start, $end, $seg.exact_interest_nanos, $seg.interest_micros, $seg.rounding_error_nanos)
        }
        foreach ($step in $tenant.interest_steps) {
            $m = [int64]$step.minute - $BaseMinute
            Write-Host ("    step tenant={0} lot={1} minute={2} tier={3} principal_milli={4} exact_nanos={5} delta_micros={6} carry_nanos={7}" -f $step.tenant_id, $step.lot_id, $m, $step.rate_tier, $step.principal_milli, $step.exact_interest_nanos, $step.delta_interest_micros, $step.carry_nanos)
        }
    }
}

function Print-Pool($Pool) {
    Write-Host ("  pool={0} used_micros={1} used_ratio={2} ppm={3} overcommit={4} excess={5} trigger={6} cross_exposure={7}" -f $Pool.pool_id, $Pool.used_micros, $Pool.used_ratio_fixed, $Pool.used_ratio_ppm, $Pool.overcommit_risk, $Pool.excess_over_threshold_micros, $Pool.trigger_tenant_id, $Pool.cross_tenant_exposure_micros)
}

function Print-StateSummary($State) {
    Write-Host ("  ledger_version={0} as_of_minute={1} state_digest={2}" -f $State.ledger_version, $State.as_of_minute, $State.state_digest)
    foreach ($tenant in $State.tenants) {
        Write-Host ("  tenant={0} principal_milli={1} interest_micros={2} available_credit_micros={3} rounding_nanos={4}" -f $tenant.tenant_id, $tenant.principal_outstanding_milli, $tenant.unsettled_interest_micros, $tenant.available_credit_micros, $tenant.rounding_error_nanos)
    }
    foreach ($pool in $State.pools) { Print-Pool $pool }
}

try {
    Write-Host "=== quota-ledger Windows end-to-end verification ==="
    & go version
    if ($LASTEXITCODE -ne 0) { Fail "go command is not available" }
    $sqliteCmd = Get-Command sqlite3 -ErrorAction SilentlyContinue
    if ($null -eq $sqliteCmd) { Fail "sqlite3 command is not available on PATH" }

    if (Test-Path $Artifacts) {
        Remove-Item -LiteralPath $Artifacts -Recurse -Force
    }
    New-Item -ItemType Directory -Path $Artifacts -Force | Out-Null
    if ([string]::IsNullOrWhiteSpace($ServerExe)) {
        $ServerExe = Join-Path $Artifacts "ledger-server.exe"
        Push-Location $Root
        try {
            $buildArgs = @("build", "-o", $ServerExe, "./cmd/ledger-server")
            if (Test-Path (Join-Path $Root "vendor")) {
                $buildArgs = @("build", "-mod=vendor", "-o", $ServerExe, "./cmd/ledger-server")
            }
            & go @buildArgs
            if ($LASTEXITCODE -ne 0) { Fail "go build failed" }
        } finally {
            Pop-Location
        }
    }
    $ServerExe = (Resolve-Path $ServerExe).Path
    $Port = Get-FreePort
    $BaseMinute = [int64][math]::Floor(1767225600000 / 60000)

    $FirstOut = Join-Path $Artifacts "server-first.out.log"
    $FirstErr = Join-Path $Artifacts "server-first.err.log"
    $SecondOut = Join-Path $Artifacts "server-second.out.log"
    $SecondErr = Join-Path $Artifacts "server-second.err.log"
    Start-Ledger "ledger.db" $FirstOut $FirstErr

    $snap1 = Read-Text (Join-Path $SamplesDir "snapshot-v1.json")
    $snap2 = Read-Text (Join-Path $SamplesDir "snapshot-v2.json")
    $snap1Body = '{"request_id":"snapshot-v1","snapshot":' + $snap1 + '}'
    $snap2Body = '{"request_id":"snapshot-v2","snapshot":' + $snap2 + '}'
    $r1 = Invoke-Ledger "POST" "/v1/snapshots" $snap1Body
    $r2 = Invoke-Ledger "POST" "/v1/snapshots" $snap2Body
    Assert-Equal $r1.Status 200 "snapshot v1 HTTP"
    Assert-Equal $r2.Status 200 "snapshot v2 HTTP"
    Assert-Equal $r1.Json.data.snapshot_digest ((Get-Content -Raw (Join-Path $SamplesDir "snapshot-v1.json") | ConvertFrom-Json).digest) "snapshot v1 digest"

    $eventsText = Read-Text (Join-Path $SamplesDir "events.json")
    $eventResp = Invoke-Ledger "POST" "/v1/events" $eventsText
    Assert-Equal $eventResp.Status 200 "event ingestion HTTP"
    Assert-Equal $eventResp.Json.data.new_event_count 7 "event ingestion count"
    Write-Utf8NoBom (Join-Path $Artifacts "events-response.json") $eventResp.Text

    $settleText = Read-Text (Join-Path $SamplesDir "settle.json")
    $settleResp = Invoke-Ledger "POST" "/v1/settle" $settleText
    Assert-Equal $settleResp.Status 200 "settlement HTTP"
    $report = $settleResp.Json.data
    $expectedSettlement = Read-Text (Join-Path $ExpectedDir "settlement-10m-summary.json") | ConvertFrom-Json
    Assert-Equal $report.ledger_version $expectedSettlement.ledger_version "settlement ledger version"
    Assert-Equal $report.state_digest $expectedSettlement.state_digest "settlement state digest"
    Assert-Equal $report.calculation_digest $expectedSettlement.calculation_digest "settlement calculation digest"
    Assert-Equal $report.through_minute $expectedSettlement.through_minute "settlement through minute"
    foreach ($expectedTenant in $expectedSettlement.tenant_segments) {
        $actualTenant = Find-ItemById $report.tenant_segments $expectedTenant.tenant_id "tenant_id"
        Assert-Equal $actualTenant.principal_outstanding_milli $expectedTenant.principal_outstanding_milli "$($expectedTenant.tenant_id) principal"
        Assert-Equal $actualTenant.unsettled_interest_micros $expectedTenant.unsettled_interest_micros "$($expectedTenant.tenant_id) interest"
        Assert-Equal $actualTenant.available_credit_micros $expectedTenant.available_credit_micros "$($expectedTenant.tenant_id) available credit"
        Assert-Equal $actualTenant.cumulative_rounding_error_nanos $expectedTenant.cumulative_rounding_error_nanos "$($expectedTenant.tenant_id) rounding"
        Assert-Equal $actualTenant.rate_segments.Count $expectedTenant.rate_segments.Count "$($expectedTenant.tenant_id) rate segment count"
        for ($i = 0; $i -lt $expectedTenant.rate_segments.Count; $i++) {
            Assert-Equal $actualTenant.rate_segments[$i].rate_tier $expectedTenant.rate_segments[$i].rate_tier "$($expectedTenant.tenant_id) segment tier"
            Assert-Equal $actualTenant.rate_segments[$i].rate_ppm_per_minute $expectedTenant.rate_segments[$i].rate_ppm_per_minute "$($expectedTenant.tenant_id) segment rate"
            Assert-Equal $actualTenant.rate_segments[$i].start_minute $expectedTenant.rate_segments[$i].start_minute "$($expectedTenant.tenant_id) segment start"
            Assert-Equal $actualTenant.rate_segments[$i].end_minute_exclusive $expectedTenant.rate_segments[$i].end_minute_exclusive "$($expectedTenant.tenant_id) segment end"
            Assert-Equal $actualTenant.rate_segments[$i].interest_micros $expectedTenant.rate_segments[$i].interest_micros "$($expectedTenant.tenant_id) segment interest"
        }
    }
    foreach ($expectedPool in $expectedSettlement.pools) {
        $actualPool = Find-ItemById $report.pools $expectedPool.pool_id "pool_id"
        Assert-Equal $actualPool.used_ratio_fixed $expectedPool.used_ratio_fixed "pool ratio"
        Assert-Equal $actualPool.used_ratio_ppm $expectedPool.used_ratio_ppm "pool ratio ppm"
        Assert-Equal $actualPool.overcommit_risk $expectedPool.overcommit_risk "pool risk"
        Assert-Equal $actualPool.excess_over_threshold_micros $expectedPool.excess_over_threshold_micros "pool excess"
        Assert-Equal $actualPool.trigger_tenant_id $expectedPool.trigger_tenant_id "pool trigger tenant"
        Assert-Equal $actualPool.cross_tenant_exposure_micros $expectedPool.cross_tenant_exposure_micros "pool cross exposure"
    }
    Write-Utf8NoBom (Join-Path $Artifacts "settlement-10m-actual.json") (($report | ConvertTo-Json -Depth 100))

    $stateResp = Invoke-Ledger "GET" "/v1/state"
    Assert-Equal $stateResp.Status 200 "state HTTP"
    $stateBefore = $stateResp.Json.data
    Assert-Equal $stateBefore.ledger_version $expectedSettlement.ledger_version "state before borrow version"
    Assert-Equal $stateBefore.state_digest $expectedSettlement.state_digest "state before borrow digest"
    $expectedState = Read-Text (Join-Path $ExpectedDir "state-after-settle.json") | ConvertFrom-Json
    $actualStateJson = $stateBefore | ConvertTo-Json -Depth 100 -Compress
    $expectedStateJson = $expectedState | ConvertTo-Json -Depth 100 -Compress
    Assert-Equal $actualStateJson $expectedStateJson "complete derived ledger after settlement"
    Write-Utf8NoBom (Join-Path $Artifacts "state-before-crash.json") ($stateBefore | ConvertTo-Json -Depth 100)

    Write-Host ""
    Write-Host "=== Segmented interest ==="
    Print-SegmentDetails $report $BaseMinute
    Write-Host ""
    Write-Host "=== Pool overcommit before new borrow ==="
    foreach ($p in $report.pools) { Print-Pool $p }

    $overResp = Invoke-Ledger "POST" "/v1/borrow-requests" (Read-Text (Join-Path $SamplesDir "borrow-overcommit.json"))
    $expectedOver = Read-Text (Join-Path $ExpectedDir "borrow-overcommit.json") | ConvertFrom-Json
    Assert-Equal $overResp.Status $expectedOver.http_status "overcommit HTTP"
    Assert-Equal $overResp.Json.error.code $expectedOver.error.code "overcommit code"
    Assert-Equal $overResp.Json.error.context.trigger_tenant_id $expectedOver.error.context.trigger_tenant_id "overcommit trigger"
    Assert-Equal $overResp.Json.error.context.projected_used_micros $expectedOver.error.context.projected_used_micros "overcommit projected used"
    Assert-Equal $overResp.Json.error.context.excess_over_threshold_micros $expectedOver.error.context.excess_over_threshold_micros "overcommit excess"
    Write-Utf8NoBom (Join-Path $Artifacts "borrow-overcommit-actual.json") $overResp.Text
    Write-Host ""
    Write-Host "=== Pool overcommit adjudication ==="
    Write-Host ("  HTTP={0} code={1} tenant={2} projected_used={3} excess={4} ratio={5}" -f $overResp.Status, $overResp.Json.error.code, $overResp.Json.error.context.trigger_tenant_id, $overResp.Json.error.context.projected_used_micros, $overResp.Json.error.context.excess_over_threshold_micros, $overResp.Json.error.context.projected_used_ratio_fixed)

    $stateAfterReject = (Invoke-Ledger "GET" "/v1/state").Json.data
    Assert-Equal $stateAfterReject.ledger_version $stateBefore.ledger_version "rejected borrow version unchanged"
    Assert-Equal $stateAfterReject.state_digest $stateBefore.state_digest "rejected borrow digest unchanged"

    Write-Host ""
    Write-Host "=== Force termination during settlement/rebuild ==="
    $crashBody = Read-Text (Join-Path $SamplesDir "crash-settle.json")
    $crashConnectionDropped = $false
    try {
        Invoke-Ledger "POST" "/v1/admin/crash-during-settle" $crashBody | Out-Null
    } catch {
        $crashConnectionDropped = $true
        Write-Host ("  forced connection termination observed: {0}" -f $_.Exception.Message)
    }
    if (-not $crashConnectionDropped) { Fail "crash endpoint returned without dropping the HTTP connection" }
    $ServerProcess.WaitForExit(15000) | Out-Null
    if (-not $ServerProcess.HasExited) { Fail "server did not terminate after crash injection" }
    if ($ServerProcess.ExitCode -eq 0) { Fail "crash-injected server exit code must be non-zero" }
    Write-Host ("  server exit code={0}" -f $ServerProcess.ExitCode)

    Start-Sleep -Milliseconds 250
    $Port = Get-FreePort
    Start-Ledger "ledger.db" $SecondOut $SecondErr
    $recoveryResp = Invoke-Ledger "GET" "/v1/recovery"
    Assert-Equal $recoveryResp.Status 200 "recovery API HTTP"
    $recovery = $recoveryResp.Json.data
    $expectedRecovery = Read-Text (Join-Path $ExpectedDir "recovery.json") | ConvertFrom-Json
    Assert-Equal $recovery.last_complete_version $expectedRecovery.last_complete_version "recovery complete version"
    Assert-Equal $recovery.last_state_digest $expectedRecovery.last_state_digest "recovery state digest"
    Assert-Equal $recovery.aborted_intent_count $expectedRecovery.aborted_intent_count "recovery aborted count"
    Assert-Equal $recovery.reconciled_intent_count $expectedRecovery.reconciled_intent_count "recovery reconciled count"
    Assert-Equal $recovery.actions[0].action $expectedRecovery.action "recovery action"
    Assert-Equal $recovery.actions[0].target_ledger_version $expectedRecovery.target_ledger_version "recovery target version"
    Write-Utf8NoBom (Join-Path $Artifacts "recovery-report.json") ($recovery | ConvertTo-Json -Depth 100)

    $stateAfterRecovery = (Invoke-Ledger "GET" "/v1/state").Json.data
    Assert-Equal $stateAfterRecovery.ledger_version $stateBefore.ledger_version "recovery ledger version equality"
    Assert-Equal $stateAfterRecovery.state_digest $stateBefore.state_digest "recovery ledger digest equality"
    $beforeJson = $stateBefore | ConvertTo-Json -Depth 100 -Compress
    $afterJson = $stateAfterRecovery | ConvertTo-Json -Depth 100 -Compress
    Assert-Equal $afterJson $beforeJson "full derived ledger before/after restart"
    Write-Utf8NoBom (Join-Path $Artifacts "state-after-recovery.json") ($stateAfterRecovery | ConvertTo-Json -Depth 100)

    Write-Host ""
    Write-Host "=== Recovery report ==="
    Write-Host ("  last_complete_version={0} state_digest={1} aborted={2} reconciled={3}" -f $recovery.last_complete_version, $recovery.last_state_digest, $recovery.aborted_intent_count, $recovery.reconciled_intent_count)
    foreach ($action in $recovery.actions) {
        Write-Host ("  intent={0} action={1} target_version={2} reason={3}" -f $action.intent_id, $action.action, $action.target_ledger_version, $action.reason)
    }
    Write-Host "  derived ledger before restart == after restart: PASS"
    Write-Host "  before restart:"
    Print-StateSummary $stateBefore
    Write-Host "  after restart:"
    Print-StateSummary $stateAfterRecovery

    $postResp = Invoke-Ledger "POST" "/v1/settle" (Read-Text (Join-Path $SamplesDir "settle-after-recovery.json"))
    Assert-Equal $postResp.Status 200 "post-recovery settlement HTTP"
    $post = $postResp.Json.data
    $expectedPost = Read-Text (Join-Path $ExpectedDir "post-recovery-settlement.json") | ConvertFrom-Json
    Assert-Equal $post.ledger_version $expectedPost.ledger_version "post-recovery version"
    Assert-Equal $post.state_digest $expectedPost.state_digest "post-recovery digest"
    Assert-Equal $post.calculation_digest $expectedPost.calculation_digest "post-recovery calculation digest"
    foreach ($expectedTenant in $expectedPost.tenant_segments) {
        $actualTenant = Find-ItemById $post.tenant_segments $expectedTenant.tenant_id "tenant_id"
        Assert-Equal $actualTenant.unsettled_interest_micros $expectedTenant.unsettled_interest_micros "$($expectedTenant.tenant_id) post-recovery interest"
        Assert-Equal $actualTenant.cumulative_rounding_error_nanos $expectedTenant.cumulative_rounding_error_nanos "$($expectedTenant.tenant_id) post-recovery rounding"
    }
    $postPool = Find-ItemById $post.pools ($expectedPost.pools[0].pool_id) "pool_id"
    Assert-Equal $postPool.used_micros $expectedPost.pools[0].used_micros "post-recovery pool used"
    Assert-Equal $postPool.used_ratio_fixed $expectedPost.pools[0].used_ratio_fixed "post-recovery pool ratio"
    Write-Utf8NoBom (Join-Path $Artifacts "post-recovery-settlement-actual.json") (($post | ConvertTo-Json -Depth 100))
    Write-Host ""
    Write-Host ("=== Post-recovery settlement === version={0} digest={1}" -f $post.ledger_version, $post.state_digest)
    Print-Pool $postPool

    Write-Host ""
    Write-Host "=== SQLite persisted facts ==="
    $sqliteOutput = & sqlite3 (Join-Path $Artifacts "ledger.db") "SELECT ledger_version, state_digest, as_of_minute FROM ledger_versions ORDER BY ledger_version; SELECT request_id,status,phase,target_ledger_version FROM intents ORDER BY created_ms; SELECT ledger_version,pool_id,used_ratio_fixed,overcommit_risk FROM pool_states ORDER BY ledger_version;"
    $sqliteOutput | ForEach-Object { Write-Host "  $_" }
    Write-Utf8NoBom (Join-Path $Artifacts "sqlite-check.txt") (($sqliteOutput -join [Environment]::NewLine) + [Environment]::NewLine)

    Write-Host ""
    Write-Host "=== Result ==="
    Write-Host "PASS: segmented interest, pool overcommit rejection, force-kill recovery, pre/post recovery ledger equality, post-recovery settlement"
    Write-Host ("Artifacts: {0}" -f $Artifacts)
} catch {
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
} finally {
    Stop-Ledger
    $Http.Dispose()
}
