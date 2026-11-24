$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root
$Work = Join-Path ([System.IO.Path]::GetTempPath()) ("lagengine-verify-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $Work | Out-Null
$Port = if ($env:PORT) { [int]$env:PORT } else { 18081 }
$Server = $null

try {
    Write-Host '== Go version =='
    go version
    Write-Host '== Vet and tests =='
    go vet ./...
    go test ./... -count=1

    Write-Host '== Windows build (CGO disabled) =='
    $env:CGO_ENABLED = '0'
    go build -trimpath -o (Join-Path $Work 'lagengine.exe') ./cmd/lagengine
    Get-Item (Join-Path $Work 'lagengine.exe') | Select-Object Length, FullName

    Write-Host '== Deterministic samples and seed import =='
    go run ./cmd/samplegen -out ./samples
    $Exe = Join-Path $Work 'lagengine.exe'
    $Db = Join-Path $Work 'verify.db'
    & $Exe -db $Db -compat ./samples/compat.json -import ./samples/observations.jsonl -format jsonl -batch verify-seed |
        Tee-Object -Variable ImportJson | Out-Null
    if ($ImportJson -notmatch '"observation_count":13' -or $ImportJson -notmatch '"version":1') {
        throw 'seed import assertion failed'
    }

    Write-Host '== Legacy degrade =='
    & $Exe -db $Db -compat ./samples/compat.json -import ./samples/legacy.jsonl -format jsonl -batch verify-legacy |
        Tee-Object -Variable LegacyJson | Out-Null
    if ($LegacyJson -notmatch '"compatibility":"DEGRADE"') { throw 'legacy degrade assertion failed' }

    Write-Host '== Forced termination =='
    $env:LAGENGINE_CRASH_AFTER_INTENT = '1'
    $Crash = Start-Process -FilePath $Exe -PassThru -Wait -NoNewWindow -ArgumentList @(
        '-db', $Db, '-compat', './samples/compat.json',
        '-import', './samples/legacy.jsonl', '-format', 'jsonl', '-batch', 'verify-crashed'
    )
    Remove-Item Env:LAGENGINE_CRASH_AFTER_INTENT
    if ($Crash.ExitCode -ne 137) { throw "expected exit 137, got $($Crash.ExitCode)" }

    Write-Host '== Restart, recovery report and HTTP smoke =='
    $Server = Start-Process -FilePath $Exe -PassThru -NoNewWindow -ArgumentList @(
        '-db', $Db, '-compat', './samples/compat.json', '-addr', "127.0.0.1:$Port"
    )
    $Base = "http://127.0.0.1:$Port"
    $Ready = $false
    for ($i = 0; $i -lt 100; $i++) {
        try {
            Invoke-RestMethod "$Base/healthz" | Out-Null
            $Ready = $true
            break
        } catch { Start-Sleep -Milliseconds 100 }
    }
    if (-not $Ready) { throw 'HTTP server did not become ready' }

    $Recovery = Invoke-RestMethod "$Base/v1/recovery/report"
    if ($Recovery.recovery.pending_intents_found -ne 1 -or
        $Recovery.recovery.events[0].action -ne 'ABORT_PENDING_INTENT' -or
        $Recovery.recovery.last_complete_version -ne 2) {
        throw 'recovery report assertion failed'
    }
    $Sequence = Invoke-RestMethod "$Base/v1/replicas/replica-a/sequence"
    if ($Sequence.sequence.current_state -ne 'HEALTHY') { throw 'sequence assertion failed' }
    $Export = Invoke-WebRequest "$Base/v1/export?format=binary" -OutFile (Join-Path $Work 'export.bin') -PassThru
    if (($Export.Headers['X-Engine-Version'] | Select-Object -First 1) -ne '2') { throw 'export version header assertion failed' }

    Write-Host "PASS: recovery aborted pending batch at version 2; binary bytes=$((Get-Item (Join-Path $Work 'export.bin')).Length)"
}
finally {
    if ($Server -and -not $Server.HasExited) { Stop-Process -Id $Server.Id -Force }
    Remove-Item -Recurse -Force $Work -ErrorAction SilentlyContinue
}

