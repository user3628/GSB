$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root
$Bin = if ($env:BIN) { $env:BIN } else { "bin/authlab.exe" }
if (-not (Test-Path $Bin)) {
    go build -trimpath -o $Bin ./cmd/authlab
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("authlab-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    & $Bin experiment --out (Join-Path $Tmp "run-1") @args | Out-Null
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $Bin experiment --out (Join-Path $Tmp "run-2") @args | Out-Null
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $r1 = Get-Content (Join-Path $Tmp "run-1/report.json") -Raw
    $r2 = Get-Content (Join-Path $Tmp "run-2/report.json") -Raw
    if ($r1 -ne $r2) { throw "reports differ" }
    $states1 = Get-ChildItem (Join-Path $Tmp "run-1/states") -File | Sort-Object Name
    $states2 = Get-ChildItem (Join-Path $Tmp "run-2/states") -File | Sort-Object Name
    if ($states1.Count -ne $states2.Count) { throw "state export count differs" }
    for ($i = 0; $i -lt $states1.Count; $i++) {
        if ($states1[$i].Name -ne $states2[$i].Name) { throw "state export names differ" }
        $s1 = Get-Content $states1[$i].FullName -Raw
        $s2 = Get-Content $states2[$i].FullName -Raw
        if ($s1 -ne $s2) { throw "state export differs: $($states1[$i].Name)" }
    }
    Get-FileHash (Join-Path $Tmp "run-1/report.json") -Algorithm SHA256
    Write-Output "deterministic report and state exports are byte-identical"
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
