[CmdletBinding()]
param(
    [string]$Db = "artifacts\slo-budget.db",
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 8765,
    [Nullable[Int64]]$FixedClockMs = $null
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = Join-Path $Root ".venv/bin/python"
}

$Arguments = @(
    "-m", "slo_budget.server",
    "--db", $Db,
    "--host", $HostAddress,
    "--port", $Port
)
if ($null -ne $FixedClockMs) {
    $Arguments += @("--fixed-clock-ms", $FixedClockMs.ToString())
}
& $Python @Arguments
