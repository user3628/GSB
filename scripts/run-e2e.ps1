param(
    [int]$Port = 18080,
    [string]$DbPath = "data\e2e-verify.db"
)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
New-Item -ItemType Directory -Force -Path bin, data, data\logs, samples | Out-Null
$exe = Join-Path $root "bin\meshpolicy.exe"
if ([System.IO.Path]::IsPathRooted($DbPath)) { $dbFull = [System.IO.Path]::GetFullPath($DbPath) } else { $dbFull = [System.IO.Path]::GetFullPath((Join-Path $root $DbPath)) }
go build -o $exe .\cmd\meshpolicy
if ($LASTEXITCODE -ne 0) { throw "go build failed" }
& $exe samples -out samples | Out-Null
Get-ChildItem -Path ([System.IO.Path]::GetDirectoryName($dbFull)) -Filter (([System.IO.Path]::GetFileName($dbFull)) + "*") -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
$serverArgs = 'serve -addr "127.0.0.1:{0}" -db "{1}"' -f $Port, $dbFull
$server = Start-Process -FilePath $exe -ArgumentList $serverArgs -PassThru -RedirectStandardOutput "data\logs\e2e.out.log" -RedirectStandardError "data\logs\e2e.err.log"
try {
    $base = "http://127.0.0.1:$Port"
    $healthy = $false
    for ($i = 0; $i -lt 120; $i++) {
        try { Invoke-RestMethod -Uri "$base/healthz" -TimeoutSec 1 | Out-Null; $healthy = $true; break } catch { Start-Sleep -Milliseconds 250 }
    }
    if (-not $healthy) { throw "server did not become healthy" }
    & $exe verify -base-url $base -samples samples
    if ($LASTEXITCODE -ne 0) { throw "verification failed" }
} finally {
    if ($server -and -not $server.HasExited) { Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue }
}
