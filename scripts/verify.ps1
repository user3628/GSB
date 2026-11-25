# Windows 10+ / PowerShell verification entry point.
# Requires Go 1.22+ and sqlite3.exe on PATH for manual persistence checks.
$ErrorActionPreference = "Stop"

Write-Host "== Go environment =="
go version
$goos = (go env GOOS)
Write-Host "go env GOOS=$goos"
if ($goos -ne "windows") {
  Write-Warning "Task expects GOOS=windows on the Windows machine; current GOOS is $goos."
}

Write-Host "`n== Download/pin dependencies =="
go mod tidy

Write-Host "`n== Build and test =="
go build ./...
go test ./...

Write-Host "`n== End-to-end and forced-kill recovery verification =="
go run ./cmd/verify

Write-Host "`n== Build Windows binaries =="
New-Item -ItemType Directory -Force -Path .\bin | Out-Null
go build -o .\bin\grayrouter-server.exe .\cmd\server
go build -o .\bin\grayrouter-verify.exe .\cmd\verify

Write-Host "`nDone. Start the workbench with:"
Write-Host "  .\bin\grayrouter-server.exe --addr 127.0.0.1:18080 --db .\data\grayrouter.db --samples-dir .\samples"
Write-Host "Then open http://127.0.0.1:18080/"
