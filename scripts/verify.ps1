$ErrorActionPreference = "Stop"

Write-Host "== Go version =="
go version

Write-Host "== Offline tests =="
$env:GOPROXY = "off"
go test ./...

Write-Host "== Vet =="
go vet ./...

Write-Host "== Deterministic demo + incremental verification =="
go run ./cmd/speakerdiar run --workdir .\demo-workspace --verify-incremental

Write-Host "== Artifact verification =="
go run ./cmd/speakerdiar verify --workdir .\demo-workspace

Write-Host "All verification steps passed."
