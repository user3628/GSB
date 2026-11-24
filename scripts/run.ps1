param(
    [string]$Addr = $(if ($env:CHECKPOINT_ENGINE_ADDR) { $env:CHECKPOINT_ENGINE_ADDR } else { "127.0.0.1:18080" }),
    [string]$Db = $(if ($env:CHECKPOINT_ENGINE_DB) { $env:CHECKPOINT_ENGINE_DB } else { "data/checkpoint-engine.db" }),
    [string]$DataDir = $(if ($env:CHECKPOINT_ENGINE_DATA_DIR) { $env:CHECKPOINT_ENGINE_DATA_DIR } else { "data" })
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
go run ./cmd/checkpoint-engine -addr $Addr -db $Db -data-dir $DataDir
