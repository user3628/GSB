@echo off
REM 可重复运行的演示脚本（Windows 命令行）：导入、导出、回放、冲突回滚、快照切换与恢复演练。
setlocal EnableDelayedExpansion
cd /d "%~dp0\.."

set GW=http://127.0.0.1:8080
set GATEWAY_PORT=8080
set DATA_DIR=data
set GATEWAY_ENABLE_DEBUG_CRASH=1

echo === 构建 ===
go build -o bin\gateway.exe .\cmd\gateway || exit /b 1
go build -o bin\backend.exe .\cmd\backend || exit /b 1
go build -o bin\gensamples.exe .\cmd\gensamples || exit /b 1

echo === 生成确定性样本 ===
bin\gensamples.exe samples || exit /b 1

echo === 启动本地后端测试服务 (9001/9002) ===
set BACKEND_PORT=9001& set BACKEND_NAME=backend-9001& start /b "" bin\backend.exe
set BACKEND_PORT=9002& set BACKEND_NAME=backend-9002& start /b "" bin\backend.exe

echo === 启动网关 ===
if exist data rmdir /s /q data
mkdir data
set GATEWAY_PORT=8080& start /b "" bin\gateway.exe
timeout /t 2 /nobreak >nul

echo === 1. 导入 JSONL 路由表 (v2, truncate) ===
curl -s -X POST "%GW%/api/import?trunc=truncate" --data-binary @samples\routes_v2.jsonl

echo === 2. 导入二进制路由表 (v2, 幂等跳过) ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\routes_v2.bin

echo === 3. 导出两种格式 ===
curl -s "%GW%/api/export?format=bin" -o %TEMP%\exp.bin
curl -s "%GW%/api/export?format=jsonl" -o %TEMP%\exp.jsonl
echo 导出完成

echo === 4. 旧版 v1 降级导入 ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\legacy_v1.jsonl

echo === 5. 高版本 v3 拒绝裁决 (E_FORMAT_UNSUPPORTED) ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\future_v3.jsonl

echo === 6. 损坏记录 (E_RECORD_CORRUPT 含偏移) ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\corrupt_crc.bin
curl -s -X POST "%GW%/api/import" --data-binary @samples\corrupt_len.bin

echo === 7. 显存溢出 (E_UNIT_OVERFLOW) ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\overflow.jsonl

echo === 8. 冲突导入：覆盖与整批回滚 (E_CONFLICTING_IMPORT) ===
curl -s -X POST "%GW%/api/import" --data-binary @samples\override.jsonl
curl -s -X POST "%GW%/api/import" --data-binary @samples\conflict.jsonl
echo 回滚后 llm-omega 不应存在:
curl -s "%GW%/api/route/llm-omega"

echo === 9. 确定性回放 (seed=42, 两次逐条一致) ===
curl -s -X POST "%GW%/api/replay" -d "{\"seed\":42,\"count\":8}" > %TEMP%\replay1.json
curl -s -X POST "%GW%/api/replay" -d "{\"seed\":42,\"count\":8}" > %TEMP%\replay2.json
fc /b %TEMP%\replay1.json %TEMP%\replay2.json >nul && echo 回放确定性校验: PASS || echo 回放确定性校验: FAIL
type %TEMP%\replay1.json

echo === 10. 命中统计 ===
curl -s "%GW%/api/stats"

echo === 11. 快照校验与切换 (E_SNAPSHOT_INVALID) ===
curl -s -X POST "%GW%/api/snapshot" -d "{\"path\":\"samples/snapshot_stale.json\"}"
curl -s -X POST "%GW%/api/snapshot" -d "{\"path\":\"samples/snapshot_badver.json\"}"
curl -s -X POST "%GW%/api/snapshot" -d "{\"path\":\"samples/snapshot_1.json\"}"

echo === 12. 切换后路由与命中统计 ===
curl -s "%GW%/api/stats"
curl -s -X POST "%GW%/api/replay" -d "{\"seed\":42,\"count\":4}"

echo === 13. 后端不可达 (E_BACKEND_UNAVAILABLE) ===
taskkill /f /im backend.exe /fi "WINDOWTITLE eq backend-9001*" >nul 2>&1
curl -s -X POST "%GW%/api/forward" -d "{\"model_id\":\"llm-sigma\",\"payload\":\"x\"}"

echo === 14. 强制终止恢复演练 ===
curl -s -X POST "%GW%/api/debug/crash"
timeout /t 2 /nobreak >nul
start /b "" bin\gateway.exe
timeout /t 2 /nobreak >nul
curl -s "%GW%/api/recovery"
curl -s "%GW%/api/route/llm-alpha"

echo === 演示完成 ===
endlocal
