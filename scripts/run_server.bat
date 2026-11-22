@echo off
REM 资源编排依赖图诊断工作台 - Windows 本地启动脚本
REM 仅绑定回环地址 127.0.0.1，不访问任何线上服务或外部账号。
setlocal
cd /d %~dp0..
if "%GRAPH_BENCH_DB%"=="" set GRAPH_BENCH_DB=%cd%\data\graph_bench.db
if "%GRAPH_BENCH_HOST%"=="" set GRAPH_BENCH_HOST=127.0.0.1
if "%GRAPH_BENCH_PORT%"=="" set GRAPH_BENCH_PORT=8080
python -m uvicorn graph_bench.api:app --host %GRAPH_BENCH_HOST% --port %GRAPH_BENCH_PORT%
endlocal
