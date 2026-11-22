#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m venv --system-site-packages .venv
.venv/bin/python - <<'PY'
import sqlite3
import fastapi
import httpx
import numpy
import pytest
import uvicorn

print("fastapi", fastapi.__version__)
print("uvicorn", uvicorn.__version__)
print("numpy", numpy.__version__)
print("pytest", pytest.__version__)
print("httpx", httpx.__version__)
print("sqlite3", sqlite3.sqlite_version)
PY

# A system-site-packages venv can import local packages but does not copy their
# console entry points. Provide thin local entry points for the documented tools.
for command in pytest uvicorn; do
  cat >".venv/bin/${command}" <<EOF
#!/usr/bin/env bash
exec "\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)/python" -m ${command} "\$@"
EOF
  chmod +x ".venv/bin/${command}"
done

