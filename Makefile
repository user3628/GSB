PYTHON ?= .venv/bin/python
VAD ?= .venv/bin/vadkit

.PHONY: install generate demo test verify

install:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -e .

generate:
	$(VAD) generate --out data

demo:
	$(VAD) demo --data data --out artifacts/demo --tolerance-ms 80

test:
	$(PYTHON) -m pytest

verify: test
	$(PYTHON) -m compileall -q src tests
	$(PYTHON) -m pip check
