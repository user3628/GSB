PYTHON ?= python3
VENV ?= .venv
PY := $(VENV)/bin/python
DETDIFF := $(VENV)/bin/detdiff
WORKSPACE ?= demo_workspace

.PHONY: setup test demo status diagnostics verify clean-demo

setup:
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --disable-pip-version-check -e .

test:
	$(PY) -m unittest discover -s tests -v

demo:
	$(DETDIFF) demo --workspace $(WORKSPACE) --reset

status:
	$(DETDIFF) status --workspace $(WORKSPACE)

diagnostics:
	$(DETDIFF) diagnostics --workspace $(WORKSPACE) --html $(WORKSPACE)/diagnostics.html

verify:
	$(DETDIFF) verify --workspace $(WORKSPACE)

clean-demo:
	rm -rf -- $(WORKSPACE)

