PYTHON ?= python3

.PHONY: setup generate doctor run status test acceptance

setup:
	$(PYTHON) -m pip install -r requirements.txt

generate:
	$(PYTHON) -m mtlab generate --workspace . --seed 20261123

doctor:
	$(PYTHON) -m mtlab doctor --workspace .

run:
	$(PYTHON) -m mtlab run --workspace . --data-dir data

status:
	$(PYTHON) -m mtlab status --workspace . --data-dir data

test:
	$(PYTHON) -m unittest discover -s tests -v

acceptance:
	$(PYTHON) scripts/acceptance.py

