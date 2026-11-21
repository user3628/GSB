PYTHON := .venv/bin/python

.PHONY: setup generate test demo serve validate

setup:
	python3.11 -m venv .venv
	$(PYTHON) -m pip install -r requirements.txt

generate:
	$(PYTHON) -m kde_drift generate --out data

validate:
	$(PYTHON) -m kde_drift validate --data-dir data

test:
	$(PYTHON) -m unittest discover -s tests -t . -v

demo:
	$(PYTHON) -m kde_drift demo --out runs/demo

serve:
	$(PYTHON) -m kde_drift serve --data-dir data --workdir-base runs/http --port 0
