.PHONY: setup samples test demo verify serve

PYTHON ?= .venv/bin/python

setup:
	bash scripts/setup_local.sh

samples:
	$(PYTHON) scripts/build_samples.py

test:
	$(PYTHON) -m unittest discover -s tests -t . -v

demo:
	bash scripts/demo_all.sh

verify:
	$(PYTHON) scripts/verify_all.py

serve:
	$(PYTHON) -m contract_elements.cli serve
