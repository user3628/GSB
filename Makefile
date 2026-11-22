PYTHON ?= python3
DATA ?= data
WORKSPACE ?= demo_workspace

.PHONY: doctor data verify test run compare demo

doctor:
	$(PYTHON) -m pipeline.cli doctor --workspace .

data:
	$(PYTHON) -m pipeline.cli generate-data --out $(DATA)

verify: data
	$(PYTHON) -m pipeline.cli doctor --workspace .
	$(PYTHON) -m pipeline.cli verify-data --index $(DATA)/index.json --output $(DATA)/verification.json
	$(PYTHON) -m unittest discover -v

test:
	$(PYTHON) -m unittest discover -v

run:
	$(PYTHON) -m pipeline.cli run --workspace $(WORKSPACE) --config $(DATA)/config.yaml --data $(DATA)/index.json

compare:
	$(PYTHON) -m pipeline.cli compare --left $(LEFT) --right $(RIGHT)

demo: verify run
