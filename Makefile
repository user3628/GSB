PYTHON := .venv/bin/python
WORKSPACE ?= .lab

.PHONY: setup generate init run status test demo serve clean-runtime

setup:
	python3.11 -m venv .venv
	.venv/bin/pip install -r requirements.txt

generate:
	$(PYTHON) -m recall_lab generate --out data --seed 20261120

init:
	$(PYTHON) -m recall_lab --workspace $(WORKSPACE) init --manifest data/manifest.jsonl --config configs/fusion.yaml --force

run:
	$(PYTHON) -m recall_lab --workspace $(WORKSPACE) run --scales all --until summary

status:
	$(PYTHON) -m recall_lab --workspace $(WORKSPACE) status

test:
	$(PYTHON) -m unittest discover -s tests -v

demo:
	$(PYTHON) scripts/demo.py

serve:
	$(PYTHON) -m recall_lab --workspace $(WORKSPACE) serve --host 127.0.0.1 --port 8765

clean-runtime:
	$(PYTHON) -c "from pathlib import Path; import shutil; [shutil.rmtree(p) for p in map(Path, ['.lab']) if p.exists()]"
