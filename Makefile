PYTHON ?= python3

.PHONY: demo test verify generate run serve doctor

demo: doctor generate run verify

doctor:
	$(PYTHON) -m shotcheck.cli doctor

generate:
	$(PYTHON) -m shotcheck.cli generate --root runtime --seed 20261123

run:
	$(PYTHON) -m shotcheck.cli run --workspace runtime/small
	$(PYTHON) -m shotcheck.cli run --workspace runtime/large

verify:
	$(PYTHON) -m shotcheck.cli verify --root runtime --seed 20261123

test:
	$(PYTHON) -m unittest discover -s tests -v

serve:
	$(PYTHON) -m shotcheck.cli serve --workspace runtime/large --port 18765
