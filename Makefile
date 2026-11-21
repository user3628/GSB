PY := .venv/bin/python

.PHONY: bootstrap test demo serve clean

bootstrap:
	./scripts/bootstrap.sh

test:
	$(PY) -m unittest discover -s tests -v

demo:
	./run.sh --root runtime demo

serve:
	./run.sh --root runtime serve --port 8767

clean:
	rm -rf runtime

