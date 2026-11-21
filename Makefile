.PHONY: test demo init-data serve

test:
	python -m pytest

init-data:
	python -m chunkops init-data --root data --overwrite

demo:
	python -m chunkops demo --root . --overwrite

serve:
	python -m chunkops serve --host 127.0.0.1 --port 8765
