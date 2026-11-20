.PHONY: generate test demo verify serve

generate:
	python3 scripts/generate_samples.py

test:
	python3 -m pytest

demo:
	python3 scripts/run_demo.py --workspace runtime/demo --write-expected

verify:
	python3 scripts/verify_demo.py

serve:
	python3 -m linreg_workbench serve --workspace runtime/web --port 8765
