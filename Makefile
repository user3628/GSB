.PHONY: generate test verify demo

generate:
	python3 scripts/generate_samples.py --out data

test:
	python3 -m unittest discover -s tests -v

verify:
	python3 scripts/verify_demo.py

demo: generate test verify
