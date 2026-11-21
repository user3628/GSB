.PHONY: test demo serve

test:
	python3 -m pytest -q

demo:
	python3 -m cabin_voice --workspace ./demo_workspace demo --summary ./artifacts/demo_summary.json

serve:
	python3 -m cabin_voice --workspace ./demo_workspace serve --host 127.0.0.1 --port 8765
