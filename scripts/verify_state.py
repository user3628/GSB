from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eventbus.verifier import StateVerifier


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify durable event-bus state.")
    parser.add_argument("--db", default="var/eventbus.db")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    result = StateVerifier(args.db).run(topic_id=args.topic)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
