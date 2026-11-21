#!/usr/bin/env python3

import json

from kde_drift.demo import run_demo


if __name__ == "__main__":
    result = run_demo()
    print(json.dumps({"all_checks_passed": result["all_checks_passed"], "checks": result["checks"], "demo_digest": result["demo_digest"]}, indent=2))
