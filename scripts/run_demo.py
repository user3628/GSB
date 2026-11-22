#!/usr/bin/env python3
from blockgemm.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["demo", *__import__("sys").argv[1:]]))
