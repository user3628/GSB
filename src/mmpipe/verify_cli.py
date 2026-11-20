"""mmpipe-verify 验证工具命令行入口。"""

import argparse
import json

from .storage import Workspace
from .verifier import compare_products, verify_workspace


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mmpipe-verify")
    parser.add_argument("-w", "--workspace", default="mmpipe_workspace")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("path_a")
    p_cmp.add_argument("path_b")
    args = parser.parse_args(argv)

    if args.command == "check":
        report = verify_workspace(Workspace(args.workspace))
    else:
        report = compare_products(args.path_a, args.path_b)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
