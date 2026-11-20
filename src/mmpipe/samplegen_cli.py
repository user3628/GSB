"""样本生成命令行入口：python -m mmpipe.samplegen_cli [output_dir]"""

import json
import sys

from .samplegen import generate


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    out_dir = argv[0] if argv else "samples"
    info = generate(out_dir)
    print(json.dumps({"ok": True, "outputs": info}, ensure_ascii=False,
                     indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
