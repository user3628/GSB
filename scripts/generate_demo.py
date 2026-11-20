from pathlib import Path

from qworkbench.generator import generate_demo


if __name__ == "__main__":
    print(generate_demo(Path("data/demo"), seed=20261120, shards=4))
