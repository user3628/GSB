from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def tcp_free(host: str, port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def udp_free(host: str, port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def parse_address(address: str) -> tuple[str, int]:
    host, port = address.rsplit(":", 1)
    host = "127.0.0.1" if host in {"localhost", "::1"} else host.strip("[]")
    return host, int(port)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check loopback ports used by the gateway demo")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--http-port", type=int, default=18765)
    parser.add_argument("--topology", default=str(ROOT / "samples" / "topology.json"))
    args = parser.parse_args()
    topology = json.loads(Path(args.topology).read_text(encoding="utf-8"))
    checks: list[dict[str, object]] = [
        {
            "name": "HTTP gateway",
            "protocol": "tcp",
            "address": f"{args.host}:{args.http_port}",
            "free": tcp_free(args.host, args.http_port),
        }
    ]
    for name, item in topology["carriers"].items():
        carrier_host, carrier_port = parse_address(item["listen"])
        egress_host, egress_port = parse_address(item["egress"])
        checks.append(
            {
                "name": f"carrier-{name}",
                "protocol": "udp",
                "address": f"{carrier_host}:{carrier_port}",
                "free": udp_free(carrier_host, carrier_port),
            }
        )
        checks.append(
            {
                "name": f"decap-{name}",
                "protocol": "udp",
                "address": f"{egress_host}:{egress_port}",
                "free": udp_free(egress_host, egress_port),
            }
        )
    for item in checks:
        print(
            f"{'FREE' if item['free'] else 'BUSY':<5} "
            f"{item['protocol'].upper():<3} {item['address']:<24} {item['name']}"
        )
    if not all(item["free"] for item in checks):
        print("One or more reserved loopback ports are busy.")
        return 1
    print("All reserved loopback ports are available.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

