"""Loopback-only HTTP diagnostic interface."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .pipeline import Pipeline
from .config import Config
from .injection import Injection
from .checkpoints import STAGE_ORDER
from .hashing import canonical


def _make_handler(work_dir):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            return

        def _send(self, payload, status=200):
            body = canonical(payload).encode("utf-8") + b"\n"
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _pipeline(self, query):
            injection = Injection(
                kind=query.get("injection", ["none"])[0],
                image_id=query.get("image_id", [""])[0],
                value=float(query.get("value", ["-1.0"])[0]),
            )
            return Pipeline(work_dir, Config().validate(), injection)

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            pipeline = Pipeline(work_dir)
            if parsed.path == "/health":
                self._send({"status": "ok"})
            elif parsed.path == "/status":
                self._send(pipeline.status())
            elif parsed.path == "/fingerprints":
                status = pipeline.status()
                self._send(
                    {
                        item["image_id"]: {
                            stage: None if detail is None else detail["fingerprint"]
                            for stage, detail in item["stages"].items()
                        }
                        for item in status["images"]
                    }
                )
            elif parsed.path == "/scores":
                payload = []
                for metadata in pipeline.load_metadata():
                    image_id = metadata["image_id"]
                    entry = {"image_id": image_id, "stages": {}}
                    for stage in STAGE_ORDER:
                        record = pipeline.store.load(image_id, stage)
                        entry["stages"][stage] = None if record is None else record["product"]
                    payload.append(entry)
                self._send(payload)
            elif parsed.path == "/diagnostics":
                path = f"{work_dir}/outputs/diagnostics.json"
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        self._send(json.loads(handle.read()))
                except FileNotFoundError:
                    self._send({"diagnostics": [], "out_of_range": [], "invalidations": []})
            else:
                self._send({"error": "not found"}, status=404)

        def do_POST(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/run":
                summary = self._pipeline(query).run()
                self._send(summary)
            elif parsed.path == "/stop":
                Pipeline(work_dir).store.request_stop()
                self._send({"stop_requested": True})
            elif parsed.path == "/resume":
                pipeline = Pipeline(work_dir)
                pipeline.store.clear_stop()
                self._send(pipeline.run())
            elif parsed.path == "/import":
                import sys

                sys.path.insert(0, f"{work_dir}/../scripts")
                from generate_samples import main as generate_main

                generate_main(work_dir)
                self._send({"imported": True, "work_dir": work_dir})
            else:
                self._send({"error": "not found"}, status=404)

    return Handler


def serve(work_dir, host="127.0.0.1", port=8765):
    server = ThreadingHTTPServer((host, port), _make_handler(work_dir))
    print(f"IQA diagnostic HTTP interface listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
