from __future__ import annotations

import argparse
import json
import uvicorn

from .api import create_app
from .config import Settings
from .sample_data import generate_large, generate_small, seed_dataset
from .service import GatewayService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local gateway middleware API")
    parser.add_argument("--db", default="data/gateway.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--seed-sample", choices=("small", "large"), default=None)
    parser.add_argument("--large-events", type=int, default=100_000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env().with_db(args.db)
    if args.seed_sample:
        service = GatewayService(settings)
        dataset = (
            generate_small(settings)
            if args.seed_sample == "small"
            else generate_large(settings, event_count=args.large_events)
        )
        version = seed_dataset(service, dataset, allow_existing=True)
        print(json.dumps({"seeded": dataset.name, "snapshot_version": version}, ensure_ascii=False))
    if args.reload:
        os.environ["GW_DB_PATH"] = str(settings.db_path)
        uvicorn.run(
            "gateway_middleware.api:app",
            host=args.host,
            port=args.port,
            reload=True,
        )
    else:
        uvicorn.run(
            create_app(settings=settings),
            host=args.host,
            port=args.port,
        )


if __name__ == "__main__":
    main()
