from __future__ import annotations

import argparse
import os

import uvicorn


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Semantic News Novelty FastAPI service"
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="recreate the database and seed demo users and articles",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.demo:
        os.environ["DEMO_MODE"] = "true"
    uvicorn.run("api.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
