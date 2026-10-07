import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Pleiades read-only dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8080, type=int)
    parser.add_argument("--demo", action="store_true", help="Use explicitly labeled sample data")
    parser.add_argument("--root-path", default="", help="Prefix stripped by the reverse proxy")
    args = parser.parse_args()
    if args.demo:
        os.environ["PLEIADES_DASHBOARD_DEMO"] = "1"
    uvicorn.run(
        "pleiades_dashboard.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        root_path=args.root_path,
    )


if __name__ == "__main__":
    main()
