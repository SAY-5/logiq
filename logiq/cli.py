"""Command-line entry points for the server and the file tailer."""

from __future__ import annotations

import argparse
import sys

from .api import build_state, create_app
from .ingest import Ingestor, tail_file
from .store import Store
from .wal import WriteAheadLog


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    state = build_state(args.db, args.wal)
    app = create_app(state)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _tail(args: argparse.Namespace) -> int:
    store = Store(args.db)
    wal = WriteAheadLog(args.wal)
    ingestor = Ingestor(store, wal)
    ingestor.recover()
    n = tail_file(args.file, args.source, ingestor, batch_size=args.batch_size)
    print(f"ingested {n} records from {args.file}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="logiq", description="LogIQ service")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="run the REST API")
    p_serve.add_argument("--db", default="logiq.db")
    p_serve.add_argument("--wal", default="logiq.wal")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_serve)

    p_tail = sub.add_parser("tail", help="ingest a log file")
    p_tail.add_argument("file")
    p_tail.add_argument("--source", required=True)
    p_tail.add_argument("--db", default="logiq.db")
    p_tail.add_argument("--wal", default="logiq.wal")
    p_tail.add_argument("--batch-size", type=int, default=100)
    p_tail.set_defaults(func=_tail)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
