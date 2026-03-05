from __future__ import annotations

import argparse
import logging
import sys

from .config import load_config
from .db import connect_db, get_latest_scan_run_ids, init_db
from .pipeline import run_all, run_news, run_report, run_scan, run_score
from .reporting import export_report_csv


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level)
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Newstrade CLI")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan price movers")
    scan_parser.add_argument("--window", choices=["1d", "intraday"], default=None)
    scan_parser.add_argument("--mode", choices=["env", "ibkr", "both"], default=None)

    news_parser = subparsers.add_parser("news", help="Fetch news for a scan run")
    news_parser.add_argument("--scan-run-id", type=int, default=None)

    score_parser = subparsers.add_parser("score", help="AI score news for a scan run")
    score_parser.add_argument("--scan-run-id", type=int, default=None)

    report_parser = subparsers.add_parser("report", help="Print ranked symbol report")
    report_parser.add_argument("--scan-run-id", type=int, default=None)
    report_parser.add_argument("--top", type=int, default=30)

    run_all_parser = subparsers.add_parser("run-all", help="Run scan -> news -> score -> report")
    run_all_parser.add_argument("--window", choices=["1d", "intraday"], default=None)
    run_all_parser.add_argument("--mode", choices=["env", "ibkr", "both"], default=None)
    run_all_parser.add_argument("--top", type=int, default=30)

    export_parser = subparsers.add_parser("export", help="Export report as CSV")
    export_parser.add_argument("--scan-run-id", type=int, default=None)
    export_parser.add_argument("--format", choices=["csv"], default="csv")

    return parser


def resolve_scan_run_id(config, scan_run_id: int | None) -> int:
    if scan_run_id is not None:
        return scan_run_id

    conn = connect_db(config.db_path)
    init_db(conn)
    latest_ids = get_latest_scan_run_ids(conn, limit=1)
    conn.close()

    if not latest_ids:
        raise ValueError("No scan runs found. Run `newstrade scan` first or pass --scan-run-id explicitly.")
    return latest_ids[0]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.env_file)
    configure_logging(config.log_level)

    if args.command == "scan":
        run_id = run_scan(config=config, window=args.window, mode=args.mode)
        print(f"scan_run_id={run_id}")
        return 0

    if args.command == "news":
        try:
            scan_run_id = resolve_scan_run_id(config=config, scan_run_id=args.scan_run_id)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        inserted = run_news(config=config, scan_run_id=scan_run_id)
        print(f"Inserted {inserted} news articles for scan_run_id={scan_run_id}")
        return 0

    if args.command == "score":
        try:
            scan_run_id = resolve_scan_run_id(config=config, scan_run_id=args.scan_run_id)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2

        def _print_score_progress(event: dict[str, object]) -> None:
            current = int(event.get("current", 0))
            total = int(event.get("total", 0))
            status = str(event.get("status", "ok")).upper()
            symbol = str(event.get("symbol", ""))
            url = str(event.get("url", ""))
            title = str(event.get("title", ""))
            impact_direction = str(event.get("impact_direction", ""))
            main_symbol = str(event.get("main_symbol", ""))
            relevance_score = event.get("relevance_score", "")
            print(f"[{current}/{total}] {status}")
            print(f"symbol: {symbol}")
            print(f"url: {url}")
            print(f"title: {title}")
            print(f"impact_direction: {impact_direction}")
            print(f"main_symbol: {main_symbol}")
            print(f"relevance_score: {relevance_score}")
            print(
                "tokens: "
                f"prompt={event.get('prompt_tokens', '')} "
                f"completion={event.get('completion_tokens', '')} "
                f"total={event.get('total_tokens', '')} "
                f"reasoning={event.get('reasoning_tokens', '')}"
            )
            if status == "ERROR":
                print(f"error: {event.get('error_message', '')}")
            print("")

        scored_articles, symbol_scores = run_score(
            config=config,
            scan_run_id=scan_run_id,
            progress_callback=_print_score_progress,
        )
        print(
            f"Scored {scored_articles} article(s) and updated {symbol_scores} symbol score(s) "
            f"for scan_run_id={scan_run_id}"
        )
        return 0

    if args.command == "report":
        try:
            scan_run_id = resolve_scan_run_id(config=config, scan_run_id=args.scan_run_id)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        report_text = run_report(config=config, scan_run_id=scan_run_id, top=args.top)
        print(report_text)
        return 0

    if args.command == "run-all":
        run_id, report_text = run_all(config=config, window=args.window, mode=args.mode, top=args.top)
        print(f"scan_run_id={run_id}")
        print(report_text)
        return 0

    if args.command == "export":
        try:
            scan_run_id = resolve_scan_run_id(config=config, scan_run_id=args.scan_run_id)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        conn = connect_db(config.db_path)
        init_db(conn)
        path = export_report_csv(conn, scan_run_id=scan_run_id, export_dir=config.csv_export_dir)
        conn.close()
        print(f"Exported CSV: {path}")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
