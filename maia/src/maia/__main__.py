"""
Entry point for running Maia flows via python -m maia
"""

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

from maia import __version__
from maia.purge import purge_short_videos, purge_transcripts
from maia.quality_report import report_ingestion_quality
from maia.registry import AGENT_REGISTRY


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for Maia.

    ``force=True`` reclaims the root logger if an imported dependency (e.g.
    prefect) already installed a handler, so CLI runs actually emit logs.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def main(args: list[str] | None = None) -> int:
    """CLI entry point. Builds one subparser per registered agent plus the
    maintenance commands (purge, purge-transcripts, quality-report).
    """
    parser = argparse.ArgumentParser(
        prog="maia",
        description="Maia - The Stateless Agent Layer for Project Pleiades",
    )
    parser.add_argument("--version", action="version", version=f"Maia v{__version__}")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available agents")

    for agent_name, agent_class in AGENT_REGISTRY.items():
        agent_parser = subparsers.add_parser(
            agent_name, help=f"Run the {agent_name.capitalize()} agent"
        )
        agent_class.add_cli_args(agent_parser)

    purge_parser = subparsers.add_parser(
        "purge", help="Purge short / low-quality videos (< threshold) from DB + vault"
    )
    purge_parser.add_argument(
        "--min-duration",
        type=int,
        default=180,
        help="Purge videos shorter than this many seconds (default: 180 = 3 min).",
    )
    purge_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be removed; delete nothing (default behaviour).",
    )
    purge_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the deletion. Required unless --dry-run is passed.",
    )
    purge_parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Delete DB rows only; leave vault artifact files in place.",
    )

    transcript_purge_parser = subparsers.add_parser(
        "purge-transcripts",
        help="Reset (uncheck) transcripts for a scoped set of videos (DB only, no delete).",
    )
    transcript_purge_parser.add_argument(
        "--scope",
        default="without_visuals",
        choices=["all", "without_visuals", "without_audio", "pending"],
        help="Which transcripts to reset (default: without_visuals).",
    )
    transcript_purge_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be reset; change nothing (default behaviour).",
    )
    transcript_purge_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the reset. Required unless --dry-run is passed.",
    )

    report_parser = subparsers.add_parser(
        "quality-report", help="Print ingestion-quality statistics for the corpus."
    )
    report_parser.add_argument(
        "--json", action="store_true", help="Emit raw JSON instead of a formatted report."
    )

    parsed_args = parser.parse_args(args)

    if not parsed_args.command:
        parser.print_help()
        return 1

    setup_logging(parsed_args.log_level)
    logger = logging.getLogger(__name__)

    command = parsed_args.command

    if command == "purge":
        if not parsed_args.dry_run and not parsed_args.confirm:
            parser.error("purge requires --confirm (or pass --dry-run to preview).")
        result = asyncio.run(
            purge_short_videos(
                min_duration=parsed_args.min_duration,
                dry_run=parsed_args.dry_run,
                delete_artifacts=not parsed_args.keep_artifacts,
            )
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if command == "purge-transcripts":
        if not parsed_args.dry_run and not parsed_args.confirm:
            parser.error("purge-transcripts requires --confirm (or pass --dry-run to preview).")
        result = asyncio.run(
            purge_transcripts(
                scope=parsed_args.scope,
                dry_run=parsed_args.dry_run,
            )
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if command == "quality-report":
        report = asyncio.run(report_ingestion_quality())
        if parsed_args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            _print_quality_report(report)
        return 0

    try:
        agent_class = AGENT_REGISTRY[command]
        agent = agent_class()

        kwargs = {k: v for k, v in vars(parsed_args).items() if k not in ["command", "log_level"]}

        logger.info(f"Starting Maia {command.capitalize()} agent")
        result = asyncio.run(agent.run(**kwargs))
        logger.info(f"{command.capitalize()} completed: {result}")
        return 0

    except KeyboardInterrupt:
        logger.info("Maia stopped by user (SIGINT)")
        return 130

    except Exception as e:
        logger.exception(f"Maia failed with error: {e}")
        return 1


def _print_quality_report(report: dict[str, Any]) -> None:
    """Render the ingestion-quality report as a human-readable summary."""
    total = report.get("total_videos", 0)
    shorts = report.get("shorts_under_3m", 0)
    pct = (
        (shorts / report["total_with_duration"] * 100) if report.get("total_with_duration") else 0.0
    )
    print("=== Pleiades Ingestion Quality ===")
    print(f"Total videos           : {total}")
    print(f"With duration          : {report.get('total_with_duration', 0)}")
    print(f"Shorts (<3 min)        : {shorts} ({pct:.1f}% of durationed)")
    print("Duration mix           :")
    for bucket, n in report.get("duration_buckets", {}).items():
        print(f"  - {bucket:10s}: {n}")
    print("By status              :")
    for status, n in report.get("by_status", {}).items():
        print(f"  - {status:10s}: {n}")
    cov = report.get("artifact_coverage", {})
    print("Artifact coverage      :")
    print(f"  - transcripts: {cov.get('transcripts', 0)}")
    print(f"  - visuals    : {cov.get('visuals', 0)}")
    print(f"  - audio      : {cov.get('audio', 0)}")
    print(f"  - video      : {cov.get('video', 0)}")


if __name__ == "__main__":
    sys.exit(main())
