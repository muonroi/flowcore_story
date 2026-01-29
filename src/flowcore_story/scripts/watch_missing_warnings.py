#!/usr/bin/env python3

"""Tail logs/crawler.log, print missing warnings, and optionally forward them to Kafka."""
from __future__ import annotations

import argparse
import asyncio
import io
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
from flowcore_story.config import config as app_config  # noqa: E402

DEFAULT_LOG_PATH = Path("logs") / "crawler.log"
DEFAULT_PATTERN = re.compile(r"\[WARNING\].*Sau crawl", re.IGNORECASE)
WARNING_PARSE_RE = re.compile(
    r"\[WARNING\].*?truy\S* '(?P<title>[^']+)' v?n thi?u chuong: (?P<crawled>\d+)\+(?P<dead>\d+)/(?:\s?)(?P<total>\d+)",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Watch crawler.log and highlight missing-chapter warnings.")
    parser.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Path to the log file (default: logs/crawler.log)",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=DEFAULT_PATTERN.pattern,
        help="Regex to match lines of interest (default: '[WARNING].*Sau crawl')",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.5,
        help="Seconds to wait between file polling when idle (default: 0.5)",
    )
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="Scan entire existing file before tailing for new lines.",
    )
    parser.add_argument(
        "--emit-kafka",
        action="store_true",
        help="Forward matched warnings to Kafka as jobs.",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        default=app_config.MISSING_WARNING_TOPIC,
        help="Kafka topic for emitted jobs (default: missing_warnings or env MISSING_WARNING_TOPIC).",
    )
    parser.add_argument(
        "--kafka-job-type",
        type=str,
        default="missing_warning",
        help="Job type to include in Kafka payload (default: missing_warning).",
    )
    return parser.parse_args()


@dataclass
class WarningPayload:
    raw_line: str
    story_title: str | None = None
    crawled_count: int | None = None
    dead_count: int | None = None
    total_count: int | None = None

    @classmethod
    def from_log_line(cls, line: str) -> WarningPayload:
        match = WARNING_PARSE_RE.search(line)
        if not match:
            return cls(raw_line=line.strip())
        try:
            crawled = int(match.group("crawled"))
            dead = int(match.group("dead"))
            total = int(match.group("total"))
        except (ValueError, TypeError):
            crawled = dead = total = None  # type: ignore
        return cls(
            raw_line=line.strip(),
            story_title=match.group("title").strip(),
            crawled_count=crawled,
            dead_count=dead,
            total_count=total,
        )


async def emit_kafka_job(payload: WarningPayload, topic: str, job_type: str) -> None:
    from flowcore_story.kafka.kafka_producer import send_job

    job = {
        "type": job_type,
        "raw_line": payload.raw_line,
        "story_title": payload.story_title,
        "crawled_count": payload.crawled_count,
        "dead_count": payload.dead_count,
        "total_count": payload.total_count,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    await send_job(job, topic=topic)


def open_log(path: Path) -> io.TextIOWrapper:
    try:
        return path.open("r", encoding="utf-8")
    except FileNotFoundError:
        print(f"[watch-missing] Log file not found: {path}", file=sys.stderr)
        sys.exit(1)


def seek_start(log: io.TextIOWrapper, from_start: bool) -> None:
    if from_start:
        log.seek(0)
    else:
        log.seek(0, os.SEEK_END)


def main() -> None:
    args = parse_args()
    pattern = re.compile(args.pattern, re.IGNORECASE)
    log_path = args.log
    log = open_log(log_path)
    seek_start(log, args.from_start)

    print(
        f"[watch-missing] Watching '{log_path}' for pattern '{pattern.pattern}'",
        file=sys.stderr,
    )

    try:
        while True:
            line = log.readline()
            if not line:
                time.sleep(args.poll_interval)
                continue
            if pattern.search(line):
                sys.stdout.write(line)
                sys.stdout.flush()
                if args.emit_kafka:
                    payload = WarningPayload.from_log_line(line)
                    try:
                        asyncio.run(emit_kafka_job(payload, args.kafka_topic, args.kafka_job_type))
                    except Exception as exc:  # pragma: no cover - runtime safeguard
                        print(f"[watch-missing] Failed to emit Kafka job: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n[watch-missing] Stopped.", file=sys.stderr)
    finally:
        log.close()


if __name__ == "__main__":
    main()
