"""Fail a scraping run when it did not produce a fresh usable snapshot."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-file", required=True)
    parser.add_argument("--min-events", type=int, default=0)
    parser.add_argument("--min-bouts", type=int, default=0)
    parser.add_argument("--min-items", type=int, default=0)
    parser.add_argument("--allowed-types", nargs="*")
    args = parser.parse_args()

    path = Path(args.raw_file)
    if not path.is_file():
        raise SystemExit(f"Scraper output does not exist: {path}")

    counts: Counter[str] = Counter()
    total = 0
    with path.open(encoding="utf-8") as feed:
        for line_number, line in enumerate(feed, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"Invalid JSON at line {line_number}: {exc}") from exc
            item_type = item.get("type")
            if item_type:
                counts[item_type] += 1
            total += 1

    if args.allowed_types:
        total = sum(counts[item_type] for item_type in args.allowed_types)

    print(f"Fresh scrape summary: total={total}, types={dict(counts)}")
    if counts["event"] < args.min_events or counts["bout"] < args.min_bouts or total < args.min_items:
        raise SystemExit(
            "Scraper did not produce the required fresh data. "
            "Stopping before ingestion to avoid stale-card corruption."
        )


if __name__ == "__main__":
    main()
