"""One-time staged timing backfill for scheduled 2026 UFC cards."""

from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

from pymongo import MongoClient
import requests

from tapology_scraper.espn_etl import (
    build_section_times_utc,
    event_status,
    infer_competition_sections,
)


BACKFILL_ID = "2026-scheduled-section-timing-v1"
ESPN_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard"
)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _scheduled_events() -> list[dict]:
    response = requests.get(
        ESPN_SCOREBOARD_URL,
        params={"dates": "2026", "limit": 200},
        headers={"User-Agent": "UFC-Picks/1.0 staged timing backfill"},
        timeout=60,
    )
    response.raise_for_status()
    return [
        event
        for event in response.json().get("events") or []
        if "ufc" in str(event.get("name") or "").lower()
        and event_status(event) == "scheduled"
        and event.get("competitions")
    ]


def _run_scraper() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            "espn",
            "-a",
            "MODE=general",
            "-a",
            "SEASON=2026",
            "-a",
            "ONLY_SCHEDULED=true",
        ],
        cwd=Path(__file__).resolve().parent,
        check=True,
    )


def _verify(db) -> dict:
    problems: list[str] = []
    events = _scheduled_events()
    for espn_event in events:
        espn_id = str(espn_event["id"])
        local_event = db.events.find_one({"espn_event_id": espn_id})
        if not local_event:
            problems.append(f"Scheduled ESPN event {espn_id} is not linked")
            continue

        sections = infer_competition_sections(
            espn_event.get("competitions") or []
        )
        expected_times = build_section_times_utc(
            espn_event.get("competitions") or [],
            sections,
        )
        stored_times = local_event.get("section_start_times_utc") or {}
        stored_locks = local_event.get("section_lock_times_utc") or {}
        if not stored_times or not stored_locks:
            problems.append(
                f"Event {local_event['id']} has no staged section schedule"
            )
        elif local_event.get("timing_source") != "admin":
            if stored_times != expected_times:
                problems.append(
                    f"Event {local_event['id']} section starts differ from ESPN"
                )
            if stored_locks != expected_times:
                problems.append(
                    f"Event {local_event['id']} default locks differ from ESPN"
                )

        for competition in espn_event.get("competitions") or []:
            competition_id = str(competition["id"])
            bout = db.bouts.find_one(
                {
                    "event_id": local_event["id"],
                    "espn_competition_id": competition_id,
                }
            )
            if not bout:
                problems.append(
                    f"Event {local_event['id']} is missing bout {competition_id}"
                )
                continue
            if bout.get("card_section") != sections.get(competition_id):
                problems.append(
                    f"Bout {bout['id']} has wrong card section"
                )
            resolved_lock = (
                bout.get("automatic_lock_time_utc")
                or stored_locks.get(bout.get("card_section"))
            )
            if not resolved_lock:
                problems.append(
                    f"Bout {bout['id']} has no automatic lock time"
                )

    if problems:
        raise RuntimeError("\n".join(problems))
    return {
        "scheduled_events": len(events),
        "verified_at": _utcnow(),
    }


def main() -> int:
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        print("BACKFILL_FAILED: MONGODB_URI is required", file=sys.stderr)
        return 1

    client = MongoClient(mongo_uri)
    db = client.ufc_picks
    migrations = db.data_backfills
    marker = migrations.find_one({"_id": BACKFILL_ID})
    if marker and marker.get("status") == "completed":
        print(f"{BACKFILL_ID} already completed; nothing to do.")
        client.close()
        return 0

    migrations.update_one(
        {"_id": BACKFILL_ID},
        {
            "$set": {
                "status": "running",
                "started_at": _utcnow(),
                "last_error": None,
            },
            "$inc": {"attempts": 1},
        },
        upsert=True,
    )
    try:
        _run_scraper()
        summary = _verify(db)
        migrations.update_one(
            {"_id": BACKFILL_ID},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": _utcnow(),
                    "summary": summary,
                }
            },
        )
    except Exception as error:
        migrations.update_one(
            {"_id": BACKFILL_ID},
            {
                "$set": {
                    "status": "failed",
                    "failed_at": _utcnow(),
                    "last_error": str(error),
                }
            },
        )
        print(f"BACKFILL_FAILED: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()

    print(
        "BACKFILL_COMPLETE: "
        f"{summary['scheduled_events']} scheduled cards verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
