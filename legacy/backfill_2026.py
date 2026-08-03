"""One-time completed-card backfill for the 2026 UFC season.

This migration is deliberately separate from the scheduled scraper windows.
It links the legacy White House card to ESPN, imports only completed 2026 UFC
events and results, resolves historical posters, verifies coverage, and then
records a completion marker so the migration cannot run twice accidentally.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

from pymongo import MongoClient
import requests

from tapology_scraper.spiders.event_images import (
    image_url_is_live,
    poster_is_displayable,
)


BACKFILL_ID = "2026-completed-cards-v1"
SEASON = 2026
ESPN_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard"
)
WHITE_HOUSE_EVENT_ID = 137848
WHITE_HOUSE_ESPN_ID = "600058854"
WHITE_HOUSE_POSTER_URL = (
    "https://upload.wikimedia.org/wikipedia/en/5/57/"
    "Official_Poster_for_UFC_Freedom_250.jpg"
)
WHITE_HOUSE_POSTER_PAGE = (
    "https://en.wikipedia.org/wiki/"
    "File:Official_Poster_for_UFC_Freedom_250.jpg"
)


class BackfillError(RuntimeError):
    """Raised when the historical migration cannot be completed safely."""


def _utcnow() -> datetime:
    return datetime.utcnow()


def _run_scraper(*arguments: str) -> None:
    command = [sys.executable, "-m", "scrapy", "crawl", *arguments]
    print(f"Running: {' '.join(command)}", flush=True)
    subprocess.run(
        command,
        cwd=Path(__file__).resolve().parent,
        check=True,
    )


def _completed_espn_events() -> list[dict]:
    response = requests.get(
        ESPN_SCOREBOARD_URL,
        params={"dates": str(SEASON), "limit": 200},
        headers={"User-Agent": "UFC-Picks/1.0 2026 historical backfill"},
        timeout=60,
    )
    response.raise_for_status()
    return [
        event
        for event in response.json().get("events") or []
        if "ufc" in str(event.get("name") or "").lower()
        and ((event.get("status") or {}).get("type") or {}).get("completed")
    ]


def _prepare_white_house_link(db) -> dict:
    event = db.events.find_one({"id": WHITE_HOUSE_EVENT_ID})
    if not event:
        raise BackfillError(
            f"Legacy UFC White House event {WHITE_HOUSE_EVENT_ID} was not found"
        )

    current_espn_id = str(event.get("espn_event_id") or "")
    if current_espn_id and current_espn_id != WHITE_HOUSE_ESPN_ID:
        raise BackfillError(
            "UFC White House already has a different ESPN mapping: "
            f"{current_espn_id}"
        )

    db.events.update_one(
        {"id": WHITE_HOUSE_EVENT_ID},
        {
            "$set": {
                "espn_event_id": WHITE_HOUSE_ESPN_ID,
                "historical_backfill_id": BACKFILL_ID,
            }
        },
    )
    return {
        "name": event.get("name"),
        "date": event.get("date") or event.get("event_date"),
    }


def _restore_white_house_identity(db, identity: dict) -> None:
    fields = {
        "poster_image_url": WHITE_HOUSE_POSTER_URL,
        "poster_image_source": "wikipedia_file",
        "poster_source_page_url": WHITE_HOUSE_POSTER_PAGE,
        "wikipedia_file_url": WHITE_HOUSE_POSTER_PAGE,
        "wikipedia_image_url": WHITE_HOUSE_POSTER_URL,
        "poster_image_updated_at": _utcnow(),
        "historical_backfill_id": BACKFILL_ID,
    }
    if identity.get("name"):
        fields["name"] = identity["name"]
    if identity.get("date"):
        fields["date"] = identity["date"]
    db.events.update_one({"id": WHITE_HOUSE_EVENT_ID}, {"$set": fields})


def _verify(db) -> dict:
    espn_events = _completed_espn_events()
    problems: list[str] = []
    poster_urls: dict[int, str] = {}

    for espn_event in espn_events:
        espn_id = str(espn_event["id"])
        event = db.events.find_one({"espn_event_id": espn_id})
        if not event:
            problems.append(
                f"ESPN event {espn_id} ({espn_event.get('name')}) is not linked"
            )
            continue

        event_id = int(event["id"])
        competitions = espn_event.get("competitions") or []
        competition_ids = [str(item["id"]) for item in competitions]
        linked_bouts = list(
            db.bouts.find(
                {
                    "event_id": event_id,
                    "espn_competition_id": {"$in": competition_ids},
                },
                {"espn_competition_id": 1, "result": 1},
            )
        )
        resulted_ids = {
            str(bout.get("espn_competition_id"))
            for bout in linked_bouts
            if isinstance(bout.get("result"), dict) and bout["result"]
        }
        missing_results = [
            competition_id
            for competition_id in competition_ids
            if competition_id not in resulted_ids
        ]
        if missing_results:
            problems.append(
                f"Event {event_id} is missing {len(missing_results)} ESPN results"
            )

        if int(event.get("total_bouts") or 0) != len(competitions):
            problems.append(
                f"Event {event_id} has total_bouts={event.get('total_bouts')} "
                f"but ESPN has {len(competitions)}"
            )
        if event.get("status") != "completed":
            problems.append(f"Event {event_id} is not completed")
        if not poster_is_displayable(event):
            problems.append(f"Event {event_id} has no displayable poster")
        else:
            poster_urls[event_id] = event["poster_image_url"]

    with ThreadPoolExecutor(max_workers=6) as executor:
        poster_checks = dict(
            zip(
                poster_urls,
                executor.map(image_url_is_live, poster_urls.values()),
            )
        )
    for event_id, is_live in poster_checks.items():
        if not is_live:
            problems.append(f"Event {event_id} poster URL is not live")

    if problems:
        raise BackfillError("\n".join(problems))

    return {
        "completed_espn_events": len(espn_events),
        "events_with_live_posters": len(poster_urls),
        "verified_at": _utcnow(),
    }


def run_backfill(force: bool = False) -> dict:
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        raise BackfillError("MONGODB_URI is required")

    client = MongoClient(mongo_uri)
    db = client.ufc_picks
    migrations = db.data_backfills
    marker = migrations.find_one({"_id": BACKFILL_ID})
    if marker and marker.get("status") == "completed" and not force:
        print(
            f"Backfill {BACKFILL_ID} already completed at "
            f"{marker.get('completed_at')}; nothing to do."
        )
        client.close()
        return marker.get("summary") or {}

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
        white_house_identity = _prepare_white_house_link(db)
        _run_scraper(
            "espn",
            "-a",
            "MODE=general",
            "-a",
            f"SEASON={SEASON}",
            "-a",
            "ONLY_COMPLETED=true",
        )
        _run_scraper(
            "event_images",
            "-a",
            f"SEASON={SEASON}",
            "-a",
            "FORCE=true",
            "-a",
            "ONLY_COMPLETED=true",
        )
        _restore_white_house_identity(db, white_house_identity)
        summary = _verify(db)
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
        raise
    else:
        migrations.update_one(
            {"_id": BACKFILL_ID},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": _utcnow(),
                    "summary": summary,
                    "last_error": None,
                }
            },
        )
        return summary
    finally:
        client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="rerun even if the completion marker already exists",
    )
    args = parser.parse_args()
    try:
        summary = run_backfill(force=args.force)
    except Exception as error:
        print(f"BACKFILL_FAILED: {error}", file=sys.stderr)
        return 1

    print(
        "BACKFILL_COMPLETE: "
        f"{summary.get('completed_espn_events', 0)} completed events, "
        f"{summary.get('events_with_live_posters', 0)} live posters"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
