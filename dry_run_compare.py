"""
Run the UFC spider locally without Mongo writes and compare the generated
Mongo-shaped snapshot against live Mongo documents for one event.

Usage:
    python dry_run_compare.py --event-id 136871
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import dotenv_values
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = BASE_DIR / "artifacts" / "dry-run"


def ensure_mongo_uri() -> str:
    mongo_uri = os.getenv("MONGODB_URI")
    if mongo_uri:
        return mongo_uri

    for env_path in (BASE_DIR / ".env", BASE_DIR.parent / "backend" / ".env"):
        if env_path.exists():
            env_values = dotenv_values(env_path)
            mongo_uri = env_values.get("MONGODB_URI")
            if mongo_uri:
                os.environ["MONGODB_URI"] = mongo_uri
                return mongo_uri

    raise RuntimeError("MONGODB_URI is required to compare the dry-run snapshot against MongoDB")


MONGO_URI = ensure_mongo_uri()

import ingest  # noqa: E402


def compact_dict(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    return value


def prune_none(value: Any) -> Any:
    if isinstance(value, dict):
        pruned = {
            key: prune_none(item)
            for key, item in value.items()
            if item is not None
        }
        return pruned
    if isinstance(value, list):
        return [prune_none(item) for item in value]
    return value


def prepare_fighter_detail_data(fighter_data: dict[str, Any]) -> dict[str, Any]:
    if not fighter_data:
        return {
            "fighter_name": None,
            "tapology_id": None,
        }

    name = fighter_data.get("name") or fighter_data.get("fighter_name")

    nationality = fighter_data.get("nationality", "Unknown")
    if nationality and isinstance(nationality, str):
        nationality = nationality.strip()

    fighting_out_of = fighter_data.get("fighting_out_of")
    if fighting_out_of and isinstance(fighting_out_of, str):
        fighting_out_of = fighting_out_of.strip()

    prepared = {
        "fighter_name": name,
        "tapology_id": fighter_data.get("tapology_id"),
        "tapology_url": fighter_data.get("tapology_url"),
        "nickname": fighter_data.get("nickname"),
        "nationality": nationality,
        "fighting_out_of": fighting_out_of,
        "age_at_fight_years": fighter_data.get("age_at_fight", {}).get("years", 0)
        if isinstance(fighter_data.get("age_at_fight"), dict)
        else 0,
        "age_at_fight": fighter_data.get("age_at_fight"),
        "height_cm": fighter_data.get("height", {}).get("cm")
        if isinstance(fighter_data.get("height"), dict)
        else None,
        "height": fighter_data.get("height"),
        "reach_cm": fighter_data.get("reach", {}).get("cm")
        if isinstance(fighter_data.get("reach"), dict)
        else None,
        "reach": fighter_data.get("reach"),
        "latest_weight": fighter_data.get("latest_weight"),
        "record_at_fight": fighter_data.get("record_at_fight"),
        "ufc_ranking": fighter_data.get("ufc_ranking"),
        "last_5_fights": fighter_data.get("last_5_fights"),
        "betting_odds": fighter_data.get("betting_odds"),
        "title_status": fighter_data.get("title_status"),
        "gym": fighter_data.get("gym"),
    }

    return compact_dict(prepared)


def build_bout_detail_doc(item: dict[str, Any], scraped_at: datetime) -> dict[str, Any]:
    fighters = item.get("fighters", {})

    bout_detail_doc = {
        "_id": int(item["bout_id"]),
        "bout_id": int(item["bout_id"]),
        "event_id": int(item["event_id"]),
        "bout_date": item.get("bout_date"),
        "broadcast": item.get("broadcast"),
        "weight_info": item.get("weight_info"),
        "scraped_at": scraped_at,
    }

    if fighters:
        bout_detail_doc["fighters"] = {
            "red": prepare_fighter_detail_data(fighters.get("red", {})),
            "blue": prepare_fighter_detail_data(fighters.get("blue", {})),
        }

    if item.get("result"):
        bout_detail_doc["result"] = item["result"]

    return compact_dict(bout_detail_doc)


def load_jsonl(raw_path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with raw_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def build_snapshot(items: list[dict[str, Any]]) -> dict[str, Any]:
    run_now = datetime.utcnow()
    ingest.now = run_now
    ingest.bout_details_cache.clear()

    for item in items:
        if item.get("type") == "bout_detail" and item.get("bout_id"):
            ingest.bout_details_cache[int(item["bout_id"])] = item

    valid_event_ids: set[int] = set()
    event_docs: list[dict[str, Any]] = []
    bout_docs: list[dict[str, Any]] = []
    bout_detail_docs: list[dict[str, Any]] = []
    seen_bout_ids: set[int] = set()

    for item in items:
        if item.get("type") != "event":
            continue
        event_date = ingest.parse_date(item.get("event_date"))
        if event_date and event_date < ingest.MIN_DATE:
            continue
        if not ingest.is_ufc_event(item):
            continue

        valid_event_ids.add(int(item["event_id"]))
        event_docs.append(deepcopy(ingest.transform_event(item)))

    for item in items:
        if item.get("type") != "bout":
            continue
        bout_id = int(item["bout_id"])
        event_id = int(item["event_id"])
        if event_id not in valid_event_ids:
            continue
        if item.get("cancelled") or item.get("status") == "cancelled":
            continue
        if bout_id in seen_bout_ids:
            continue

        seen_bout_ids.add(bout_id)
        bout_docs.append(deepcopy(ingest.transform_bout(item)))

    for bout_id, detail in sorted(ingest.bout_details_cache.items()):
        if int(detail.get("event_id", 0)) not in valid_event_ids:
            continue
        bout_detail_docs.append(build_bout_detail_doc(detail, run_now))

    event_docs.sort(key=lambda doc: doc["id"])
    bout_docs.sort(key=lambda doc: doc["id"])
    bout_detail_docs.sort(key=lambda doc: doc["bout_id"])

    return {
        "generated_at": run_now,
        "counts": {
            "raw_items": len(items),
            "events": len(event_docs),
            "bouts": len(bout_docs),
            "bout_details": len(bout_detail_docs),
        },
        "events": event_docs,
        "bouts": bout_docs,
        "bout_details": bout_detail_docs,
    }


def normalize_event(doc: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "id": doc.get("id"),
        "source": doc.get("source"),
        "promotion": doc.get("promotion"),
        "name": doc.get("name"),
        "subtitle": doc.get("subtitle"),
        "slug": doc.get("slug"),
        "url": doc.get("url") or doc.get("tapology_url"),
        "date": doc.get("date"),
        "start_time_et": doc.get("start_time_et"),
        "event_type": doc.get("event_type"),
        "timezone": doc.get("timezone"),
        "location": doc.get("location"),
        "total_bouts": doc.get("total_bouts"),
    }
    return prune_none(jsonable(normalized))


def normalize_bout_fighter(doc: dict[str, Any], corner: str) -> dict[str, Any]:
    normalized = {
        "fighter_name": doc.get("fighter_name") or doc.get("name"),
        "corner": doc.get("corner") or corner,
        "nationality": doc.get("nationality"),
        "record_at_fight": doc.get("record_at_fight"),
        "last_fights": doc.get("last_fights") or doc.get("last_5_fights"),
        "fighting_out_of": doc.get("fighting_out_of"),
        "ranking": doc.get("ranking") or doc.get("ufc_ranking"),
        "age_at_fight_years": doc.get("age_at_fight_years")
        or (doc.get("age_at_fight") or {}).get("years"),
        "height_cm": doc.get("height_cm") or (doc.get("height") or {}).get("cm"),
        "reach_cm": doc.get("reach_cm") or (doc.get("reach") or {}).get("cm"),
        "tapology_id": doc.get("tapology_id"),
        "tapology_url": doc.get("tapology_url"),
        "gym": doc.get("gym"),
        "betting_odds": doc.get("betting_odds"),
        "title_status": doc.get("title_status"),
    }
    return prune_none(jsonable(normalized))


def normalize_bout(doc: dict[str, Any]) -> dict[str, Any]:
    fighters = doc.get("fighters", {})
    normalized = {
        "id": doc.get("id"),
        "event_id": doc.get("event_id"),
        "source": doc.get("source"),
        "url": doc.get("url") or doc.get("tapology_url"),
        "slug": doc.get("slug"),
        "weight_class": doc.get("weight_class"),
        "gender": doc.get("gender"),
        "rounds_scheduled": doc.get("rounds_scheduled") or doc.get("scheduled_rounds"),
        "is_title_fight": doc.get("is_title_fight", False),
        "status": doc.get("status"),
        "fighters": {
            "red": normalize_bout_fighter(fighters.get("red", {}), "red"),
            "blue": normalize_bout_fighter(fighters.get("blue", {}), "blue"),
        },
        "card_section": doc.get("card_section") or ingest.normalize_card_section(doc.get("card")),
        "card_order": doc.get("card_order") or doc.get("order"),
        "is_main_event": doc.get("is_main_event", False),
        "is_co_main_event": doc.get("is_co_main_event", False),
    }
    return prune_none(jsonable(normalized))


def normalize_bout_detail_fighter(doc: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "fighter_name": doc.get("fighter_name") or doc.get("name"),
        "tapology_id": doc.get("tapology_id"),
        "tapology_url": doc.get("tapology_url"),
        "nickname": doc.get("nickname"),
        "nationality": doc.get("nationality"),
        "fighting_out_of": doc.get("fighting_out_of"),
        "age_at_fight_years": doc.get("age_at_fight_years")
        or (doc.get("age_at_fight") or {}).get("years"),
        "age_at_fight": doc.get("age_at_fight"),
        "height_cm": doc.get("height_cm") or (doc.get("height") or {}).get("cm"),
        "height": doc.get("height"),
        "reach_cm": doc.get("reach_cm") or (doc.get("reach") or {}).get("cm"),
        "reach": doc.get("reach"),
        "latest_weight": doc.get("latest_weight"),
        "record_at_fight": doc.get("record_at_fight"),
        "ufc_ranking": doc.get("ufc_ranking"),
        "last_5_fights": doc.get("last_5_fights") or doc.get("last_fights"),
        "betting_odds": doc.get("betting_odds"),
        "title_status": doc.get("title_status"),
        "gym": doc.get("gym"),
    }
    return prune_none(jsonable(normalized))


def normalize_bout_detail(doc: dict[str, Any]) -> dict[str, Any]:
    fighters = doc.get("fighters", {})
    normalized = {
        "bout_id": doc.get("bout_id") or doc.get("id"),
        "event_id": doc.get("event_id"),
        "bout_date": doc.get("bout_date"),
        "broadcast": doc.get("broadcast"),
        "weight_info": doc.get("weight_info"),
        "fighters": {
            "red": normalize_bout_detail_fighter(fighters.get("red", {})),
            "blue": normalize_bout_detail_fighter(fighters.get("blue", {})),
        },
        "result": doc.get("result"),
    }
    return prune_none(jsonable(normalized))


def collect_diff_paths(left: Any, right: Any, path: str = "") -> list[str]:
    if type(left) is not type(right):
        return [path or "$"]

    if isinstance(left, dict):
        paths: list[str] = []
        for key in sorted(set(left) | set(right)):
            next_path = f"{path}.{key}" if path else key
            if key not in left or key not in right:
                paths.append(next_path)
                continue
            paths.extend(collect_diff_paths(left[key], right[key], next_path))
        return paths

    if isinstance(left, list):
        paths: list[str] = []
        if len(left) != len(right):
            paths.append(path or "$")
            return paths
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            next_path = f"{path}[{index}]" if path else f"[{index}]"
            paths.extend(collect_diff_paths(left_item, right_item, next_path))
        return paths

    if left != right:
        return [path or "$"]

    return []


def compare_docs(
    generated_docs: list[dict[str, Any]],
    mongo_docs: list[dict[str, Any]],
    id_key: str,
    normalizer,
) -> dict[str, Any]:
    generated_map = {doc[id_key]: normalizer(doc) for doc in generated_docs}
    mongo_map = {doc[id_key]: normalizer(doc) for doc in mongo_docs}

    missing_in_mongo = sorted(set(generated_map) - set(mongo_map))
    missing_in_generated = sorted(set(mongo_map) - set(generated_map))
    matched = 0
    mismatches: list[dict[str, Any]] = []

    for doc_id in sorted(set(generated_map) & set(mongo_map)):
        generated = generated_map[doc_id]
        mongo = mongo_map[doc_id]
        if generated == mongo:
            matched += 1
            continue

        diff_paths = collect_diff_paths(generated, mongo)
        mismatches.append({
            "id": doc_id,
            "diff_paths": diff_paths[:25],
            "generated": generated,
            "mongo": mongo,
        })

    return {
        "generated_count": len(generated_docs),
        "mongo_count": len(mongo_docs),
        "matched": matched,
        "missing_in_mongo": missing_in_mongo,
        "missing_in_generated": missing_in_generated,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


def fetch_event_url(event_id: int) -> str | None:
    client = MongoClient(MONGO_URI)
    db = client["ufc_picks"]
    try:
        event = db.events.find_one({"id": event_id}, {"_id": 0, "url": 1, "tapology_url": 1})
    finally:
        client.close()

    if not event:
        return None

    return event.get("url") or event.get("tapology_url")


def run_spider(event_id: int, event_url: str | None, raw_path: Path) -> None:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_path.exists():
        raw_path.unlink()

    command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "ufc",
        "-a",
        f"EVENT_ID={event_id}",
        "-a",
        "MODE=results",
    ]
    if event_url:
        command.extend(["-a", f"EVENT_URL={event_url}"])
    command.extend([
        "-o",
        str(raw_path),
    ])

    env = os.environ.copy()
    env.pop("MONGODB_URI", None)

    subprocess.run(command, cwd=BASE_DIR, env=env, check=True)


def fetch_mongo_docs(event_id: int) -> dict[str, Any]:
    client = MongoClient(MONGO_URI)
    db = client["ufc_picks"]
    try:
        event_doc = db.events.find_one({"id": event_id}) or {}
        bouts = list(db.bouts.find({"event_id": event_id}))
        bout_details = list(db.bout_details.find({"event_id": event_id}))
    finally:
        client.close()

    return {
        "events": [event_doc] if event_doc else [],
        "bouts": bouts,
        "bout_details": bout_details,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run the UFC spider and compare its output against MongoDB")
    parser.add_argument("--event-id", type=int, required=True, help="Tapology event id to scrape")
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional filename prefix for artifacts; defaults to ufc-event-<event-id>",
    )
    args = parser.parse_args()

    prefix = args.prefix or f"ufc-event-{args.event_id}"
    raw_path = ARTIFACTS_DIR / f"{prefix}-raw.jsonl"
    snapshot_path = ARTIFACTS_DIR / f"{prefix}-mongo-shape.json"
    compare_path = ARTIFACTS_DIR / f"{prefix}-compare.json"
    event_url = fetch_event_url(args.event_id)

    run_spider(args.event_id, event_url, raw_path)
    items = load_jsonl(raw_path)
    snapshot = build_snapshot(items)
    mongo_docs = fetch_mongo_docs(args.event_id)

    comparison = {
        "event_id": args.event_id,
        "generated_at": snapshot["generated_at"],
        "artifacts": {
            "raw_jsonl": raw_path,
            "mongo_shape_json": snapshot_path,
        },
        "raw_counts": snapshot["counts"],
        "events": compare_docs(snapshot["events"], mongo_docs["events"], "id", normalize_event),
        "bouts": compare_docs(snapshot["bouts"], mongo_docs["bouts"], "id", normalize_bout),
        "bout_details": compare_docs(
            snapshot["bout_details"],
            mongo_docs["bout_details"],
            "bout_id",
            normalize_bout_detail,
        ),
    }

    snapshot_path.write_text(json.dumps(jsonable(snapshot), indent=2, ensure_ascii=False), encoding="utf-8")
    compare_path.write_text(json.dumps(jsonable(comparison), indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(jsonable({
        "raw_jsonl": raw_path,
        "mongo_shape_json": snapshot_path,
        "compare_json": compare_path,
        "summary": {
            "events": {
                "matched": comparison["events"]["matched"],
                "mismatches": comparison["events"]["mismatch_count"],
            },
            "bouts": {
                "matched": comparison["bouts"]["matched"],
                "mismatches": comparison["bouts"]["mismatch_count"],
            },
            "bout_details": {
                "matched": comparison["bout_details"]["matched"],
                "mismatches": comparison["bout_details"]["mismatch_count"],
            },
        },
    }), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
