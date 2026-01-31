"""
Script de ingestión de datos

Transforma la data scrapeada de Tapology alesquema del backend
"""

import json
import os
import re
from datetime import datetime, date
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Config
MONGO_URI = os.environ.get("MONGODB_URI")
if not MONGO_URI:
    raise ValueError("MONGODB_URI environment variable not set")

DB_NAME = "ufc_picks"  
MIN_DATE = date(2026, 1, 1)  

# MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

events_col = db.events
bouts_col = db.bouts
users_col = db.users  

now = datetime.utcnow()

stats = {
    "events_processed": 0,
    "events_inserted": 0,
    "events_updated": 0,
    "bouts_processed": 0,
    "bouts_inserted": 0,
    "bouts_updated": 0,
    "skipped_non_ufc": 0,
    "skipped_old": 0,
}


def is_ufc_event(event_data: dict) -> bool:
    """Check if event is a UFC event based on name or URL."""
    name = event_data.get("name", "")
    url = event_data.get("tapology_url", "")

    if name:
        name_lower = name.lower()
        if name_lower.startswith("ufc ") or "ufc fight night" in name_lower:
            return True

    if url and "/ufc-" in url.lower():
        return True

    return False


def parse_date(date_str: str) -> date:
    if not date_str:
        return None
    return date.fromisoformat(date_str)


def normalize_card_section(card: str) -> str:
    if not card:
        return "main"

    card_lower = card.lower()

    if "early" in card_lower and "prelim" in card_lower:
        return "early_prelim"
    elif "prelim" in card_lower:
        return "prelim"
    else:
        return "main"


def extract_slug_from_url(url: str) -> str:
    if not url:
        return ""

    # Extract last part of URL path
    match = re.search(r'/events/\d+-(.+)$', url)
    if match:
        return match.group(1)

    match = re.search(r'/bouts/\d+-(.+)$', url)
    if match:
        return match.group(1)

    return ""


def detect_event_type(name: str) -> str:
    """Detectar si es un evento numerado o Fight Night."""
    if not name:
        return "fight_night"
    
    name_lower = name.lower()
    # UFC 300, UFC 324, etc. son eventos numerados
    if re.search(r'ufc\s+\d{1,4}(?:\s|:|$)', name_lower):
        return "numbered"
    # UFC Fight Night, UFC on ESPN, etc.
    return "fight_night"


def transform_event(item: dict) -> dict:
    event_date = parse_date(item.get("event_date"))

    # Parse location
    location = None
    venue = item.get("venue")
    loc_str = item.get("location")

    if venue or loc_str:
        location = {
            "venue": venue,
            "city": loc_str.split(",")[0].strip() if loc_str and "," in loc_str else loc_str,
            "country": loc_str.split(",")[-1].strip() if loc_str and "," in loc_str else None,
        }

    return {
        "_id": int(item["event_id"]),
        "id": int(item["event_id"]),
        "source": "tapology",
        "promotion": "UFC",
        "name": item.get("name"),
        "subtitle": None,
        "slug": extract_slug_from_url(item.get("tapology_url")),
        "url": item.get("tapology_url"),
        "date": datetime.combine(event_date, datetime.min.time()) if event_date else None,
        "start_time_et": item.get("start_time_et"),  # Hora de inicio en ET (ej: "22:00")
        "event_type": detect_event_type(item.get("name")),  # "numbered" o "fight_night"
        "timezone": item.get("timezone", "ET"),
        "location": location,
        "status": "scheduled",  # Will be updated when results come in
        "total_bouts": item.get("total_bouts") or 0,
        "main_event_bout_id": None,  # Set later
        "scraped_at": now,
        "last_updated": now,
    }


def transform_fighter(fighter_data: dict, corner: str) -> dict:
    if not fighter_data:
        return {
            "fighter_name": "TBD",
            "corner": corner,
            "nationality": "Unknown",
            "record_at_fight": {"wins": 0, "losses": 0, "draws": 0},
            "last_fights": [],
            "fighting_out_of": None,
            "ranking": None,
            "age_at_fight_years": 0,
            "height_cm": None,
            "reach_cm": None,
        }

    return {
        "fighter_name": fighter_data.get("name", "TBD"),
        "corner": corner,
        "nationality": "Unknown",  
        "record_at_fight": {"wins": 0, "losses": 0, "draws": 0},  
        "last_fights": [],  
        "fighting_out_of": None,  
        "ranking": None,
        "age_at_fight_years": 0,
        "height_cm": None,
        "reach_cm": None,
        "tapology_id": fighter_data.get("tapology_id"),
        "tapology_url": fighter_data.get("tapology_url"),
    }


def transform_bout(item: dict) -> dict:
    fighters = item.get("fighters", {})
    red_fighter = transform_fighter(fighters.get("red"), "red")
    blue_fighter = transform_fighter(fighters.get("blue"), "blue")
    weight_class = item.get("weight_class") or ""
    gender = "female" if "women" in weight_class.lower() or "strawweight" in weight_class.lower() else "male"

    return {
        "_id": int(item["bout_id"]),
        "id": int(item["bout_id"]),
        "event_id": int(item["event_id"]),
        "source": "tapology",
        "url": item.get("tapology_url"),
        "slug": extract_slug_from_url(item.get("tapology_url")),
        "weight_class": item.get("weight_class") or f"{item.get('weight_lbs', '')} lbs".strip(),
        "gender": gender,
        "rounds_scheduled": item.get("scheduled_rounds") or 3,
        "is_title_fight": item.get("is_title_fight", False),
        "status": "scheduled" if not item.get("cancelled") else "cancelled",
        "fighters": {
            "red": red_fighter,
            "blue": blue_fighter,
        },
        "result": None,  # Set when results are scraped
        "card_section": normalize_card_section(item.get("card")),
        "card_order": item.get("order"),
        "is_main_event": item.get("is_main_event", False),
        "is_co_main_event": item.get("is_co_main_event", False),
        "scraped_at": now,
        "last_updated": now,
    }


def process_event(item: dict) -> bool:
    event_date = parse_date(item.get("event_date"))

    if event_date and event_date < MIN_DATE:
        stats["skipped_old"] += 1
        return False

    if not is_ufc_event(item):
        stats["skipped_non_ufc"] += 1
        return False

    stats["events_processed"] += 1

    event_id = int(item["event_id"])
    existing = events_col.find_one({"_id": event_id})

    event_doc = transform_event(item)

    if not existing:
        events_col.insert_one(event_doc)
        stats["events_inserted"] += 1
        print(f"  Inserted event: {item.get('name')}")
        return True
    elif existing.get("status") == "scheduled":
        events_col.update_one(
            {"_id": event_id},
            {"$set": {k: v for k, v in event_doc.items() if k != "_id"}}
        )
        stats["events_updated"] += 1
        return True

    return False


def process_bout(item: dict, valid_event_ids: set) -> bool:
    event_id = int(item["event_id"])

    if event_id not in valid_event_ids:
        return False

    stats["bouts_processed"] += 1

    bout_id = int(item["bout_id"])
    existing = bouts_col.find_one({"_id": bout_id})

    bout_doc = transform_bout(item)

    if not existing:
        bouts_col.insert_one(bout_doc)
        stats["bouts_inserted"] += 1
        return True
    elif not existing.get("result"):
        # Only update if no result yet
        bouts_col.update_one(
            {"_id": bout_id},
            {"$set": {k: v for k, v in bout_doc.items() if k != "_id"}}
        )
        stats["bouts_updated"] += 1
        return True

    return False


def main():
    print(f"Starting UFC data ingestion...")
    print(f"Database: {DB_NAME}")
    print(f"Minimum date: {MIN_DATE}")
    print()

    valid_event_ids = set()

    # 1. Procesar eventos
    print("Processing events...")
    with open("raw.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get("type") == "event":
                    if process_event(item):
                        valid_event_ids.add(int(item["event_id"]))
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"  Error processing event: {e}")

    print(f"\nValid UFC event IDs: {len(valid_event_ids)}")

    # 2. Procesar peleas
    print("\nProcessing bouts...")
    seen_bout_ids = set()  

    with open("raw.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get("type") == "bout":
                    bout_id = int(item["bout_id"])
                    if bout_id not in seen_bout_ids:
                        seen_bout_ids.add(bout_id)
                        process_bout(item, valid_event_ids)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"  Error processing bout: {e}")

    print("\nUpdating main event references...")
    for event_id in valid_event_ids:
        main_bout = bouts_col.find_one({
            "event_id": event_id,
            "is_main_event": True
        })
        if main_bout:
            events_col.update_one(
                {"_id": event_id},
                {"$set": {"main_event_bout_id": main_bout["_id"]}}
            )

    print("\n" + "="*50)
    print("INGESTION COMPLETE")
    print("="*50)
    print(f"Events processed: {stats['events_processed']}")
    print(f"  - Inserted: {stats['events_inserted']}")
    print(f"  - Updated: {stats['events_updated']}")
    print(f"  - Skipped (non-UFC): {stats['skipped_non_ufc']}")
    print(f"  - Skipped (old): {stats['skipped_old']}")
    print(f"Bouts processed: {stats['bouts_processed']}")
    print(f"  - Inserted: {stats['bouts_inserted']}")
    print(f"  - Updated: {stats['bouts_updated']}")


if __name__ == "__main__":
    main()
