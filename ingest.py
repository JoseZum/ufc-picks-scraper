"""
Script de ingestión de datos.

Transforma la data scrapeada de Tapology al esquema del backend.
Valida, normaliza e inserta en MongoDB.
"""

import json
import os
import re
from datetime import datetime, date
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Configuración
MONGO_URI = os.environ.get("MONGODB_URI")
if not MONGO_URI:
    raise ValueError("MONGODB_URI no configurada")

DB_NAME = "ufc_picks"
MIN_DATE = date(2026, 1, 1)

# Conexión a MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

events_col = db.events
bouts_col = db.bouts
users_col = db.users

now = datetime.utcnow()

# Estadísticas de ejecución
stats = {
    "events_processed": 0,
    "events_inserted": 0,
    "events_updated": 0,
    "bouts_processed": 0,
    "bouts_inserted": 0,
    "bouts_updated": 0,
    "bouts_deleted": 0,
    "bout_details_processed": 0,
    "skipped_non_ufc": 0,
    "skipped_old": 0,
}

# Cache para datos enriquecidos de bouts
bout_details_cache = {}


def is_ufc_event(event_data: dict) -> bool:
    """Verifica si el evento es del UFC (por nombre o URL)."""
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
    """Parsea una fecha en formato ISO a objeto date."""
    if not date_str:
        return None
    return date.fromisoformat(date_str)


def normalize_card_section(card: str) -> str:
    """Normaliza la sección de la cartelera a valores estándar."""
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
    """Extrae el slug (último segmento) de una URL de Tapology."""
    if not url:
        return ""

    # Extrae la última parte del path
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


def parse_record(record_str: str) -> dict:
    """Parse record string like '23-5-0' or '23-5' into dict."""
    if not record_str:
        return {"wins": 0, "losses": 0, "draws": 0}

    parts = record_str.replace(" ", "").split("-")
    if len(parts) >= 2:
        return {
            "wins": int(parts[0]) if parts[0].isdigit() else 0,
            "losses": int(parts[1]) if parts[1].isdigit() else 0,
            "draws": int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0,
        }
    return {"wins": 0, "losses": 0, "draws": 0}


def parse_height_to_cm(height_str: str) -> int | None:
    """Convert height like 5'10\" or 5'10 to centimeters."""
    if not height_str:
        return None

    match = re.search(r"(\d+)'(\d+)", height_str)
    if match:
        feet = int(match.group(1))
        inches = int(match.group(2))
        return int((feet * 30.48) + (inches * 2.54))
    return None


def parse_reach_to_cm(reach_str: str) -> int | None:
    """Convert reach like 72\" or 72 to centimeters."""
    if not reach_str:
        return None

    match = re.search(r"(\d+)", reach_str)
    if match:
        inches = int(match.group(1))
        return int(inches * 2.54)
    return None


def transform_fighter(fighter_data: dict, corner: str, bout_detail: dict = None) -> dict:
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

    # Base data from bout
    fighter = {
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

    # Enrich with bout_detail data if available
    if bout_detail:
        # bout_detail has structure: fighters.red and fighters.blue
        fighters_data = bout_detail.get("fighters", {})
        fighter_detail = fighters_data.get(corner, {})

        if fighter_detail:
            # Nationality (string)
            if fighter_detail.get("nationality"):
                fighter["nationality"] = fighter_detail["nationality"]

            # Record at fight (object with wins, losses, draws)
            if fighter_detail.get("record_at_fight"):
                record = fighter_detail["record_at_fight"]
                if isinstance(record, dict):
                    fighter["record_at_fight"] = {
                        "wins": record.get("wins", 0),
                        "losses": record.get("losses", 0),
                        "draws": record.get("draws", 0)
                    }

            # Age at fight (object with years, months, days)
            if fighter_detail.get("age_at_fight"):
                age_data = fighter_detail["age_at_fight"]
                if isinstance(age_data, dict) and "years" in age_data:
                    fighter["age_at_fight_years"] = age_data["years"]

            # Height (object with cm)
            if fighter_detail.get("height"):
                height_data = fighter_detail["height"]
                if isinstance(height_data, dict) and "cm" in height_data:
                    fighter["height_cm"] = height_data["cm"]

            # Reach (object with cm)
            if fighter_detail.get("reach"):
                reach_data = fighter_detail["reach"]
                if isinstance(reach_data, dict) and "cm" in reach_data:
                    fighter["reach_cm"] = reach_data["cm"]

            # UFC Ranking (object with position and division)
            if fighter_detail.get("ufc_ranking"):
                ranking_data = fighter_detail["ufc_ranking"]
                if isinstance(ranking_data, dict):
                    # Keep ranking as dict for Pydantic validation
                    fighter["ranking"] = ranking_data

            # Title status (Champion/Challenger)
            if fighter_detail.get("title_status"):
                fighter["title_status"] = fighter_detail["title_status"]

            # Fighting out of
            if fighter_detail.get("fighting_out_of"):
                fighter["fighting_out_of"] = fighter_detail["fighting_out_of"]

            # Last 5 fights (array of W/L)
            if fighter_detail.get("last_5_fights"):
                fighter["last_fights"] = fighter_detail["last_5_fights"]

            # Gym info
            if fighter_detail.get("gym"):
                gym_data = fighter_detail["gym"]
                if isinstance(gym_data, dict):
                    # Keep gym as dict for frontend
                    fighter["gym"] = gym_data

            # Betting odds
            if fighter_detail.get("betting_odds"):
                fighter["betting_odds"] = fighter_detail["betting_odds"]

    return fighter


def transform_bout(item: dict) -> dict:
    bout_id = int(item["bout_id"])
    fighters = item.get("fighters", {})

    # Get bout_detail data if available
    bout_detail = bout_details_cache.get(bout_id)

    red_fighter = transform_fighter(fighters.get("red"), "red", bout_detail)
    blue_fighter = transform_fighter(fighters.get("blue"), "blue", bout_detail)
    weight_class = item.get("weight_class") or ""
    gender = "female" if "women" in weight_class.lower() or "strawweight" in weight_class.lower() else "male"

    return {
        "_id": bout_id,
        "id": bout_id,
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


def process_bout_detail(item: dict):
    """Cache bout_detail data for later use when processing bouts."""
    bout_id = item.get("bout_id")
    if bout_id:
        bout_details_cache[int(bout_id)] = item
        stats["bout_details_processed"] += 1


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
    else:
        # Always update name and poster_image_url, even if event has finished
        # This ensures event information stays in sync with Tapology
        update_fields = {
            "name": event_doc["name"],
            "last_updated": now,
        }

        # Update poster if it exists in scraped data
        if event_doc.get("poster_image_url"):
            update_fields["poster_image_url"] = event_doc["poster_image_url"]

        # If event is still scheduled, update all fields
        if existing.get("status") == "scheduled":
            update_fields = {k: v for k, v in event_doc.items() if k != "_id"}

        events_col.update_one(
            {"_id": event_id},
            {"$set": update_fields}
        )
        stats["events_updated"] += 1
        return True


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


def cleanup_deleted_bouts(event_id: int, scraped_bout_ids: set):
    """
    Remove bouts from database that are no longer in Tapology for this event.

    Args:
        event_id: The event ID to check
        scraped_bout_ids: Set of bout IDs that were scraped from Tapology
    """
    # Find all bouts in DB for this event
    db_bouts = list(bouts_col.find({"event_id": event_id}))
    db_bout_ids = {bout["_id"] for bout in db_bouts}

    # Find bouts that are in DB but not in Tapology anymore
    deleted_bout_ids = db_bout_ids - scraped_bout_ids

    if deleted_bout_ids:
        print(f"  Deleting {len(deleted_bout_ids)} bouts no longer in Tapology for event {event_id}:")
        for bout_id in deleted_bout_ids:
            bout = next((b for b in db_bouts if b["_id"] == bout_id), None)
            if bout:
                fighter_names = f"{bout['fighters']['red']['fighter_name']} vs {bout['fighters']['blue']['fighter_name']}"
                print(f"    - Bout {bout_id}: {fighter_names}")
                bouts_col.delete_one({"_id": bout_id})
                stats["bouts_deleted"] += 1


def main():
    print(f"Starting UFC data ingestion...")
    print(f"Database: {DB_NAME}")
    print(f"Minimum date: {MIN_DATE}")
    print()

    valid_event_ids = set()
    # Track scraped bouts per event for cleanup
    scraped_bouts_by_event = {}

    # 1. First pass: Cache bout_detail items for fighter enrichment
    print("Caching bout details for fighter data...")
    with open("raw.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get("type") == "bout_detail":
                    process_bout_detail(item)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"  Error caching bout_detail: {e}")

    print(f"Cached {stats['bout_details_processed']} bout details")

    # 2. Procesar eventos
    print("\nProcessing events...")
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

    # 3. Procesar peleas (now enriched with bout_detail data)
    print("\nProcessing bouts...")
    seen_bout_ids = set()

    with open("raw.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get("type") == "bout":
                    bout_id = int(item["bout_id"])
                    event_id = int(item["event_id"])

                    if bout_id not in seen_bout_ids:
                        seen_bout_ids.add(bout_id)

                        # Track scraped bouts per event
                        if event_id not in scraped_bouts_by_event:
                            scraped_bouts_by_event[event_id] = set()
                        scraped_bouts_by_event[event_id].add(bout_id)

                        process_bout(item, valid_event_ids)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"  Error processing bout: {e}")

    # 4. Clean up deleted bouts for each event
    print("\nCleaning up deleted bouts...")
    for event_id in valid_event_ids:
        scraped_bout_ids = scraped_bouts_by_event.get(event_id, set())
        cleanup_deleted_bouts(event_id, scraped_bout_ids)

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
    print(f"Bout details cached: {stats['bout_details_processed']}")
    print(f"Events processed: {stats['events_processed']}")
    print(f"  - Inserted: {stats['events_inserted']}")
    print(f"  - Updated: {stats['events_updated']}")
    print(f"  - Skipped (non-UFC): {stats['skipped_non_ufc']}")
    print(f"  - Skipped (old): {stats['skipped_old']}")
    print(f"Bouts processed: {stats['bouts_processed']}")
    print(f"  - Inserted: {stats['bouts_inserted']}")
    print(f"  - Updated: {stats['bouts_updated']}")
    print(f"  - Deleted (no longer in Tapology): {stats['bouts_deleted']}")
    print(f"  - Enriched with fighter details: {min(stats['bout_details_processed'], stats['bouts_processed'])}")


if __name__ == "__main__":
    main()
