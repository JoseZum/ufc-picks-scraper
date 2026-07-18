"""One-time cleanup for the failed official-UFC fallback run on 2026-07-18.

The run created synthetic IDs above 8,000,000,000.  Those IDs never existed
in the Tapology-backed model and can be removed without touching normal data.
"""

import os
from pymongo import MongoClient


mongo_uri = os.environ["MONGODB_URI"]
db = MongoClient(mongo_uri).ufc_picks

synthetic_events = list(db.events.find(
    {"_id": {"$gte": 8_000_000_000}, "source": "ufc_official"},
    {"_id": 1},
))
event_ids = [event["_id"] for event in synthetic_events]
if len(event_ids) != 8:
    raise RuntimeError(f"Expected exactly 8 synthetic events, found {len(event_ids)}: {event_ids}")

bout_ids = [
    bout["_id"]
    for bout in db.bouts.find({"event_id": {"$in": event_ids}}, {"_id": 1})
]

print(f"Removing {len(event_ids)} synthetic events and {len(bout_ids)} synthetic bouts")
print(f"Deleted picks: {db.picks.delete_many({'bout_id': {'$in': bout_ids}}).deleted_count}")
print(f"Deleted card slots: {db.event_card_slots.delete_many({'bout_id': {'$in': bout_ids}}).deleted_count}")
print(f"Deleted bout details: {db.bout_details.delete_many({'bout_id': {'$in': bout_ids}}).deleted_count}")
print(f"Deleted bouts: {db.bouts.delete_many({'event_id': {'$in': event_ids}}).deleted_count}")
print(f"Deleted events: {db.events.delete_many({'_id': {'$in': event_ids}}).deleted_count}")
