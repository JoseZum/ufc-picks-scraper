"""
MongoDB Pipeline for UFC Scraper

This pipeline handles inserting and updating scraped UFC data into MongoDB.
It processes events, bouts, and bout details separately.
"""

import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime


class MongoDBPipeline:
    """
    Pipeline para guardar datos scrapeados en MongoDB.

    Maneja tres tipos de items:
    - event: Información del evento
    - bout: Información básica de la pelea
    - bout_detail: Detalles completos de la pelea (con datos comparativos)
    """

    def __init__(self):
        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en las variables de entorno")

        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        self.logger = logging.getLogger(__name__)

    async def process_item(self, item, spider):
        """Process different item types"""

        item_type = item.get("type")

        if item_type == "event":
            await self.process_event(item, spider)
        elif item_type == "bout":
            await self.process_bout(item, spider)
        elif item_type == "bout_detail":
            await self.process_bout_detail(item, spider)
        else:
            self.logger.warning(f"Unknown item type: {item_type}")

        return item

    async def process_event(self, item, spider):
        """Insert or update event data"""

        event_id = item.get("event_id")

        if not event_id:
            self.logger.error("Event missing event_id")
            return

        # Convert event_id to int for consistency
        try:
            event_id = int(event_id)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid event_id: {event_id}")
            return

        # Prepare event document
        event_doc = {
            "id": event_id,
            "name": item.get("name"),
            "event_date": item.get("event_date"),
            "start_time_et": item.get("start_time_et"),
            "timezone": item.get("timezone"),
            "broadcast_us": item.get("broadcast_us"),
            "promotion": item.get("promotion"),
            "owner": item.get("owner"),
            "venue": item.get("venue"),
            "location": item.get("location"),
            "total_bouts": item.get("total_bouts"),
            "tapology_url": item.get("tapology_url"),
            "scraped_at": datetime.utcnow().isoformat(),
        }

        # Remove None values
        event_doc = {k: v for k, v in event_doc.items() if v is not None}

        # Upsert event
        await self.db.events.update_one(
            {"id": event_id},
            {"$set": event_doc},
            upsert=True
        )

        spider.logger.info(f"Saved event: {event_id} - {item.get('name')}")

    async def process_bout(self, item, spider):
        """Insert or update bout data"""

        bout_id = item.get("bout_id")
        event_id = item.get("event_id")

        if not bout_id or not event_id:
            self.logger.error(f"Bout missing bout_id or event_id: {item}")
            return

        # Convert IDs to int
        try:
            bout_id = int(bout_id)
            event_id = int(event_id)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid IDs - bout_id: {bout_id}, event_id: {event_id}")
            return

        # Prepare bout document
        bout_doc = {
            "id": bout_id,
            "event_id": event_id,
            "card": item.get("card"),
            "order": item.get("order"),
            "is_main_event": item.get("is_main_event", False),
            "is_co_main_event": item.get("is_co_main_event", False),
            "is_title_fight": item.get("is_title_fight", False),
            "weight_lbs": item.get("weight_lbs"),
            "weight_class": item.get("weight_class"),
            "scheduled_rounds": item.get("scheduled_rounds"),
            "cancelled": item.get("cancelled", False),
            "status": item.get("status", "scheduled"),
            "tapology_url": item.get("tapology_url"),
            "scraped_at": datetime.utcnow().isoformat(),
        }

        # Process fighters data
        fighters = item.get("fighters", {})
        if fighters:
            bout_doc["fighters"] = {
                "red": self._prepare_fighter_data(fighters.get("red", {})),
                "blue": self._prepare_fighter_data(fighters.get("blue", {}))
            }

        # Remove None values
        bout_doc = {k: v for k, v in bout_doc.items() if v is not None}

        # Upsert bout
        await self.db.bouts.update_one(
            {"id": bout_id},
            {"$set": bout_doc},
            upsert=True
        )

        spider.logger.info(f"Saved bout: {bout_id} - Event {event_id}")

    async def process_bout_detail(self, item, spider):
        """Update bout with detailed information"""

        bout_id = item.get("bout_id")
        event_id = item.get("event_id")

        if not bout_id or not event_id:
            self.logger.error(f"Bout detail missing bout_id or event_id: {item}")
            return

        # Convert IDs to int
        try:
            bout_id = int(bout_id)
            event_id = int(event_id)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid IDs - bout_id: {bout_id}, event_id: {event_id}")
            return

        # Prepare update document with detailed info
        update_doc = {
            "bout_date": item.get("bout_date"),
            "broadcast": item.get("broadcast"),
            "weight_info": item.get("weight_info"),
            "updated_at": datetime.utcnow().isoformat(),
        }

        # Update fighters with detailed comparison data
        fighters = item.get("fighters", {})
        if fighters:
            update_doc["fighters"] = {
                "red": self._prepare_fighter_data(fighters.get("red", {})),
                "blue": self._prepare_fighter_data(fighters.get("blue", {}))
            }

        # Update result if available
        if item.get("result"):
            update_doc["result"] = item["result"]
            # Also update status if result is present
            if item["result"].get("winner"):
                update_doc["status"] = "completed"

        # Remove None values
        update_doc = {k: v for k, v in update_doc.items() if v is not None}

        # Update bout
        result = await self.db.bouts.update_one(
            {"id": bout_id},
            {"$set": update_doc}
        )

        if result.modified_count > 0:
            spider.logger.info(f"Updated bout details: {bout_id}")
        else:
            spider.logger.warning(f"No bout found to update: {bout_id}")

    def _prepare_fighter_data(self, fighter_data):
        """Prepare fighter data, removing None values and ensuring consistency"""

        if not fighter_data:
            return {
                "fighter_name": None,
                "tapology_id": None,
                "tapology_url": None,
                "nationality": "Unknown",
                "age_at_fight_years": 0,
                "height_cm": None,
                "reach_cm": None
            }

        # Use 'name' or 'fighter_name'
        name = fighter_data.get("name") or fighter_data.get("fighter_name")

        prepared = {
            "fighter_name": name,
            "tapology_id": fighter_data.get("tapology_id"),
            "tapology_url": fighter_data.get("tapology_url"),
            "nickname": fighter_data.get("nickname"),
            "nationality": fighter_data.get("nationality", "Unknown"),
            "fighting_out_of": fighter_data.get("fighting_out_of"),
            "age_at_fight_years": fighter_data.get("age_at_fight", {}).get("years", 0) if isinstance(fighter_data.get("age_at_fight"), dict) else 0,
            "age_at_fight": fighter_data.get("age_at_fight"),
            "height_cm": fighter_data.get("height", {}).get("cm") if isinstance(fighter_data.get("height"), dict) else None,
            "height": fighter_data.get("height"),
            "reach_cm": fighter_data.get("reach", {}).get("cm") if isinstance(fighter_data.get("reach"), dict) else None,
            "reach": fighter_data.get("reach"),
            "latest_weight": fighter_data.get("latest_weight"),
            "record_at_fight": fighter_data.get("record_at_fight"),
            "ufc_ranking": fighter_data.get("ufc_ranking"),
            "last_5_fights": fighter_data.get("last_5_fights"),
            "betting_odds": fighter_data.get("betting_odds"),
            "title_status": fighter_data.get("title_status"),
            "gym": fighter_data.get("gym"),
        }

        # Remove None values
        return {k: v for k, v in prepared.items() if v is not None}

    def close_spider(self, spider):
        """Close MongoDB connection when spider closes"""
        self.mongo_client.close()
        spider.logger.info("MongoDB connection closed")
