"""
Spider de Backfill de Detalles de Peleadores

Este spider visita los perfiles de peleadores en Tapology para obtener
información detallada como altura, alcance, edad, nacionalidad, récord, etc.

Usage:
    scrapy crawl ufc_fighters                      # Todos los peleadores con datos faltantes
    scrapy crawl ufc_fighters -a EVENT_ID=135755   # Solo un evento específico
    scrapy crawl ufc_fighters -a LIMIT=100         # Limitar cantidad de peleadores
"""

import scrapy
import re
from motor.motor_asyncio import AsyncIOMotorClient
import os
from datetime import datetime, date


class UfcFightersSpider(scrapy.Spider):
    name = "ufc_fighters"
    allowed_domains = ["tapology.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,  # Be respectful, fighter pages have more data
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "ROBOTSTXT_OBEY": False,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {
            'tapology_scraper.spiders.ufc_fighters.UfcFightersPipeline': 300,
        },
        # Anti-ban headers
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.tapology.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    }

    def __init__(self, EVENT_ID=None, LIMIT=None, *args, **kwargs):
        super(UfcFightersSpider, self).__init__(*args, **kwargs)
        self.target_event_id = EVENT_ID
        self.limit = int(LIMIT) if LIMIT else None

        # MongoDB connection
        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en las variables de entorno")

        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks

        self.logger.info(f"UFC Fighters Spider initialized")
        if self.target_event_id:
            self.logger.info(f"Targeting specific event: {self.target_event_id}")
        if self.limit:
            self.logger.info(f"Limit: {self.limit} fighters")

    async def start(self):
        """Async entry point - load fighters needing details from MongoDB"""
        async for req in self.load_fighters_from_mongo():
            yield req

    async def load_fighters_from_mongo(self):
        """
        Cargar peleadores que necesitan detalles.

        Busca bouts donde los fighters tienen datos por defecto:
        - nationality: "Unknown"
        - age_at_fight_years: 0
        - height_cm: None
        - reach_cm: None
        """
        query = {
            "$or": [
                # Red fighter missing details
                {"fighters.red.nationality": "Unknown"},
                {"fighters.red.age_at_fight_years": 0},
                {"fighters.red.height_cm": None},
                {"fighters.red.reach_cm": None},
                # Blue fighter missing details
                {"fighters.blue.nationality": "Unknown"},
                {"fighters.blue.age_at_fight_years": 0},
                {"fighters.blue.height_cm": None},
                {"fighters.blue.reach_cm": None},
            ]
        }

        if self.target_event_id:
            query["event_id"] = int(self.target_event_id)

        try:
            # Get unique fighters from bouts
            bouts = await self.db.bouts.find(query).to_list(length=None)
            self.logger.info(f"Found {len(bouts)} bouts with missing fighter details")

            # Track unique fighters to avoid duplicates
            seen_fighter_ids = set()
            fighter_count = 0

            for bout in bouts:
                if self.limit and fighter_count >= self.limit:
                    break

                bout_id = bout.get("id") or bout.get("_id")
                fighters = bout.get("fighters", {})

                for corner in ["red", "blue"]:
                    if self.limit and fighter_count >= self.limit:
                        break

                    fighter = fighters.get(corner, {})
                    tapology_url = fighter.get("tapology_url")
                    tapology_id = fighter.get("tapology_id")
                    fighter_name = fighter.get("fighter_name", "Unknown")

                    # Skip if no URL or already processed
                    if not tapology_url or not tapology_id:
                        self.logger.warning(f"Bout {bout_id} {corner} fighter has no tapology_url")
                        continue

                    if tapology_id in seen_fighter_ids:
                        continue

                    # Check if fighter needs update
                    needs_update = (
                        fighter.get("nationality") == "Unknown" or
                        fighter.get("age_at_fight_years") == 0 or
                        fighter.get("height_cm") is None or
                        fighter.get("reach_cm") is None
                    )

                    if not needs_update:
                        continue

                    seen_fighter_ids.add(tapology_id)
                    fighter_count += 1

                    self.logger.info(f"Queuing fighter: {fighter_name} ({tapology_id})")

                    yield scrapy.Request(
                        url=tapology_url,
                        callback=self.parse_fighter,
                        meta={
                            "tapology_id": tapology_id,
                            "fighter_name": fighter_name,
                        },
                        errback=self.handle_error,
                        dont_filter=True
                    )

            self.logger.info(f"Total unique fighters to scrape: {fighter_count}")

        except Exception as e:
            self.logger.error(f"Error loading fighters from Mongo: {e}")

    def parse_fighter(self, response):
        """
        Extraer detalles del perfil del peleador en Tapology.

        La página tiene una estructura con:
        - Datos principales en una lista ul con data-controller
        - Record en formato W-L-D
        - Altura, alcance, edad, nacionalidad, etc.
        """
        tapology_id = response.meta["tapology_id"]
        fighter_name = response.meta["fighter_name"]

        self.logger.info(f"Parsing fighter: {fighter_name}")

        # Initialize extracted data
        data = {
            "tapology_id": tapology_id,
            "fighter_name": fighter_name,
        }

        # Extract details from the structured list
        details_list = response.css('ul[data-controller="unordered-list-background"] li')

        for li in details_list:
            label = li.css('span.font-bold::text').get()
            value_elem = li.css('span.text-neutral-700')

            if not label:
                continue

            label = label.strip().rstrip(':').lower()

            # Get value text, including from links
            value = value_elem.css("::text").get()
            if not value:
                value = value_elem.css("a::text").get()
            if value:
                value = value.strip()

            # Parse specific fields
            if label == 'nationality' or label == 'born':
                # Extract country from nationality or born field
                if value:
                    data["nationality"] = value

            elif label == 'age':
                # Extract age in years
                if value:
                    age_match = re.search(r'(\d+)', value)
                    if age_match:
                        data["age_years"] = int(age_match.group(1))

            elif label == 'height':
                # Extract height in cm
                # Format: "5'11" (180 cm)" or "180 cm"
                if value:
                    cm_match = re.search(r'(\d+)\s*cm', value)
                    if cm_match:
                        data["height_cm"] = int(cm_match.group(1))
                    else:
                        # Try to convert from feet/inches
                        ft_match = re.search(r"(\d+)'(\d+)", value)
                        if ft_match:
                            feet = int(ft_match.group(1))
                            inches = int(ft_match.group(2))
                            data["height_cm"] = int((feet * 12 + inches) * 2.54)

            elif label == 'reach':
                # Extract reach in cm
                # Format: "76" (193 cm)" or "193 cm"
                if value:
                    cm_match = re.search(r'(\d+)\s*cm', value)
                    if cm_match:
                        data["reach_cm"] = int(cm_match.group(1))
                    else:
                        # Try to convert from inches
                        inch_match = re.search(r'(\d+)"', value)
                        if inch_match:
                            inches = int(inch_match.group(1))
                            data["reach_cm"] = int(inches * 2.54)

            elif label == 'fighting out of':
                if value:
                    data["fighting_out_of"] = value

        # Extract record from the page
        # Record is usually displayed prominently, look for W-L-D pattern
        all_text = " ".join(response.css("body ::text").getall())

        # Look for record pattern like "25-5-0" or "25 - 5 - 0"
        # Usually near "Pro Record" or similar label
        record_patterns = [
            r'Pro\s+Record[:\s]+(\d+)\s*-\s*(\d+)\s*-\s*(\d+)',
            r'Record[:\s]+(\d+)\s*-\s*(\d+)\s*-\s*(\d+)',
            r'(\d+)\s*(?:wins?|W)\s*[-·]\s*(\d+)\s*(?:loss(?:es)?|L)\s*[-·]\s*(\d+)\s*(?:draws?|D)?',
        ]

        for pattern in record_patterns:
            record_match = re.search(pattern, all_text, re.IGNORECASE)
            if record_match:
                data["record"] = {
                    "wins": int(record_match.group(1)),
                    "losses": int(record_match.group(2)),
                    "draws": int(record_match.group(3))
                }
                break

        # Look for record in specific elements
        if "record" not in data:
            record_elem = response.css('span.record::text, div.record::text').get()
            if record_elem:
                match = re.search(r'(\d+)\s*-\s*(\d+)\s*-\s*(\d+)', record_elem)
                if match:
                    data["record"] = {
                        "wins": int(match.group(1)),
                        "losses": int(match.group(2)),
                        "draws": int(match.group(3))
                    }

        # Try to find ranking if available
        ranking_match = re.search(r'#(\d+)\s+(?:at\s+)?(\w+(?:\s+\w+)?)', all_text)
        if ranking_match:
            data["ranking"] = {
                "position": int(ranking_match.group(1)),
                "division": ranking_match.group(2)
            }

        # Extract nationality from flag if not found
        if "nationality" not in data:
            flag_img = response.css('img[src*="flags"]::attr(alt)').get()
            if flag_img:
                data["nationality"] = flag_img.strip()

        # Try alternate nationality extraction
        if "nationality" not in data:
            # Look for country name after flag emoji or in specific divs
            country_elem = response.css('span.nationality::text, div.nationality::text').get()
            if country_elem:
                data["nationality"] = country_elem.strip()

        self.logger.info(f"Extracted data for {fighter_name}: {data}")

        # Yield the fighter data for pipeline processing
        yield {
            "type": "fighter_details",
            **data
        }

    def handle_error(self, failure):
        """Manejo de errores HTTP"""
        request = failure.request
        self.logger.error(f"Request failed: {request.url}")
        self.logger.error(f"   Reason: {failure.value}")

    async def close(self, reason):
        """Cerrar conexión MongoDB al terminar"""
        self.mongo_client.close()
        self.logger.info(f"Spider closed: {reason}")


class UfcFightersPipeline:
    """
    Pipeline para actualizar MongoDB con los detalles de peleadores extraídos.

    Actualiza todos los bouts donde aparece el fighter con sus nuevos datos.
    """

    def __init__(self):
        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en las variables de entorno")
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks

    async def process_item(self, item, spider):
        """Procesar cada item y actualizar MongoDB"""

        if item.get("type") != "fighter_details":
            return item

        tapology_id = item.get("tapology_id")
        fighter_name = item.get("fighter_name")

        if not tapology_id:
            spider.logger.warning(f"No tapology_id for fighter {fighter_name}")
            return item

        # Build update document for fighter fields
        update_fields = {}

        if item.get("nationality") and item.get("nationality") != "Unknown":
            update_fields["nationality"] = item["nationality"]

        if item.get("age_years") and item["age_years"] > 0:
            update_fields["age_at_fight_years"] = item["age_years"]

        if item.get("height_cm"):
            update_fields["height_cm"] = item["height_cm"]

        if item.get("reach_cm"):
            update_fields["reach_cm"] = item["reach_cm"]

        if item.get("fighting_out_of"):
            update_fields["fighting_out_of"] = item["fighting_out_of"]

        if item.get("record"):
            update_fields["record_at_fight"] = item["record"]

        if item.get("ranking"):
            update_fields["ranking"] = item["ranking"]

        if not update_fields:
            spider.logger.warning(f"No fields to update for fighter {fighter_name}")
            return item

        # Update all bouts where this fighter appears in red corner
        red_update = {"$set": {f"fighters.red.{k}": v for k, v in update_fields.items()}}
        red_result = await self.db.bouts.update_many(
            {"fighters.red.tapology_id": tapology_id},
            red_update
        )

        # Update all bouts where this fighter appears in blue corner
        blue_update = {"$set": {f"fighters.blue.{k}": v for k, v in update_fields.items()}}
        blue_result = await self.db.bouts.update_many(
            {"fighters.blue.tapology_id": tapology_id},
            blue_update
        )

        total_updated = red_result.modified_count + blue_result.modified_count

        if total_updated > 0:
            spider.logger.info(f"Updated {total_updated} bouts for fighter {fighter_name}: {list(update_fields.keys())}")
        else:
            spider.logger.warning(f"No bouts updated for fighter {fighter_name} (tapology_id: {tapology_id})")

        return item

    def close_spider(self, spider):
        """Cerrar conexión al finalizar"""
        self.mongo_client.close()
