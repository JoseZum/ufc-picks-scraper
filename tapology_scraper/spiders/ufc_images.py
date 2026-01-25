"""
Spider de Backfill de Imágenes UFC

Este obtiene posters y headshots para completar registros existentes.

Usage:
    scrapy crawl ufc_images                      # Ambos (posters + headshots)
    scrapy crawl ufc_images -a MODE=events       # Solo posters
    scrapy crawl ufc_images -a MODE=bouts        # Solo headshots
    scrapy crawl ufc_images -a EVENT_ID=135755   # Solo un evento específico
"""

import scrapy
import re
from motor.motor_asyncio import AsyncIOMotorClient
import os
from urllib.parse import urljoin


class UfcImagesSpider(scrapy.Spider):
    name = "ufc_images"
    allowed_domains = ["tapology.com"]
    
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "ROBOTSTXT_OBEY": False,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {
            'tapology_scraper.spiders.ufc_images.UfcImagesPipeline': 300,
        },
        # Anti-ban headers
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.tapology.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    }

    def __init__(self, MODE=None, EVENT_ID=None, *args, **kwargs):
        super(UfcImagesSpider, self).__init__(*args, **kwargs)
        self.mode = MODE  # 'events', 'bouts', o None (ambos)
        self.target_event_id = EVENT_ID
        
        # MongoDB connection
        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en las variables de entorno")

        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        
        self.logger.info(f"UFC Images Spider initialized - MODE: {self.mode or 'ALL'}")
        if self.target_event_id:
            self.logger.info(f"Targeting specific event: {self.target_event_id}")

    async def start(self):
        """Async entry point - decide qué scrappear según MODE"""
        if self.mode == "bouts":
            # Solo headshots
            async for req in self.load_bouts_from_mongo():
                yield req
        elif self.mode == "events":
            # Solo posters
            async for req in self.load_events_from_mongo():
                yield req
        else:
            # Ambos (default)
            async for req in self.load_events_from_mongo():
                yield req
            async for req in self.load_bouts_from_mongo():
                yield req

    async def load_events_from_mongo(self):
        """Cargar eventos que necesitan poster_image_url"""
        query = {"poster_image_url": None}
        
        if self.target_event_id:
            query["event_id"] = int(self.target_event_id)
        
        try:
            events = await self.db.events.find(query).to_list(length=None)
            self.logger.info(f"Found {len(events)} events without poster images")
            
            for event in events:
                tap_url = event.get("tapology_url") or event.get("url")
                event_id = event.get("id") or event.get("_id") or event.get("event_id")
                if not tap_url:
                    self.logger.warning(f"Event {event_id} has no tapology/url")
                    continue

                yield scrapy.Request(
                    url=tap_url,
                    callback=self.parse_event_images,
                    meta={"event_id": event_id},
                    errback=self.handle_error,
                    dont_filter=True
                )
        except Exception as e:
            self.logger.error(f"Error loading events from Mongo: {e}")

    async def load_bouts_from_mongo(self):
        """Cargar bouts que necesitan fighter headshots"""
        query = {
            "$or": [
                {"fighters.red.profile_image_url": None},
                {"fighters.blue.profile_image_url": None}
            ]
        }
        
        if self.target_event_id:
            query["event_id"] = int(self.target_event_id)
        
        try:
            bouts = await self.db.bouts.find(query).to_list(length=None)
            self.logger.info(f"📊 Found {len(bouts)} bouts without fighter images")
            
            for bout in bouts:
                tap_url = bout.get("tapology_url") or bout.get("url")
                bout_id = bout.get("id") or bout.get("_id") or bout.get("bout_id")
                if not tap_url:
                    self.logger.warning(f"Bout {bout_id} has no tapology/url")
                    continue

                yield scrapy.Request(
                    url=tap_url,
                    callback=self.parse_bout_images,
                    meta={"bout_id": bout_id},
                    errback=self.handle_error,
                    dont_filter=True
                )
        except Exception as e:
            self.logger.error(f"Error loading bouts from Mongo: {e}")

    def parse_event_images(self, response):
        """Extraer poster del evento"""
        event_id = response.meta["event_id"]
        
        # Buscar poster image
        poster_imgs = response.css('img[src*="poster_images"]::attr(src)').getall()
        
        if not poster_imgs:
            self.logger.warning(f"No poster found for event {event_id}")
            return
        
        # Tomar la primera imagen de poster encontrada
        raw_url = poster_imgs[0]
        
        # Normalizar a formato proxy
        # De: https://images.tapology.com/poster_images/135755/profile/xxx.jpg
        # A: /proxy/tapology/poster_images/135755/profile/xxx.jpg
        normalized_url = self._normalize_image_url(raw_url)
        
        if not normalized_url:
            self.logger.warning(f"Could not normalize poster URL: {raw_url}")
            return
        
        self.logger.info(f"Found poster for event {event_id}: {normalized_url}")
        
        # Actualizar MongoDB
        yield {
            "type": "event_poster",
            "event_id": event_id,
            "poster_image_url": normalized_url
        }

    def parse_bout_images(self, response):
        """Extraer headshots de fighters (red y blue)"""
        bout_id = response.meta["bout_id"]
        
        # Buscar headshot images
        # Tapology muestra primero red corner, luego blue corner
        headshot_imgs = response.css('img[src*="headshot_images"]::attr(src)').getall()
        
        if len(headshot_imgs) < 2:
            # Intentar con letterbox_images como fallback
            headshot_imgs = response.css('img[src*="letterbox_images"]::attr(src)').getall()
        
        if len(headshot_imgs) < 2:
            self.logger.warning(f"Not enough fighter images for bout {bout_id} (found {len(headshot_imgs)})")
            return
        
        # Normalizar URLs
        red_img = self._normalize_image_url(headshot_imgs[0])
        blue_img = self._normalize_image_url(headshot_imgs[1])
        
        if not red_img or not blue_img:
            self.logger.warning(f"Could not normalize fighter images for bout {bout_id}")
            return
        
        self.logger.info(f"Found fighters for bout {bout_id}")
        self.logger.info(f"   Red: {red_img}")
        self.logger.info(f"   Blue: {blue_img}")
        
        # Actualizar MongoDB
        yield {
            "type": "bout_fighters",
            "bout_id": bout_id,
            "red_profile_image_url": red_img,
            "blue_profile_image_url": blue_img
        }

    def _normalize_image_url(self, raw_url):
        """
        Normalizar URL de imagen a formato proxy
        
        Input: https://images.tapology.com/poster_images/135755/profile/xxx.jpg
        Output: /proxy/tapology/poster_images/135755/profile/xxx.jpg
        
        Input: https://images.tapology.com/letterbox_images/16421/default/image.jpg
        Output: /proxy/tapology/letterbox_images/16421/default/image.jpg
        """
        if not raw_url:
            return None
        
        # Extraer path después de images.tapology.com
        match = re.search(r'images\.tapology\.com(/.*)', raw_url)
        if match:
            path = match.group(1)
            return f"/proxy/tapology{path}"
        
        # Si ya viene como path relativo
        if raw_url.startswith('/'):
            return f"/proxy/tapology{raw_url}"
        
        return None

    def handle_error(self, failure):
        """Manejo de errores HTTP"""
        request = failure.request
        self.logger.error(f"Request failed: {request.url}")
        self.logger.error(f"   Reason: {failure.value}")

    async def close(self, reason):
        """Cerrar conexión MongoDB al terminar"""
        self.mongo_client.close()
        self.logger.info(f"Spider closed: {reason}")


class UfcImagesPipeline:
    """
    Pipeline para actualizar MongoDB con las imágenes extraídas
    
    IMPORTANTE: El pipeline debe estar activado en settings.py:
    
    ITEM_PIPELINES = {
        'tapology_scraper.pipelines.UfcImagesPipeline': 300,
    }
    """
    
    def __init__(self):
        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en las variables de entorno")
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks

    async def process_item(self, item, spider):
        """Procesar cada item y actualizar MongoDB"""
        
        if item.get("type") == "event_poster":
            # Actualizar poster del evento
            result = await self.db.events.update_one(
                {"id": item["event_id"]},
                {"$set": {"poster_image_url": item["poster_image_url"]}}
            )
            
            if result.modified_count > 0:
                spider.logger.info(f"Updated event {item['event_id']} poster")
            else:
                spider.logger.warning(f"Event {item['event_id']} not updated (already had poster or not found)")
        
        elif item.get("type") == "bout_fighters":
            # Actualizar headshots de fighters
            result = await self.db.bouts.update_one(
                {"id": item["bout_id"]},
                {
                    "$set": {
                        "fighters.red.profile_image_url": item["red_profile_image_url"],
                        "fighters.blue.profile_image_url": item["blue_profile_image_url"]
                    }
                }
            )
            
            if result.modified_count > 0:
                spider.logger.info(f"Updated bout {item['bout_id']} fighter images")
            else:
                spider.logger.warning(f"Bout {item['bout_id']} not updated (already had images or not found)")
        
        return item

    def close_spider(self, spider):
        """Cerrar conexión al finalizar"""
        self.mongo_client.close()
