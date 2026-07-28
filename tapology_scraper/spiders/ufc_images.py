"""Legacy Tapology fighter-headshot refresh.

Event posters are intentionally out of scope: ``event_images`` owns them and
resolves them through Wikipedia's credited source.  Keeping that ownership
boundary prevents a manual legacy run from restoring Tapology poster URLs.

Usage:
    scrapy crawl ufc_images
    scrapy crawl ufc_images -a MODE=bouts
    scrapy crawl ufc_images -a EVENT_ID=135755
"""

import scrapy
import re
from motor.motor_asyncio import AsyncIOMotorClient
import os


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
        """Refresh fighter headshots only."""
        if self.mode == "events":
            self.logger.warning(
                "MODE=events is disabled; run `scrapy crawl event_images` "
                "for Wikipedia/UFC event art"
            )
            return

        async for req in self.load_bouts_from_mongo():
            yield req

    async def load_bouts_from_mongo(self):
        """Cargar bouts para refrescar fighter headshots.

        Siempre re-scrapea; si Tapology no tiene headshots válidos
        parse_bout_images no emite y se mantiene lo que ya había.
        """
        query = {}
        if self.target_event_id:
            query["event_id"] = int(self.target_event_id)
        
        try:
            bouts = await self.db.bouts.find(query).to_list(length=None)
            self.logger.info(f"Found {len(bouts)} bouts to scrape for fighter images")
            
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
        
        if item.get("type") == "bout_fighters":
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
            elif result.matched_count > 0:
                spider.logger.info(f"Bout {item['bout_id']} fighter images already up to date")
            else:
                spider.logger.warning(f"Bout {item['bout_id']} not found while updating fighter images")
        
        return item

    def close_spider(self, spider):
        """Cerrar conexión al finalizar"""
        self.mongo_client.close()
