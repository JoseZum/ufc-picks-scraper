"""Settings de Scrapy. Ver https://docs.scrapy.org/en/latest/topics/settings.html"""

from pathlib import Path

from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

BOT_NAME = "tapology_scraper"

SPIDER_MODULES = ["tapology_scraper.spiders"]
NEWSPIDER_MODULE = "tapology_scraper.spiders"

ADDONS = {}

ROBOTSTXT_OBEY = False

# Tapology corta el acceso si se le pega rapido: una peticion por segundo.
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

ITEM_PIPELINES = {
    "tapology_scraper.pipelines.MongoDBPipeline": 300,
}

FEED_EXPORT_ENCODING = "utf-8"
