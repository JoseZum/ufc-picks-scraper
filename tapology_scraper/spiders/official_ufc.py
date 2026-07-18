"""Fallback card scraper using the public UFC event pages.

This is intentionally a source fallback, not an attempt to work around
Tapology's anti-bot protection.  Existing events are reconciled by date and
existing bouts by their two fighter names so that unaffected user picks keep
their original IDs.  A replacement bout receives a stable UFC-derived ID;
the normal ingestion cleanup then removes the superseded bout and its picks.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import os
import re
import unicodedata
from zoneinfo import ZoneInfo

import scrapy
from pymongo import MongoClient


def normalized_name(value: str | None) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]", "", value.lower())


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def matchup_key(red_name: str, blue_name: str) -> tuple[str, str]:
    return tuple(sorted((normalized_name(red_name), normalized_name(blue_name))))


class OfficialUfcSpider(scrapy.Spider):
    name = "official_ufc"
    allowed_domains = ["ufc.com", "www.ufc.com", "ufcespanol.com", "www.ufcespanol.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {},
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "UFC-Picks data sync (contact: support@ufcpicks.app)",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(self, EVENT_ID=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mongo_uri = os.environ.get("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI is required for official UFC reconciliation")
        self.db = MongoClient(mongo_uri).ufc_picks
        self.target_event_id = int(EVENT_ID) if EVENT_ID else None
        self.events_by_date: dict[str, dict] = {}
        self.bouts_by_event: dict[int, dict[tuple[str, str], int]] = {}

    def start_requests(self):
        query = {"status": "scheduled"}
        if self.target_event_id is not None:
            query = {"_id": self.target_event_id}

        for event in self.db.events.find(query):
            event_id = int(event["_id"])
            self.bouts_by_event[event_id] = {
                matchup_key(
                    bout.get("fighters", {}).get("red", {}).get("fighter_name"),
                    bout.get("fighters", {}).get("blue", {}).get("fighter_name"),
                ): int(bout["_id"])
                for bout in self.db.bouts.find({"event_id": event_id})
            }
            event_date = event.get("date")
            if isinstance(event_date, datetime):
                self.events_by_date[event_date.date().isoformat()] = event

            official_url = event.get("official_url")
            if self.target_event_id is not None and official_url:
                yield scrapy.Request(official_url, callback=self.parse_event, meta={"existing_event": event})

        if self.target_event_id is None:
            yield scrapy.Request("https://www.ufc.com/events", callback=self.parse_index)

    def parse_index(self, response):
        seen = set()
        for href in response.css('a[href^="/event/"]::attr(href)').getall():
            url = response.urljoin(href)
            if url not in seen:
                seen.add(url)
                yield scrapy.Request(url, callback=self.parse_event)

    def parse_event(self, response):
        date_value = response.css("time[datetime]::attr(datetime)").get()
        if not date_value:
            self.logger.warning("Skipping UFC page without an event date: %s", response.url)
            return
        event_date = date_value[:10]
        start_time_et = None
        try:
            event_time = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
            start_time_et = event_time.astimezone(ZoneInfo("America/New_York")).strftime("%H:%M")
        except ValueError:
            pass

        existing_event = response.meta.get("existing_event") or self.events_by_date.get(event_date)
        event_id = int(existing_event["_id"]) if existing_event else self._generated_event_id(response.url)
        h1 = clean_text(" ".join(response.css("h1 ::text, h1::text").getall()))
        matchup = clean_text(" ".join(response.css(".c-hero__headline .e-divider ::text").getall()))
        event_name = f"{h1}: {matchup}" if h1 and matchup else (h1 or matchup)

        poster_url = response.css(".c-hero picture img::attr(src), .c-hero img::attr(src)").get()
        fights = []
        sections = (("#main-card", "Main Card"), ("#prelims-card", "Prelim"), ("#early-prelims", "Early Prelim"))
        for selector, card in sections:
            for order, fight in enumerate(response.css(f"{selector} .c-listing-fight"), start=1):
                red_name = clean_text(" ".join(fight.css(".c-listing-fight__corner-name--red ::text, .c-listing-fight__corner-name--red::text").getall()))
                blue_name = clean_text(" ".join(fight.css(".c-listing-fight__corner-name--blue ::text, .c-listing-fight__corner-name--blue::text").getall()))
                if not red_name or not blue_name:
                    continue
                fights.append((fight, card, order, red_name, blue_name))

        if not fights:
            self.logger.warning("Skipping UFC page without fights: %s", response.url)
            return

        yield {
            "type": "event",
            "event_id": event_id,
            "name": event_name,
            "event_date": event_date,
            "start_time_et": start_time_et,
            "timezone": "ET",
            "promotion": "Ultimate Fighting Championship",
            "total_bouts": len(fights),
            "official_url": response.url,
            "tapology_url": existing_event.get("tapology_url") if existing_event else None,
            "poster_image_url": poster_url,
            "source": "ufc_official",
        }

        known_bouts = self.bouts_by_event.get(event_id, {})
        for position, (fight, card, order, red_name, blue_name) in enumerate(fights):
            existing_bout_id = known_bouts.get(matchup_key(red_name, blue_name))
            fmid = fight.attrib.get("data-fmid", str(position))
            bout_id = existing_bout_id or self._generated_bout_id(event_id, fmid)
            image_urls = fight.css(".c-listing-fight__corner-image--red img::attr(src), .c-listing-fight__corner-image--blue img::attr(src)").getall()
            weight_class = clean_text(" ".join(fight.css(".c-listing-fight__class-text ::text, .c-listing-fight__class-text::text").getall()))

            yield {
                "type": "bout",
                "event_id": event_id,
                "bout_id": bout_id,
                "card": card,
                "order": order,
                "is_main_event": card == "Main Card" and order == 1,
                "is_co_main_event": card == "Main Card" and order == 2,
                "is_title_fight": "title" in weight_class.lower() or "championship" in weight_class.lower(),
                "weight_class": weight_class or None,
                "cancelled": False,
                "status": "scheduled",
                "source": "ufc_official",
                "official_url": response.url,
                "fighters": {
                    "red": {"name": red_name, "profile_image_url": image_urls[0] if image_urls else None},
                    "blue": {"name": blue_name, "profile_image_url": image_urls[1] if len(image_urls) > 1 else None},
                },
                "official_fight_id": fmid,
            }

    @staticmethod
    def _generated_event_id(url: str) -> int:
        return 8_000_000_000 + int(hashlib.sha1(url.encode()).hexdigest()[:8], 16)

    @staticmethod
    def _generated_bout_id(event_id: int, official_fight_id: str) -> int:
        digest = hashlib.sha1(f"{event_id}:{official_fight_id}".encode()).hexdigest()[:8]
        return 9_000_000_000 + int(digest, 16)
