"""Resolve the two event images used by UFC Picks.

This spider intentionally does not download or persist image bytes:

* ``poster_image_url`` is the vertical card poster.  It is discovered through
  the event's Wikipedia file page.  The original ``Source``/``Credit`` page is
  preferred (for example an X post whose ``og:image`` points at pbs.twimg.com);
  Wikipedia's own low-resolution file is retained as a safe fallback.
* ``hero_image_url`` is the wide, high-resolution art used by landing/detail
  heroes.  It comes from the official UFC event page and specifically prefers
  the ``background_image_xl_2x`` source from the page's ``<picture>`` element.

Only URL metadata is written to MongoDB.  Existing event, bout, and pick data
is never reconciled or deleted by this spider.
"""

from __future__ import annotations

from datetime import datetime
from difflib import SequenceMatcher
from html import unescape
import os
import re
import unicodedata
from urllib.parse import urlencode, urljoin, urlparse

from parsel import Selector
from pymongo import MongoClient
import scrapy


WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
UFC_EVENTS_URL = "https://www.ufcespanol.com/events"

_GENERIC_EVENT_WORDS = {
    "file",
    "jpg",
    "jpeg",
    "png",
    "poster",
    "ufc",
    "fight",
    "night",
    "the",
    "vs",
    "and",
}
_DIRECT_IMAGE_EXTENSIONS = (".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp")
_X_SOURCE_PAGE_HOSTS = {
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
}


def _ascii_text(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(char for char in normalized if not unicodedata.combining(char))


def clean_event_name(value: str | None) -> str:
    """Collapse duplicated UFC headings found in some stored event names."""
    value = re.sub(r"\s+", " ", value or "").strip()
    value = re.sub(
        r"^(UFC\s+Fight\s+Night)\s+\1\s*:?\s*",
        r"\1: ",
        value,
        flags=re.IGNORECASE,
    )
    return value.strip(" :-")


def _normalized_words(value: str | None) -> list[str]:
    value = _ascii_text(value).lower()
    value = re.sub(r"^file:", "", value)
    value = re.sub(r"\.(?:avif|gif|jpe?g|png|webp)$", "", value)
    return re.findall(r"[a-z0-9]+", value)


def wikipedia_candidate_score(event_name: str, file_title: str) -> float:
    """Score a Wikipedia file title while strongly weighting matchup names."""
    event_words = _normalized_words(clean_event_name(event_name))
    title_words = _normalized_words(file_title)
    if "ufc" not in title_words:
        return 0.0

    event_normalized = " ".join(event_words)
    title_normalized = " ".join(title_words)
    full_ratio = SequenceMatcher(None, event_normalized, title_normalized).ratio()

    event_distinctive = {
        word for word in event_words if word not in _GENERIC_EVENT_WORDS
    }
    title_distinctive = {
        word for word in title_words if word not in _GENERIC_EVENT_WORDS
    }
    if not event_distinctive:
        return full_ratio

    overlap = len(event_distinctive & title_distinctive) / len(event_distinctive)
    return (full_ratio * 0.45) + (overlap * 0.55)


def extract_credit_url(credit_html: str | None) -> str | None:
    """Extract the external Source/Credit URL returned by MediaWiki."""
    if not credit_html:
        return None

    selector = Selector(text=unescape(credit_html))
    for href in selector.css("a::attr(href)").getall():
        absolute = href if href.startswith(("http://", "https://")) else None
        if not absolute:
            continue
        host = urlparse(absolute).hostname or ""
        if not host.endswith(("wikipedia.org", "wikimedia.org")):
            return absolute

    plain_match = re.search(r"https?://[^\s<>\"']+", unescape(credit_html))
    return plain_match.group(0) if plain_match else None


def select_wikipedia_candidate(
    payload: dict,
    event_name: str,
    minimum_score: float = 0.62,
) -> dict | None:
    """Return the best sufficiently close Wikipedia file search result."""
    pages = payload.get("query", {}).get("pages", {})
    candidates: list[tuple[float, dict]] = []

    for page in pages.values():
        image_info = (page.get("imageinfo") or [{}])[0]
        image_url = image_info.get("url")
        if not image_url:
            continue

        score = wikipedia_candidate_score(event_name, page.get("title", ""))
        metadata = image_info.get("extmetadata") or {}
        categories = str(metadata.get("Categories", {}).get("value", "")).lower()
        if "ultimate fighting championship event posters" in categories:
            score += 0.08

        credit_html = metadata.get("Credit", {}).get("value")
        candidates.append(
            (
                score,
                {
                    "score": score,
                    "file_title": page.get("title"),
                    "image_url": image_url,
                    "description_url": image_info.get("descriptionurl"),
                    "source_page_url": extract_credit_url(credit_html),
                },
            )
        )

    if not candidates:
        return None

    score, candidate = max(candidates, key=lambda entry: entry[0])
    return candidate if score >= minimum_score else None


def select_wikipedia_article(payload: dict, event_name: str) -> dict | None:
    """Match the event article and select its UFC poster file.

    Article membership matters because newer Wikipedia posters may use a
    numbered filename (for example ``UFC Fight Night 283.jpg``) that cannot be
    matched reliably from the two fighter names alone.
    """
    candidates: list[tuple[float, dict]] = []
    for page in payload.get("query", {}).get("pages", {}).values():
        score = wikipedia_candidate_score(event_name, page.get("title", ""))
        poster_files = [
            image.get("title")
            for image in page.get("images", [])
            if image.get("title")
            and "ufc" in image["title"].lower()
            and image["title"].lower().endswith(_DIRECT_IMAGE_EXTENSIONS)
        ]
        if not poster_files:
            continue

        poster_file = max(
            poster_files,
            key=lambda title: wikipedia_candidate_score(event_name, title),
        )
        candidates.append(
            (
                score,
                {
                    "score": score,
                    "article_url": page.get("fullurl"),
                    "file_title": poster_file,
                },
            )
        )

    if not candidates:
        return None

    score, candidate = max(candidates, key=lambda entry: entry[0])
    return candidate if score >= 0.62 else None


def is_direct_image_url(url: str | None) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    return path.endswith(_DIRECT_IMAGE_EXTENSIONS) or "pbs.twimg.com/media/" in url


def is_supported_source_page(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not host:
        return False
    return not host.endswith(("wikipedia.org", "wikimedia.org"))


def is_x_source_page(url: str | None) -> bool:
    if not url:
        return False
    return (urlparse(url).hostname or "").lower() in _X_SOURCE_PAGE_HOSTS


def extract_source_image_url(response) -> str | None:
    """Extract the original poster URL from an allowed source page."""
    meta_selectors = (
        'meta[property="og:image:secure_url"]::attr(content)',
        'meta[property="og:image"]::attr(content)',
        'meta[name="twitter:image"]::attr(content)',
        'meta[name="twitter:image:src"]::attr(content)',
    )
    for selector in meta_selectors:
        url = response.css(selector).get()
        if url and is_direct_image_url(url):
            return response.urljoin(unescape(url))

    # X also emits a high-resolution imageSrcSet preload.  Prefer the largest
    # candidate when og:image is temporarily absent.
    source_sets = response.xpath(
        '//link[@rel="preload" and @as="image"]/@imageSrcSet'
        ' | //link[@rel="preload" and @as="image"]/@imagesrcset'
    ).getall()
    for source_set in source_sets:
        candidates = [
            part.strip().split(" ")[0]
            for part in unescape(source_set).split(",")
            if part.strip()
        ]
        if candidates:
            return response.urljoin(candidates[-1])
    return None


def extract_ufc_hero_url(response) -> str | None:
    """Select the official UFC XL 2x hero image from a page's picture markup."""
    source_sets = response.css(
        '.c-hero picture source::attr(srcset), '
        'source[srcset*="background_image_xl"]::attr(srcset)'
    ).getall()
    candidates: list[tuple[int, str]] = []

    for source_set in source_sets:
        for part in unescape(source_set).split(","):
            tokens = part.strip().split()
            if not tokens:
                continue
            url = tokens[0]
            descriptor = tokens[1].lower() if len(tokens) > 1 else ""
            score = 0
            if "background_image_xl_2x" in url:
                score = 100
            elif "background_image_xl" in url and descriptor == "2x":
                score = 90
            elif "background_image_xl" in url:
                score = 70
            if score:
                candidates.append((score, response.urljoin(url)))

    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate[0])[1]


def extract_ufc_event_date(response) -> str | None:
    date_time = response.css("time[datetime]::attr(datetime)").get()
    if not date_time:
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})", date_time)
    return match.group(1) if match else None


def _event_date_string(event: dict) -> str | None:
    value = event.get("date") or event.get("event_date")
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, str):
        match = re.match(r"(\d{4}-\d{2}-\d{2})", value)
        return match.group(1) if match else None
    return None


def _page_event_name(response) -> str:
    heading = " ".join(response.css("h1 ::text, h1::text").getall())
    matchup = " ".join(
        response.css(
            ".c-hero__headline .e-divider ::text, "
            ".c-hero__headline .e-divider::text"
        ).getall()
    )
    title = response.css("title::text").get() or ""
    return clean_event_name(" ".join((heading, matchup, title)))


class EventImagesSpider(scrapy.Spider):
    name = "event_images"

    custom_settings = {
        "DOWNLOAD_DELAY": 0.35,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {},
        "ROBOTSTXT_OBEY": True,
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "UFC-Picks/1.0 event image resolver",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(self, EVENT_ID=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mongo_uri = os.environ.get("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI is required")

        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        self.target_event_id = int(EVENT_ID) if EVENT_ID else None
        self.events: dict[int, dict] = {}
        self.events_by_date: dict[str, list[dict]] = {}
        self.requested_official_urls: set[str] = set()

    async def start(self):
        query: dict = {"name": {"$regex": r"^UFC", "$options": "i"}}
        if self.target_event_id is not None:
            query = {"id": self.target_event_id}

        projection = {
            "_id": 1,
            "id": 1,
            "name": 1,
            "date": 1,
            "event_date": 1,
            "official_url": 1,
            "url": 1,
        }
        for event in self.db.events.find(query, projection):
            event_id = int(event.get("id") or event["_id"])
            event["id"] = event_id
            self.events[event_id] = event
            date_string = _event_date_string(event)
            if date_string:
                self.events_by_date.setdefault(date_string, []).append(event)

            yield self._wikipedia_article_request(event)

            official_url = event.get("official_url")
            if not official_url and "ufc.com/event/" in str(event.get("url", "")):
                official_url = event["url"]
            if official_url:
                official_url = official_url.replace("www.ufc.com", "www.ufcespanol.com")
                self.requested_official_urls.add(official_url)
                yield scrapy.Request(
                    official_url,
                    callback=self.parse_ufc_event,
                    errback=self.log_request_error,
                    cb_kwargs={"event_id": event_id},
                    dont_filter=True,
                )

        if not self.events:
            self.logger.warning("No UFC events found for image refresh")
            return

        yield scrapy.Request(
            UFC_EVENTS_URL,
            callback=self.parse_ufc_index,
            errback=self.log_request_error,
            dont_filter=True,
        )

    def _wikipedia_article_request(self, event: dict) -> scrapy.Request:
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": clean_event_name(event.get("name")),
            "gsrnamespace": 0,
            "gsrlimit": 6,
            "prop": "images|info",
            "imlimit": "max",
            "inprop": "url",
            "redirects": 1,
            "origin": "*",
        }
        return scrapy.Request(
            f"{WIKIPEDIA_API_URL}?{urlencode(params)}",
            callback=self.parse_wikipedia_article_search,
            errback=self.log_request_error,
            cb_kwargs={"event_id": event["id"]},
            dont_filter=True,
        )

    def _wikipedia_file_search_request(self, event: dict) -> scrapy.Request:
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": clean_event_name(event.get("name")),
            "gsrnamespace": 6,
            "gsrlimit": 8,
            "prop": "imageinfo",
            "iiprop": "url|extmetadata",
            "origin": "*",
        }
        return scrapy.Request(
            f"{WIKIPEDIA_API_URL}?{urlencode(params)}",
            callback=self.parse_wikipedia_search,
            errback=self.log_request_error,
            cb_kwargs={"event_id": event["id"]},
            dont_filter=True,
        )

    def _wikipedia_file_request(
        self,
        event: dict,
        file_title: str,
        article_url: str | None,
    ) -> scrapy.Request:
        params = {
            "action": "query",
            "format": "json",
            "titles": file_title,
            "prop": "imageinfo",
            "iiprop": "url|extmetadata",
            "origin": "*",
        }
        return scrapy.Request(
            f"{WIKIPEDIA_API_URL}?{urlencode(params)}",
            callback=self.parse_wikipedia_search,
            errback=self.log_request_error,
            cb_kwargs={
                "event_id": event["id"],
                "article_url": article_url,
                "exact_file": True,
            },
            dont_filter=True,
        )

    def parse_wikipedia_article_search(self, response, event_id: int):
        event = self.events[event_id]
        article = select_wikipedia_article(response.json(), event.get("name", ""))
        if article:
            yield self._wikipedia_file_request(
                event,
                article["file_title"],
                article.get("article_url"),
            )
            return

        # Fallback for events whose article has not been created yet but whose
        # poster file already exists in Wikipedia's File namespace.
        yield self._wikipedia_file_search_request(event)

    def parse_wikipedia_search(
        self,
        response,
        event_id: int,
        article_url: str | None = None,
        exact_file: bool = False,
    ):
        event = self.events[event_id]
        candidate = select_wikipedia_candidate(
            response.json(),
            event.get("name", ""),
            minimum_score=0.0 if exact_file else 0.62,
        )
        if not candidate:
            self.logger.warning(
                "No matching Wikipedia poster found for %s (%s)",
                event.get("name"),
                event_id,
            )
            return

        metadata = {
            "wikipedia_file_url": candidate.get("description_url"),
            "wikipedia_image_url": candidate["image_url"],
            "wikipedia_article_url": article_url,
            "poster_source_page_url": candidate.get("source_page_url"),
        }
        source_page_url = candidate.get("source_page_url")

        if is_direct_image_url(source_page_url):
            self._save_poster(
                event_id,
                source_page_url,
                "wikipedia_source",
                metadata,
            )
            return

        if is_supported_source_page(source_page_url):
            request_meta = (
                {"dont_obey_robotstxt": True}
                if is_x_source_page(source_page_url)
                else {}
            )
            yield scrapy.Request(
                source_page_url,
                callback=self.parse_poster_source,
                errback=self.poster_source_error,
                # Wikipedia commonly credits an X post.  X blocks generic
                # crawlers in robots.txt even though the logged-out status
                # page publicly exposes the poster's Open Graph image.
                meta=request_meta,
                cb_kwargs={
                    "event_id": event_id,
                    "fallback_url": candidate["image_url"],
                    "metadata": metadata,
                },
                dont_filter=True,
            )
            return

        self._save_poster(
            event_id,
            candidate["image_url"],
            "wikipedia_file",
            metadata,
        )

    def parse_poster_source(
        self,
        response,
        event_id: int,
        fallback_url: str,
        metadata: dict,
    ):
        source_image_url = extract_source_image_url(response)
        self._save_poster(
            event_id,
            source_image_url or fallback_url,
            "wikipedia_source" if source_image_url else "wikipedia_file",
            metadata,
        )

    def poster_source_error(self, failure):
        request = failure.request
        self.logger.warning("Poster source request failed: %s", request.url)
        self._save_poster(
            request.cb_kwargs["event_id"],
            request.cb_kwargs["fallback_url"],
            "wikipedia_file",
            request.cb_kwargs["metadata"],
        )

    def _save_poster(
        self,
        event_id: int,
        image_url: str,
        image_source: str,
        metadata: dict,
    ):
        fields = {
            **metadata,
            "poster_image_url": image_url,
            "poster_image_source": image_source,
            "poster_image_updated_at": datetime.utcnow(),
        }
        self.db.events.update_one({"id": event_id}, {"$set": fields})
        self.logger.info("Updated card poster for event %s from %s", event_id, image_source)

    def parse_ufc_index(self, response):
        seen: set[str] = set()
        for href in response.xpath('//a[contains(@href, "/event/")]/@href').getall():
            url = response.urljoin(href)
            if url in seen or url in self.requested_official_urls:
                continue
            seen.add(url)
            yield scrapy.Request(
                url,
                callback=self.parse_ufc_event,
                errback=self.log_request_error,
            )

    def parse_ufc_event(self, response, event_id: int | None = None):
        hero_url = extract_ufc_hero_url(response)
        if not hero_url:
            self.logger.warning("No UFC XL 2x hero found: %s", response.url)
            return

        event = self.events.get(event_id) if event_id is not None else None
        if event is None:
            event = self._match_ufc_event(response)
        if event is None:
            return

        self.db.events.update_one(
            {"id": event["id"]},
            {
                "$set": {
                    "hero_image_url": hero_url,
                    "hero_image_source": "ufc_official_xl_2x",
                    "hero_image_updated_at": datetime.utcnow(),
                    "official_url": response.url,
                }
            },
        )
        self.logger.info("Updated UFC hero for event %s", event["id"])

    def _match_ufc_event(self, response) -> dict | None:
        event_date = extract_ufc_event_date(response)
        candidates = self.events_by_date.get(event_date or "", [])
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        page_name = _page_event_name(response)
        return max(
            candidates,
            key=lambda event: SequenceMatcher(
                None,
                " ".join(_normalized_words(event.get("name"))),
                " ".join(_normalized_words(page_name)),
            ).ratio(),
        )

    def log_request_error(self, failure):
        self.logger.warning("Image metadata request failed: %s", failure.request.url)

    def closed(self, reason):
        self.mongo_client.close()
