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

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from html import unescape
import os
import re
import unicodedata
from urllib.parse import urlencode, urlparse

from parsel import Selector
from pymongo import MongoClient
import requests
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
_WIKIPEDIA_POSTER_SOURCES = {"wikipedia_source", "wikipedia_file"}
_DISPLAYABLE_POSTER_SOURCES = {
    *_WIKIPEDIA_POSTER_SOURCES,
    "ufc_official_fallback",
}
_EXPIRING_IMAGE_HOST_SUFFIXES = (
    "cdninstagram.com",
    "fbcdn.net",
)
_IMAGE_WINDOW_DAYS_BACK = 14
_IMAGE_WINDOW_DAYS_AHEAD = 75
_MONTH_NUMBERS = {
    month.lower(): index
    for index, month in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
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


def poster_is_displayable(event: dict) -> bool:
    url = event.get("poster_image_url")
    return (
        event.get("poster_image_source") in _DISPLAYABLE_POSTER_SOURCES
        and isinstance(url, str)
        and url.startswith(("https://", "http://"))
    )


def poster_needs_wikipedia_refresh(event: dict) -> bool:
    """Refresh missing/fallback posters and signed social-CDN source URLs."""
    if event.get("poster_image_source") not in _WIKIPEDIA_POSTER_SOURCES:
        return True
    host = (
        urlparse(str(event.get("poster_image_url") or "")).hostname or ""
    ).lower()
    return any(
        host == suffix or host.endswith(f".{suffix}")
        for suffix in _EXPIRING_IMAGE_HOST_SUFFIXES
    )


def image_response_is_valid(status_code: int, content_type: str | None) -> bool:
    return (
        200 <= status_code < 300
        and str(content_type or "").lower().startswith("image/")
    )


def image_url_is_live(url: str) -> bool:
    """Check the actual image response without downloading the whole asset."""
    try:
        with requests.get(
            url,
            allow_redirects=True,
            stream=True,
            timeout=15,
            headers={
                "Range": "bytes=0-1023",
                "User-Agent": "UFC-Picks/1.0 event image validator",
            },
        ) as response:
            return image_response_is_valid(
                response.status_code,
                response.headers.get("content-type"),
            )
    except requests.RequestException:
        return False


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
    if date_time:
        match = re.match(r"(\d{4}-\d{2}-\d{2})", date_time)
        if match:
            return match.group(1)

    # Newer UFC pages dropped the machine-readable <time> element but retain
    # the date in either the canonical slug or meta description.
    slug_match = re.search(
        r"/(?:event/)?ufc-fight-night-([a-z]+)-(\d{1,2})-(\d{4})",
        urlparse(response.url).path.lower(),
    )
    if slug_match and slug_match.group(1) in _MONTH_NUMBERS:
        return date(
            int(slug_match.group(3)),
            _MONTH_NUMBERS[slug_match.group(1)],
            int(slug_match.group(2)),
        ).isoformat()

    description = " ".join(
        response.css(
            'meta[name="description"]::attr(content), '
            'meta[property="og:description"]::attr(content)'
        ).getall()
    )
    description_match = re.search(
        r"\b(" + "|".join(_MONTH_NUMBERS) + r")\s+(\d{1,2}),\s+(\d{4})\b",
        description,
        flags=re.IGNORECASE,
    )
    if description_match:
        return date(
            int(description_match.group(3)),
            _MONTH_NUMBERS[description_match.group(1).lower()],
            int(description_match.group(2)),
        ).isoformat()
    return None


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


def event_is_in_image_window(
    event: dict,
    today: date | None = None,
    days_back: int = _IMAGE_WINDOW_DAYS_BACK,
    days_ahead: int = _IMAGE_WINDOW_DAYS_AHEAD,
) -> bool:
    """Limit scheduled refreshes to cards the UI can currently surface."""
    date_string = _event_date_string(event)
    if not date_string:
        return False
    event_date = date.fromisoformat(date_string)
    current_date = today or date.today()
    return (
        current_date - timedelta(days=days_back)
        <= event_date
        <= current_date + timedelta(days=days_ahead)
    )


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
        "DOWNLOAD_DELAY": 0.75,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {},
        # MediaWiki's JSON API is the intended programmatic interface. The
        # robots endpoint intermittently returns 403/429 to GitHub runners and
        # was causing Scrapy to discard otherwise valid API requests.
        "ROBOTSTXT_OBEY": False,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "UFC-Picks/1.0 event image resolver",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(self, EVENT_ID=None, FORCE=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mongo_uri = os.environ.get("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI is required")

        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        self.target_event_id = int(EVENT_ID) if EVENT_ID else None
        self.force = str(FORCE or "").lower() in {"1", "true", "yes"}
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
            "status": 1,
            "poster_image_url": 1,
            "poster_image_source": 1,
            "hero_image_url": 1,
            "hero_image_source": 1,
        }
        for event in self.db.events.find(query, projection):
            if self.target_event_id is None and not event_is_in_image_window(event):
                continue

            event_id = int(event.get("id") or event["_id"])
            event["id"] = event_id
            self.events[event_id] = event
            date_string = _event_date_string(event)
            if date_string:
                self.events_by_date.setdefault(date_string, []).append(event)

            if self.force or poster_needs_wikipedia_refresh(event):
                yield self._wikipedia_article_request(event)

            official_url = event.get("official_url")
            if not official_url and "ufc.com/event/" in str(event.get("url", "")):
                official_url = event["url"]
            if official_url and (
                self.force
                or event.get("hero_image_source") != "ufc_official_xl_2x"
                or not event.get("hero_image_url")
            ):
                official_url = official_url.replace("www.ufc.com", "www.ufcespanol.com")
                self.requested_official_urls.add(official_url)
                yield scrapy.Request(
                    official_url,
                    callback=self.parse_ufc_event,
                    errback=self.log_request_error,
                    cb_kwargs={"event_id": event_id},
                    dont_filter=True,
                )
            elif (
                event.get("hero_image_url")
                and event.get("hero_image_source") == "ufc_official_xl_2x"
                and not poster_is_displayable(event)
            ):
                self._save_official_poster_fallback(event_id, event["hero_image_url"])

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
        self.events[event_id].update(fields)
        self.logger.info("Updated card poster for event %s from %s", event_id, image_source)

    def _save_official_poster_fallback(self, event_id: int, hero_url: str):
        """Keep cards visible until Wikipedia publishes a usable event poster."""
        event = self.events[event_id]
        if event.get("poster_image_source") in _WIKIPEDIA_POSTER_SOURCES:
            return

        fields = {
            "poster_image_url": hero_url,
            "poster_image_source": "ufc_official_fallback",
            "poster_image_updated_at": datetime.utcnow(),
        }
        self.db.events.update_one({"id": event_id}, {"$set": fields})
        event.update(fields)
        self.logger.info(
            "Using official UFC hero as temporary card poster for event %s",
            event_id,
        )

    def parse_ufc_index(self, response):
        seen: set[str] = set()
        ufc_host = (urlparse(response.url).hostname or "").lower()
        for href in response.xpath('//a[contains(@href, "/event/")]/@href').getall():
            url = response.urljoin(href)
            if (urlparse(url).hostname or "").lower() != ufc_host:
                continue
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

        fields = {
            "hero_image_url": hero_url,
            "hero_image_source": "ufc_official_xl_2x",
            "hero_image_updated_at": datetime.utcnow(),
            "official_url": response.url,
        }
        self.db.events.update_one(
            {"id": event["id"]},
            {"$set": fields},
        )
        event.update(fields)
        self._save_official_poster_fallback(event["id"], hero_url)
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
        missing_posters: list[int] = []
        missing_heroes: list[int] = []
        event_images: dict[int, tuple[str | None, str | None]] = {}
        for event_id in self.events:
            event = self.db.events.find_one(
                {"id": event_id},
                {
                    "poster_image_url": 1,
                    "poster_image_source": 1,
                    "hero_image_url": 1,
                    "hero_image_source": 1,
                },
            ) or {}
            if not poster_is_displayable(event):
                missing_posters.append(event_id)
            if (
                not event.get("hero_image_url")
                or event.get("hero_image_source") != "ufc_official_xl_2x"
            ):
                missing_heroes.append(event_id)
            event_images[event_id] = (
                event.get("poster_image_url"),
                event.get("hero_image_url"),
            )

        urls = {
            url
            for images in event_images.values()
            for url in images
            if isinstance(url, str) and url.startswith(("https://", "http://"))
        }
        with ThreadPoolExecutor(max_workers=6) as executor:
            live_urls = dict(zip(urls, executor.map(image_url_is_live, urls)))
        broken_posters = [
            event_id
            for event_id, (poster_url, _) in event_images.items()
            if poster_url and not live_urls.get(poster_url, False)
        ]
        broken_heroes = [
            event_id
            for event_id, (_, hero_url) in event_images.items()
            if hero_url and not live_urls.get(hero_url, False)
        ]

        if missing_posters or missing_heroes or broken_posters or broken_heroes:
            self.logger.error(
                "EVENT_IMAGE_COVERAGE_FAILED missing_posters=%s "
                "missing_heroes=%s broken_posters=%s broken_heroes=%s",
                missing_posters,
                missing_heroes,
                broken_posters,
                broken_heroes,
            )
        else:
            self.logger.info(
                "Event image coverage complete for %s cards",
                len(self.events),
            )
        self.mongo_client.close()
