"""ESPN UFC ETL spider.

Modes:

* ``general``: cards + fighter profiles + records + S3 headshots + results.
* ``results``: only fill missing results on existing UFC Picks cards.
* ``photos``: link fighters and refresh missing ESPN headshots for upcoming cards.

Examples:
    scrapy crawl espn -a MODE=general
    scrapy crawl espn -a MODE=results -a DAYS_BACK=14
    scrapy crawl espn -a MODE=photos -a DAYS_AHEAD=60
    scrapy crawl espn -a MODE=general -a EVENT_ID=600059339
    scrapy crawl espn -a MODE=general -a SEASON=2026
    scrapy crawl espn -a MODE=general -a SEASON=2026 -a ONLY_COMPLETED=true
"""

from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone
import os
from urllib.parse import urlencode

import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
import scrapy

from tapology_scraper.canonical_card_writer import (
    CANONICAL_EVENT_FIELDS,
    CanonicalCardWriteError,
    MongoCanonicalCardStore,
    submit_card_observations,
)
from tapology_scraper.admin_command_replay import with_admin_overrides
from tapology_scraper.card_observation_sources import (
    ObservationSourceError,
    build_espn_card_observations,
)
from tapology_scraper.espn_etl import (
    age_on_date,
    calculate_pick_points,
    competitor_snapshot,
    document_date,
    event_status,
    find_bout_match,
    find_event_match,
    map_competitors_to_corners,
    safe_external_http_url,
    transform_athlete_profile,
    transform_athlete_records,
    transform_competition_metadata,
    transform_event,
)


ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard"
ESPN_ATHLETE_URL = (
    "https://sports.core.api.espn.com/v2/sports/mma/leagues/ufc/athletes/{athlete_id}"
)
ESPN_RECORDS_URL = (
    "https://sports.core.api.espn.com/v2/sports/mma/leagues/ufc/"
    "athletes/{athlete_id}/records"
)
ESPN_COMPETITION_URL = (
    "https://sports.core.api.espn.com/v2/sports/mma/leagues/ufc/"
    "events/{event_id}/competitions/{competition_id}"
)


def _source_id_values(value: str) -> list:
    values: list = [str(value)]
    if str(value).isdigit():
        values.append(int(value))
    return values


def _non_empty_fields(fields: dict) -> dict:
    return {
        key: value
        for key, value in fields.items()
        if value is not None and value != ""
    }


def has_fight_result(bout: dict) -> bool:
    """Treat only a non-empty result document as a finished fight."""
    return isinstance(bout.get("result"), dict) and bool(bout["result"])


def event_has_all_results(
    bouts: list[dict],
    expected_total: int | None = None,
) -> bool:
    """Decide completion from fight results, not ESPN's event-level flag."""
    active_bouts = [
        bout for bout in bouts if bout.get("status") != "cancelled"
    ]
    result_count = sum(has_fight_result(bout) for bout in active_bouts)
    if expected_total and expected_total > 0:
        return result_count >= expected_total
    return bool(active_bouts) and result_count == len(active_bouts)


def event_is_near(
    event: dict,
    reference_date: date | None = None,
    days_ahead: int = 7,
) -> bool:
    event_date = document_date(event.get("date") or event.get("event_date"))
    if not event_date:
        return False
    days_until = (event_date - (reference_date or date.today())).days
    return -1 <= days_until <= days_ahead


def select_ufc_events(
    payload: dict,
    only_completed: bool = False,
    only_scheduled: bool = False,
) -> list[dict]:
    """Select UFC cards while optionally excluding every unfinished event."""
    events = [
        event
        for event in payload.get("events") or []
        if "ufc" in str(event.get("name") or "").lower()
    ]
    if only_completed:
        events = [
            event
            for event in events
            if event_status(event) == "completed"
        ]
    if only_scheduled:
        events = [
            event
            for event in events
            if event_status(event) == "scheduled"
        ]
    return events


class EspnSpider(scrapy.Spider):
    name = "espn"
    allowed_domains = [
        "site.api.espn.com",
        "sports.core.api.espn.com",
        "a.espncdn.com",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.15,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "ITEM_PIPELINES": {
            "tapology_scraper.spiders.espn.EspnFighterImagePipeline": 300,
        },
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "UFC-Picks/1.0 ESPN ETL",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(
        self,
        MODE="general",
        EVENT_ID=None,
        SEASON=None,
        DAYS_BACK=None,
        DAYS_AHEAD=None,
        LIMIT=None,
        FORCE_PHOTOS=None,
        ONLY_COMPLETED=None,
        ONLY_SCHEDULED=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mode = str(MODE or "general").lower()
        if self.mode not in {"general", "results", "photos"}:
            raise ValueError("MODE must be general, results, or photos")

        mongo_uri = os.environ.get("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI is required")

        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        self.target_event_id = str(EVENT_ID) if EVENT_ID else None
        self.season = int(SEASON) if SEASON else None
        self.days_back = int(DAYS_BACK) if DAYS_BACK else (14 if self.mode != "photos" else 0)
        self.days_ahead = int(DAYS_AHEAD) if DAYS_AHEAD else (60 if self.mode != "results" else 1)
        self.limit = int(LIMIT) if LIMIT else None
        self.force_photos = str(FORCE_PHOTOS or "").lower() in {"1", "true", "yes"}
        self.only_completed = str(ONLY_COMPLETED or "").lower() in {
            "1",
            "true",
            "yes",
        }
        self.only_scheduled = str(ONLY_SCHEDULED or "").lower() in {
            "1",
            "true",
            "yes",
        }
        if self.only_completed and self.only_scheduled:
            raise ValueError(
                "ONLY_COMPLETED and ONLY_SCHEDULED cannot both be true"
            )
        self.requested_athletes: set[str] = set()
        self.processed_events = 0
        self.processed_bouts = 0
        self.results_loaded = 0
        self.profiles_loaded = 0
        # SCR-015: every CardData mutation this spider performs goes through
        # the canonical boundary.  The cached ESPN payload plus accumulated
        # competition detail is resubmitted as observations, so a later detail
        # response upgrades the same facts instead of overwriting them.
        self.card_data_dry_run = str(
            kwargs.get("CARD_DATA_DRY_RUN") or ""
        ).lower() in {"1", "true", "yes"}
        self.card_store = MongoCanonicalCardStore(self.db)
        self._espn_cards: dict[int, dict] = {}
        self.card_writes_applied = 0
        self.card_writes_blocked = 0

    async def start(self):
        reconciled = self._reconcile_fully_resulted_events()
        if reconciled:
            self.logger.info(
                "Immediately completed %s cards that already had every result",
                reconciled,
            )

        if self.mode == "results" and not self.target_event_id and not self.season:
            pending_dates = self._pending_result_dates()
            if not pending_dates:
                self.logger.info("No local UFC cards have pending results")
                return
            for pending_date in pending_dates:
                params = {"dates": pending_date.strftime("%Y%m%d"), "limit": 200}
                yield scrapy.Request(
                    f"{ESPN_SCOREBOARD_URL}?{urlencode(params)}",
                    callback=self.parse_scoreboard,
                    errback=self.log_request_error,
                    dont_filter=True,
                )
            return

        dates = self._date_query()
        params = {"dates": dates, "limit": 200}
        yield scrapy.Request(
            f"{ESPN_SCOREBOARD_URL}?{urlencode(params)}",
            callback=self.parse_scoreboard,
            errback=self.log_request_error,
            dont_filter=True,
        )

    def _pending_result_dates(self) -> list[date]:
        event_ids = self.db.bouts.distinct(
            "event_id",
            {
                "status": {"$ne": "cancelled"},
                "$or": [
                    {"result": {"$exists": False}},
                    {"result": None},
                    {"result": {}},
                ],
            },
        )
        if not event_ids:
            return []

        latest_date = date.today() + timedelta(days=1)
        pending_dates = {
            parsed_date
            for event in self.db.events.find(
                {
                    "id": {"$in": event_ids},
                    "name": {"$regex": r"\bUFC\b", "$options": "i"},
                },
                {"date": 1, "event_date": 1},
            )
            if (
                parsed_date := document_date(
                    event.get("date") or event.get("event_date")
                )
            )
            and parsed_date <= latest_date
        }
        return sorted(pending_dates)

    def _reconcile_fully_resulted_events(self) -> int:
        """Report past cards whose fights are done but event status lags.

        SCR-015 removed the direct lifecycle write this used to perform.
        ``events.status`` and ``bouts.status`` are canonical fields now, so a
        lagging card is repaired by refetching its ESPN payload — the observed
        status flows through the boundary with evidence — and never by
        inferring completion or cancellation from local document counts.
        """

        pending = 0
        latest_date = date.today() + timedelta(days=1)
        cursor = self.db.events.find(
            {
                "status": {"$ne": "completed"},
                "name": {"$regex": r"\bUFC\b", "$options": "i"},
            },
            {"id": 1, "date": 1, "event_date": 1, "total_bouts": 1},
        )
        for event in cursor:
            event_date = document_date(
                event.get("date") or event.get("event_date")
            )
            if not event_date or event_date > latest_date:
                continue

            event_id = int(event["id"])
            bouts = list(self.db.bouts.find({"event_id": event_id}))
            expected_total = int(event.get("total_bouts") or 0) or None
            if not event_has_all_results(bouts, expected_total):
                continue

            self.logger.info(
                "Event %s has every result but is not completed; a canonical "
                "ESPN observation is required to advance its lifecycle",
                event_id,
            )
            pending += 1
        return pending

    def _submit_card_observations(self, event_id: int) -> None:
        """Route the cached ESPN payload through the canonical boundary."""

        cached = self._espn_cards.get(event_id)
        if not cached or not cached.get("payload"):
            return
        state = self.card_store.load_card(event_id)
        try:
            batch = build_espn_card_observations(
                cached["payload"],
                state,
                observed_at=cached["observed_at"],
                competition_metadata=cached["details"],
            )
        except ObservationSourceError as error:
            self.card_writes_blocked += 1
            self.logger.error(
                "ESPN observations rejected for event %s: %s", event_id, error
            )
            return
        for finding in batch.findings:
            self.logger.log(
                40 if finding.severity in {"blocking", "error"} else 20,
                "CardData %s %s on event %s: %s %s",
                finding.severity,
                finding.code,
                event_id,
                finding.message,
                list(finding.example_ids),
            )
        if batch.blocked or not batch.observations:
            self.card_writes_blocked += int(batch.blocked)
            return
        # Standing Admin decisions are replayed on every pass. Without this an
        # `admin_override` only lasts until the next reconciliation, because the
        # normalizer rebuilds the snapshot from observations alone (D-DATA-010).
        observations, skipped = with_admin_overrides(
            batch.observations, state, getattr(self, "db", None)
        )
        for note in skipped:
            self.logger.warning(
                "Admin command skipped on event %s: %s", event_id, note
            )
        try:
            plan, receipt = submit_card_observations(
                self.card_store,
                event_id,
                observations,
                dry_run=self.card_data_dry_run,
            )
        except CanonicalCardWriteError as error:
            self.card_writes_blocked += 1
            self.logger.error(
                "Canonical card write failed for event %s: %s", event_id, error
            )
            return
        if plan.blocked:
            self.card_writes_blocked += 1
            self.logger.error(
                "Canonical card plan blocked for event %s: %s",
                event_id,
                [item.code for item in plan.findings],
            )
            return
        if receipt.applied:
            self.card_writes_applied += 1
            self._assign_points_for_new_results(plan)

    def _assign_points_for_new_results(self, plan) -> None:
        """Rescore picks for bouts whose canonical result actually changed."""

        for write in plan.bout_writes:
            if "result" not in write.changed_fields:
                continue
            result = write.values.get("result")
            if isinstance(result, dict) and result.get("winner_name"):
                self.results_loaded += 1
                self._assign_points(int(write.bout_id), result)

    def _attach_espn_aliases(self, event_id: int, competitions: list[dict]) -> None:
        """Link a local bout to its ESPN competition before any canonical write.

        Name matching establishes the *alias* only.  Every canonical fact is
        then resolved from that alias, so a fuzzy name never mutates CardData
        directly.
        """

        existing = list(self.db.bouts.find({"event_id": event_id}))
        linked = {
            str(bout.get("espn_competition_id"))
            for bout in existing
            if bout.get("espn_competition_id") is not None
        }
        unlinked = [
            bout for bout in existing if bout.get("espn_competition_id") is None
        ]
        for competition in competitions:
            competition_id = str(competition.get("id") or "")
            if not competition_id or competition_id in linked:
                continue
            match = find_bout_match(competition, unlinked)
            if match is None:
                continue
            self.db.bouts.update_one(
                {"id": match["id"]},
                {"$set": {"espn_competition_id": competition_id}},
            )
            linked.add(competition_id)
            unlinked = [bout for bout in unlinked if bout["id"] != match["id"]]

    def _date_query(self) -> str:
        if self.season:
            return str(self.season)

        if self.target_event_id:
            local_event = self.db.events.find_one(
                {
                    "$or": [
                        {"id": {"$in": _source_id_values(self.target_event_id)}},
                        {"_id": {"$in": _source_id_values(self.target_event_id)}},
                        {"espn_event_id": {"$in": _source_id_values(self.target_event_id)}},
                    ]
                },
                {"date": 1, "event_date": 1},
            )
            if local_event:
                target_date = document_date(
                    local_event.get("date") or local_event.get("event_date")
                )
                if target_date:
                    return target_date.strftime("%Y%m%d")
            # ESPN event IDs do not encode a date. Query the current season and
            # filter the response by ID when no local date mapping exists.
            if self.target_event_id.startswith("600"):
                return str(date.today().year)

        today = date.today()
        start = today - timedelta(days=self.days_back)
        end = today + timedelta(days=self.days_ahead)
        return f"{start:%Y%m%d}-{end:%Y%m%d}"

    def parse_scoreboard(self, response):
        payload = response.json()
        events = select_ufc_events(
            payload,
            self.only_completed,
            self.only_scheduled,
        )
        if self.target_event_id:
            events = [
                event
                for event in events
                if self._is_target_event(event)
            ]
        if self.limit:
            events = events[: self.limit]

        existing_events = list(
            self.db.events.find(
                {"name": {"$regex": r"\bUFC\b", "$options": "i"}},
            )
        )
        for espn_event in events:
            existing_event = find_event_match(espn_event, existing_events)
            if self.mode in {"results", "photos"} and not existing_event:
                self.logger.warning(
                    "Skipping ESPN event without local match in %s mode: %s",
                    self.mode,
                    espn_event.get("name"),
                )
                continue

            if self.mode == "general" and existing_event:
                self._remove_safe_espn_duplicate(espn_event, existing_event)

            internal_event = self._upsert_event(espn_event, existing_event)
            if not internal_event:
                continue

            self.processed_events += 1
            yield from self._process_card(espn_event, internal_event)

    def _remove_safe_espn_duplicate(
        self,
        espn_event: dict,
        canonical_event: dict,
    ):
        espn_id = str(espn_event.get("id") or "")
        if not espn_id.isdigit() or int(espn_id) == canonical_event.get("id"):
            return

        duplicate = self.db.events.find_one(
            {
                "id": int(espn_id),
                "source": "espn",
                "espn_event_id": espn_id,
            }
        )
        if not duplicate:
            return

        duplicate_bout_ids = self.db.bouts.distinct(
            "id",
            {"event_id": int(espn_id), "source": "espn"},
        )
        pick_query = {"event_id": int(espn_id)}
        if duplicate_bout_ids:
            pick_query = {
                "$or": [
                    pick_query,
                    {"bout_id": {"$in": duplicate_bout_ids}},
                ]
            }
        if self.db.picks.find_one(pick_query, {"_id": 1}):
            self.logger.error(
                "Refusing to remove duplicate ESPN event %s because it has picks",
                espn_id,
            )
            return

        self.db.bout_details.delete_many(
            {"bout_id": {"$in": duplicate_bout_ids}}
        )
        self.db.event_card_slots.delete_many({"event_id": int(espn_id)})
        self.db.bouts.delete_many(
            {"event_id": int(espn_id), "source": "espn"}
        )
        self.db.events.delete_one(
            {
                "id": int(espn_id),
                "source": "espn",
                "espn_event_id": espn_id,
            }
        )
        self.logger.warning(
            "Removed safe ESPN duplicate event %s in favor of canonical event %s",
            espn_id,
            canonical_event.get("id"),
        )

    def _is_target_event(self, espn_event: dict) -> bool:
        if str(espn_event.get("id")) == self.target_event_id:
            return True
        local_event = self.db.events.find_one(
            {
                "$or": [
                    {"id": {"$in": _source_id_values(self.target_event_id)}},
                    {"_id": {"$in": _source_id_values(self.target_event_id)}},
                ]
            }
        )
        if not local_event:
            return False
        return find_event_match(espn_event, [local_event]) is not None

    def _upsert_event(self, espn_event: dict, existing_event: dict | None) -> dict | None:
        if existing_event:
            internal_id = int(existing_event.get("id") or existing_event["_id"])
        elif self.mode == "general":
            internal_id = int(espn_event["id"])
        else:
            return None

        transformed = transform_event(espn_event, internal_id)
        if existing_event:
            protected_fields = {
                "_id",
                "id",
                "source",
                "slug",
                "url",
                "status",
                "poster_image_url",
                "poster_image_source",
                "poster_source_page_url",
                "wikipedia_article_url",
                "wikipedia_file_url",
                "wikipedia_image_url",
                "hero_image_url",
                "hero_image_source",
                "official_url",
                "event_art",
                "event_art_content_type",
            }
            if existing_event.get("timing_source") == "admin":
                protected_fields.update(
                    {
                        "date",
                        "start_time_et",
                        "card_start_time_utc",
                        "picks_lock_time_utc",
                        "section_start_times_utc",
                        "section_lock_times_utc",
                        "timing_source",
                    }
                )
            updates = {
                key: value
                for key, value in transformed.items()
                if key not in protected_fields
                and key not in CANONICAL_EVENT_FIELDS
                and value is not None
            }
            updates.update(
                {
                    "espn_event_id": str(espn_event["id"]),
                    "espn_url": transformed["espn_url"],
                    "espn_last_updated": datetime.utcnow(),
                }
            )
            self.db.events.update_one({"id": internal_id}, {"$set": updates})
        else:
            # Document creation only.  Canonical facts on the seed are
            # immediately reconciled by the boundary in this same pass.
            self.db.events.update_one(
                {"id": internal_id},
                {"$setOnInsert": transformed},
                upsert=True,
            )

        return self.db.events.find_one({"id": internal_id})

    def _process_card(self, espn_event: dict, internal_event: dict):
        """Route the observed ESPN card through the canonical boundary.

        SCR-015: this method no longer writes CardData.  It establishes source
        aliases, submits typed observations, and then performs only auxiliary
        enrichment and follow-up requests.  Bout/slot/result/lifecycle facts
        are resolved by the normalizer and persisted by the reconciler, so a
        later ESPN pass can never overwrite an Admin override or revive a
        terminal bout.
        """

        event_id = int(internal_event["id"])
        competitions = espn_event.get("competitions") or []
        self._attach_espn_aliases(event_id, competitions)

        cache = self._espn_cards.setdefault(
            event_id, {"payload": None, "details": {}, "observed_at": None}
        )
        cache["payload"] = copy.deepcopy(espn_event)
        cache["observed_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self._submit_card_observations(event_id)

        bouts_by_competition = {}
        bouts_by_id = {}
        for bout in self.db.bouts.find({"event_id": event_id}):
            bouts_by_id[int(bout["id"])] = bout
            if bout.get("espn_competition_id") is not None:
                bouts_by_competition[str(bout["espn_competition_id"])] = bout

        for competition in competitions:
            competition_id = str(competition.get("id") or "")
            bout = bouts_by_competition.get(competition_id)
            if bout is None and competition_id.isdigit():
                bout = bouts_by_id.get(int(competition_id))
            if bout is None:
                self.logger.warning(
                    "ESPN competition %s in event %s has no canonical bout",
                    competition_id,
                    event_id,
                )
                continue

            self.processed_bouts += 1
            corner_mapping = map_competitors_to_corners(competition, bout)
            self._link_competitors(bout, competition, corner_mapping)

            if self.mode in {"general", "photos"}:
                for competitor in corner_mapping.values():
                    athlete_id = str(competitor.get("id") or "")
                    if athlete_id and athlete_id not in self.requested_athletes:
                        self.requested_athletes.add(athlete_id)
                        yield self._athlete_request(athlete_id)

            if self.mode == "general":
                yield self._competition_request(
                    str(espn_event["id"]),
                    competition_id,
                    int(bout["id"]),
                )

    def _link_competitors(
        self,
        bout: dict,
        competition: dict,
        corner_mapping: dict[str, dict],
    ):
        set_fields = {
            "espn_competition_id": str(competition["id"]),
            "espn_last_updated": datetime.utcnow(),
        }
        is_pre_fight = not (
            ((competition.get("status") or {}).get("type") or {}).get("completed")
        )
        for corner, competitor in corner_mapping.items():
            snapshot = competitor_snapshot(competitor)
            # ``fighter_name`` is a canonical CardData fact resolved from the
            # bout observation, so this auxiliary enrichment only owns the
            # embedded profile fields around it.
            for field in ("espn_id", "espn_url", "nationality"):
                value = snapshot.get(field)
                if value and value != "Unknown":
                    set_fields[f"fighters.{corner}.{field}"] = value
            if is_pre_fight and snapshot.get("record_at_fight"):
                set_fields[f"fighters.{corner}.record_at_fight"] = snapshot[
                    "record_at_fight"
                ]

            self.db.fighters.update_one(
                {"_id": int(snapshot["espn_id"])},
                {
                    "$set": {
                        "espn_id": snapshot["espn_id"],
                        "fighter_name": snapshot["fighter_name"],
                        "espn_url": snapshot.get("espn_url"),
                        "nationality": snapshot.get("nationality"),
                        "current_record": snapshot.get("record_at_fight"),
                        "last_updated": datetime.utcnow(),
                    },
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                },
                upsert=True,
            )

        self.db.bouts.update_one({"id": bout["id"]}, {"$set": set_fields})
        detail_fields = {
            key: value
            for key, value in set_fields.items()
            if key.startswith("fighters.")
        }
        if detail_fields:
            self.db.bout_details.update_one(
                {"bout_id": bout["id"]},
                {
                    "$set": {
                        **detail_fields,
                        "event_id": bout["event_id"],
                        "espn_competition_id": str(competition["id"]),
                        "last_updated": datetime.utcnow(),
                    },
                    "$setOnInsert": {"_id": bout["id"]},
                },
                upsert=True,
            )

    def _assign_points(self, bout_id: int, result: dict):
        users_affected = set()
        for pick in self.db.picks.find({"bout_id": bout_id}):
            points, is_correct = calculate_pick_points(pick, result)
            self.db.picks.update_one(
                {"_id": pick["_id"]},
                {
                    "$set": {
                        "points_awarded": points,
                        "is_correct": is_correct,
                    }
                },
            )
            if pick.get("user_id") is not None:
                users_affected.add(pick["user_id"])

        for user_id in users_affected:
            picks = list(self.db.picks.find({"user_id": user_id}))
            evaluated = [pick for pick in picks if pick.get("is_correct") is not None]
            correct = sum(pick.get("is_correct") is True for pick in evaluated)
            self.db.users.update_one(
                {"_id": user_id},
                {
                    "$set": {
                        "total_points": sum(
                            int(pick.get("points_awarded") or 0) for pick in picks
                        ),
                        "picks_total": len(picks),
                        "picks_correct": correct,
                        "perfect_picks": sum(
                            int(pick.get("points_awarded") or 0) == 3 for pick in picks
                        ),
                        "accuracy": round(correct / len(evaluated), 4)
                        if evaluated
                        else 0.0,
                    }
                },
            )

    # SCR-015 removed ``_ensure_card_slot``.  ``event_card_slots`` is owned
    # exclusively by ``plan_slot_reconciliation`` through the canonical
    # boundary, which converges after reorders, replacements and removals
    # instead of inserting a slot once and never revisiting it.

    def _athlete_request(self, athlete_id: str) -> scrapy.Request:
        params = {"lang": "en", "region": "us"}
        return scrapy.Request(
            f"{ESPN_ATHLETE_URL.format(athlete_id=athlete_id)}?{urlencode(params)}",
            callback=self.parse_athlete,
            errback=self.log_request_error,
            cb_kwargs={"athlete_id": athlete_id},
        )

    def _competition_request(
        self,
        event_id: str,
        competition_id: str,
        bout_id: int,
    ) -> scrapy.Request:
        params = {"lang": "en", "region": "us"}
        url = ESPN_COMPETITION_URL.format(
            event_id=event_id,
            competition_id=competition_id,
        )
        return scrapy.Request(
            f"{url}?{urlencode(params)}",
            callback=self.parse_competition_metadata,
            errback=self.log_request_error,
            cb_kwargs={"bout_id": bout_id},
        )

    def parse_competition_metadata(self, response, bout_id: int):
        """Feed explicit ESPN detail into the canonical boundary.

        The detail payload used to overwrite slot placement directly, which
        also wrote a global match number into the section-local order.  It now
        becomes an ``espn_detail`` observation: it outranks scoreboard
        inference for section and timing, while both order fields are derived
        once, after final section grouping.
        """

        metadata = _non_empty_fields(
            transform_competition_metadata(response.json())
        )
        bout = self.db.bouts.find_one(
            {"id": bout_id},
            {"event_id": 1, "espn_competition_id": 1},
        ) or {}
        event_id = bout.get("event_id")
        competition_id = str(bout.get("espn_competition_id") or "")

        # Auxiliary ESPN bookkeeping only; every canonical fact below travels
        # as an observation instead.
        self.db.bouts.update_one(
            {"id": bout_id},
            {
                "$set": {
                    "espn_match_number": metadata.get("espn_match_number"),
                    "espn_card_segment": metadata.get("espn_card_segment"),
                    "espn_last_updated": datetime.utcnow(),
                }
            },
        )

        cache = self._espn_cards.get(event_id)
        if not cache or not competition_id:
            return
        cache["details"][competition_id] = {
            key: value
            for key, value in (
                ("card_section", metadata.get("card_section")),
                ("rounds_scheduled", metadata.get("rounds_scheduled")),
                ("weight_class", metadata.get("weight_class")),
                (
                    "automatic_lock_time_utc",
                    metadata.get("automatic_lock_time_utc"),
                ),
            )
            if value is not None
        }
        self._submit_card_observations(int(event_id))

    def parse_athlete(self, response, athlete_id: str):
        profile = transform_athlete_profile(response.json())
        if not profile.get("espn_id"):
            return

        self._save_athlete_profile(profile)
        self.profiles_loaded += 1

        if self.mode == "general":
            params = {"lang": "en", "region": "us"}
            yield scrapy.Request(
                f"{ESPN_RECORDS_URL.format(athlete_id=athlete_id)}?{urlencode(params)}",
                callback=self.parse_athlete_records,
                errback=self.log_request_error,
                cb_kwargs={"athlete_id": athlete_id},
            )

        image_url = profile.get("espn_headshot_download_url")
        existing = self.db.fighters.find_one({"_id": int(athlete_id)}) or {}
        already_has_espn_photo = (
            existing.get("image_source") == "espn" and existing.get("image_key")
        )
        if (
            image_url
            and safe_external_http_url(image_url)
            and (self.force_photos or not already_has_espn_photo)
        ):
            yield {
                "type": "espn_fighter_image",
                "espn_id": athlete_id,
                "fighter_name": profile.get("fighter_name"),
                "image_url": image_url,
                "source_url": profile.get("espn_headshot_url"),
            }

    def _save_athlete_profile(self, profile: dict):
        athlete_id = profile["espn_id"]
        clean_profile = _non_empty_fields(profile)
        clean_profile["last_updated"] = datetime.utcnow()
        self.db.fighters.update_one(
            {"_id": int(athlete_id)},
            {
                "$set": clean_profile,
                "$setOnInsert": {"created_at": datetime.utcnow()},
            },
            upsert=True,
        )

        static_fields = {
            key: value
            for key, value in clean_profile.items()
            if key
            in {
                "espn_id",
                "espn_url",
                "nickname",
                "nationality",
                "date_of_birth",
                "height_cm",
                "height",
                "reach_cm",
                "reach",
                "latest_weight",
                "stance",
                "weight_class",
                "gym",
                "espn_headshot_url",
            }
        }
        self._update_embedded_fighter_fields(
            athlete_id,
            static_fields,
            {},
        )
        self._update_scheduled_fighter_ages(
            athlete_id,
            clean_profile.get("date_of_birth"),
            clean_profile.get("age_at_fight_years"),
        )

    def parse_athlete_records(self, response, athlete_id: str):
        record_fields = transform_athlete_records(response.json())
        self.db.fighters.update_one(
            {"_id": int(athlete_id)},
            {
                "$set": {
                    **record_fields,
                    "records_updated_at": datetime.utcnow(),
                }
            },
        )
        self._update_embedded_fighter_fields(
            athlete_id,
            {},
            {"career_stats": record_fields.get("career_stats")},
        )

    def _update_embedded_fighter_fields(
        self,
        athlete_id: str,
        static_fields: dict,
        scheduled_fields: dict,
    ):
        source_values = _source_id_values(athlete_id)
        for collection_name in ("bouts", "bout_details"):
            collection = self.db[collection_name]
            for corner in ("red", "blue"):
                lookup = {f"fighters.{corner}.espn_id": {"$in": source_values}}
                if static_fields:
                    collection.update_many(
                        lookup,
                        {
                            "$set": {
                                f"fighters.{corner}.{key}": value
                                for key, value in static_fields.items()
                            }
                        },
                    )
                if scheduled_fields and collection_name == "bouts":
                    scheduled_lookup = dict(lookup)
                    scheduled_lookup["status"] = "scheduled"
                    collection.update_many(
                        scheduled_lookup,
                        {
                            "$set": {
                                f"fighters.{corner}.{key}": value
                                for key, value in scheduled_fields.items()
                                if value is not None
                            }
                        },
                    )

    def _update_scheduled_fighter_ages(
        self,
        athlete_id: str,
        date_of_birth_value: str | None,
        fallback_age: int | None,
    ):
        date_of_birth = document_date(date_of_birth_value)
        source_values = _source_id_values(athlete_id)
        bouts = list(
            self.db.bouts.find(
                {
                    "status": "scheduled",
                    "$or": [
                        {"fighters.red.espn_id": {"$in": source_values}},
                        {"fighters.blue.espn_id": {"$in": source_values}},
                    ],
                },
                {"id": 1, "event_id": 1, "fighters": 1},
            )
        )
        event_ids = {bout.get("event_id") for bout in bouts}
        events = {
            event["id"]: event
            for event in self.db.events.find(
                {"id": {"$in": list(event_ids)}},
                {"id": 1, "date": 1, "event_date": 1},
            )
        }
        for bout in bouts:
            event = events.get(bout.get("event_id")) or {}
            event_date = document_date(event.get("date") or event.get("event_date"))
            age = (
                age_on_date(date_of_birth, event_date)
                if date_of_birth and event_date
                else fallback_age
            )
            if age is None:
                continue
            for corner in ("red", "blue"):
                fighter = (bout.get("fighters") or {}).get(corner) or {}
                if str(fighter.get("espn_id")) != athlete_id:
                    continue
                field = f"fighters.{corner}.age_at_fight_years"
                self.db.bouts.update_one({"id": bout["id"]}, {"$set": {field: age}})
                self.db.bout_details.update_one(
                    {"bout_id": bout["id"]},
                    {"$set": {field: age}},
                )

    def log_request_error(self, failure):
        self.logger.warning("ESPN request failed: %s", failure.request.url)

    def closed(self, reason):
        self.logger.info(
            "ESPN ETL finished: mode=%s events=%s bouts=%s results=%s profiles=%s",
            self.mode,
            self.processed_events,
            self.processed_bouts,
            self.results_loaded,
            self.profiles_loaded,
        )
        self.mongo_client.close()


class EspnFighterImagePipeline:
    """Link ESPN headshots immediately and mirror them to S3 when available."""

    def __init__(self):
        mongo_uri = os.environ.get("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI is required")
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks
        self._s3_service = None
        self.crawler = None
        self.s3_mirror_disabled = False
        self.direct_images_linked = 0
        self.s3_images_uploaded = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    def open_spider(self):
        if self.crawler.spider.mode in {"general", "photos"}:
            try:
                self._get_s3_service()
            except Exception as error:
                self.s3_mirror_disabled = True
                self.crawler.spider.logger.warning(
                    "S3 headshot mirror unavailable; using ESPN CDN directly: %s",
                    error,
                )

    def _get_s3_service(self):
        if self._s3_service is None:
            missing = [
                variable
                for variable in (
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_S3_BUCKET",
                )
                if not os.environ.get(variable)
            ]
            if missing:
                raise RuntimeError(
                    f"Missing S3 variables for ESPN photos: {', '.join(missing)}"
                )
            from tapology_scraper.s3_service import get_s3_service

            self._s3_service = get_s3_service()
            if self._s3_service.is_read_only:
                raise RuntimeError("IMAGE_SOURCE_MODE must be s3 for ESPN photos")
        return self._s3_service

    async def process_item(self, item):
        if item.get("type") != "espn_fighter_image":
            return item

        spider = self.crawler.spider
        athlete_id = str(item["espn_id"])
        image_url = item["image_url"]
        direct_url = item.get("source_url") or image_url
        direct_fields = {
            "image_source": "espn_direct",
            "espn_headshot_url": direct_url,
            "image_updated_at": datetime.utcnow(),
        }
        await self._persist_image_fields(athlete_id, direct_fields)
        self.direct_images_linked += 1

        if self.s3_mirror_disabled:
            return item

        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(
                    image_url,
                    headers={
                        "User-Agent": "UFC-Picks/1.0 ESPN image mirror",
                        "Referer": "https://www.espn.com/",
                    },
                )
                response.raise_for_status()

            content_type = response.headers.get("content-type", "image/png").split(";")[0]
            extension = "png" if "png" in content_type else "jpg"
            s3_service = self._get_s3_service()
            s3_key = s3_service.generate_fighter_image_key(
                f"espn-{athlete_id}",
                extension,
            )
            await s3_service.upload_image(
                s3_key=s3_key,
                image_data=response.content,
                content_type=content_type,
                metadata={
                    "espn_id": athlete_id,
                    "fighter_name": item.get("fighter_name") or "",
                    "source": "espn",
                    "source_url": item.get("source_url") or image_url,
                },
            )

            update_fields = {
                "image_key": s3_key,
                "image_source": "espn",
                "espn_headshot_url": direct_url,
                "image_updated_at": datetime.utcnow(),
            }
            await self._persist_image_fields(athlete_id, update_fields)
            self.s3_images_uploaded += 1
            spider.logger.info(
                "Uploaded ESPN headshot for %s to %s",
                athlete_id,
                s3_key,
            )
        except Exception as error:
            self.s3_mirror_disabled = True
            spider.logger.warning(
                "S3 headshot mirror disabled after upload error; "
                "ESPN CDN fallback remains active for %s: %s",
                athlete_id,
                error,
            )
        return item

    async def _persist_image_fields(self, athlete_id: str, update_fields: dict):
        await self.db.fighters.update_one(
            {"_id": int(athlete_id)},
            {"$set": update_fields},
            upsert=True,
        )
        source_values = _source_id_values(athlete_id)
        for collection_name in ("bouts", "bout_details"):
            collection = self.db[collection_name]
            for corner in ("red", "blue"):
                await collection.update_many(
                    {f"fighters.{corner}.espn_id": {"$in": source_values}},
                    {
                        "$set": {
                            f"fighters.{corner}.{key}": value
                            for key, value in update_fields.items()
                        }
                    },
                )

    def close_spider(self):
        self.mongo_client.close()
        self.crawler.spider.logger.info(
            "ESPN headshots linked=%s mirrored_to_s3=%s direct_fallback=%s",
            self.direct_images_linked,
            self.s3_images_uploaded,
            self.s3_mirror_disabled,
        )
