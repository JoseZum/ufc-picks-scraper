"""Pure ESPN -> UFC Picks transformation and matching helpers.

ESPN IDs are stored as source identifiers. Existing UFC Picks event/bout IDs
remain canonical so picks and URLs never break when ESPN is added to an
already-ingested Tapology card.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from difflib import SequenceMatcher
import re
import unicodedata
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


ESPN_FIGHTCENTER_URL = (
    "https://www.espn.com/mma/fightcenter/_/id/{event_id}/league/ufc"
)
ESPN_HEADSHOT_URL = (
    "https://a.espncdn.com/combiner/i?"
    "img=/i/headshots/mma/players/full/{athlete_id}.png&w=350&h=254"
)

_GENERIC_EVENT_WORDS = {
    "ufc",
    "fight",
    "night",
    "the",
    "vs",
    "and",
}


def ascii_text(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(
        character
        for character in normalized
        if not unicodedata.combining(character)
    )


def normalize_text(value: str | None) -> str:
    value = ascii_text(value).lower()
    value = re.sub(r"\bufc\s+fight\s+night\s+ufc\s+fight\s+night\b", "ufc fight night", value)
    return " ".join(re.findall(r"[a-z0-9]+", value))


def name_similarity(left: str | None, right: str | None) -> float:
    left_normalized = normalize_text(left)
    right_normalized = normalize_text(right)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0

    left_words = set(left_normalized.split())
    right_words = set(right_normalized.split())
    overlap = len(left_words & right_words) / max(len(left_words), len(right_words))
    sequence = SequenceMatcher(None, left_normalized, right_normalized).ratio()
    return (overlap * 0.45) + (sequence * 0.55)


def event_name_similarity(left: str | None, right: str | None) -> float:
    left_words = [
        word for word in normalize_text(left).split() if word not in _GENERIC_EVENT_WORDS
    ]
    right_words = [
        word for word in normalize_text(right).split() if word not in _GENERIC_EVENT_WORDS
    ]
    if left_words and right_words:
        distinctive = name_similarity(" ".join(left_words), " ".join(right_words))
    else:
        distinctive = 0.0
    return (name_similarity(left, right) * 0.45) + (distinctive * 0.55)


def parse_espn_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def mongo_utc_datetime(value: str | None) -> datetime | None:
    parsed = parse_espn_datetime(value)
    return parsed.replace(tzinfo=None) if parsed else None


def document_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        match = re.match(r"(\d{4}-\d{2}-\d{2})", value)
        if match:
            return date.fromisoformat(match.group(1))
    return None


def age_on_date(date_of_birth: date, target_date: date) -> int:
    return target_date.year - date_of_birth.year - (
        (target_date.month, target_date.day)
        < (date_of_birth.month, date_of_birth.day)
    )


def espn_event_date(event: dict) -> date | None:
    parsed = parse_espn_datetime(event.get("date"))
    return parsed.date() if parsed else None


def find_event_match(espn_event: dict, existing_events: list[dict]) -> dict | None:
    espn_id = str(espn_event.get("id") or "")
    for event in existing_events:
        if str(event.get("espn_event_id") or "") == espn_id:
            return event

    target_date = espn_event_date(espn_event)
    date_matches = [
        event
        for event in existing_events
        if document_date(event.get("date") or event.get("event_date")) == target_date
    ]
    if not date_matches:
        return None

    candidate = max(
        date_matches,
        key=lambda event: event_name_similarity(
            espn_event.get("name"),
            event.get("name"),
        ),
    )
    score = event_name_similarity(espn_event.get("name"), candidate.get("name"))
    return candidate if score >= 0.62 else None


def parse_record_summary(value: str | None) -> dict | None:
    if not value:
        return None
    numbers = [int(number) for number in re.findall(r"\d+", value)]
    if len(numbers) < 2:
        return None
    return {
        "wins": numbers[0],
        "losses": numbers[1],
        "draws": numbers[2] if len(numbers) > 2 else 0,
    }


def athlete_profile_url(athlete: dict) -> str | None:
    for link in athlete.get("links") or []:
        if "playercard" in (link.get("rel") or []) and link.get("href"):
            return link["href"]
    return None


def sorted_competitors(competition: dict) -> list[dict]:
    return sorted(
        competition.get("competitors") or [],
        key=lambda competitor: int(competitor.get("order") or 99),
    )


def competitor_snapshot(competitor: dict) -> dict:
    athlete = competitor.get("athlete") or {}
    overall_record = next(
        (
            record.get("summary")
            for record in competitor.get("records") or []
            if record.get("type") == "total" or record.get("name") == "overall"
        ),
        None,
    )
    nationality = (athlete.get("flag") or {}).get("alt")
    return {
        "fighter_name": athlete.get("displayName") or athlete.get("fullName") or "TBD",
        "espn_id": str(competitor.get("id") or athlete.get("id") or ""),
        "espn_url": athlete_profile_url(athlete),
        "nationality": nationality or "Unknown",
        "record_at_fight": parse_record_summary(overall_record)
        or {"wins": 0, "losses": 0, "draws": 0},
    }


def _bout_fighter_names(bout: dict) -> list[str]:
    fighters = bout.get("fighters") or {}
    return [
        (fighters.get("red") or {}).get("fighter_name", ""),
        (fighters.get("blue") or {}).get("fighter_name", ""),
    ]


def _competition_names(competition: dict) -> list[str]:
    return [
        (competitor.get("athlete") or {}).get("displayName", "")
        for competitor in sorted_competitors(competition)
    ]


def _unordered_match_score(left_names: list[str], right_names: list[str]) -> float:
    if len(left_names) != 2 or len(right_names) != 2:
        return 0.0
    direct = (
        name_similarity(left_names[0], right_names[0])
        + name_similarity(left_names[1], right_names[1])
    ) / 2
    swapped = (
        name_similarity(left_names[0], right_names[1])
        + name_similarity(left_names[1], right_names[0])
    ) / 2
    return max(direct, swapped)


def find_bout_match(competition: dict, existing_bouts: list[dict]) -> dict | None:
    competition_id = str(competition.get("id") or "")
    for bout in existing_bouts:
        if str(bout.get("espn_competition_id") or "") == competition_id:
            return bout

    competition_names = _competition_names(competition)
    if len(competition_names) != 2:
        return None
    candidate = max(
        existing_bouts,
        key=lambda bout: _unordered_match_score(
            competition_names,
            _bout_fighter_names(bout),
        ),
        default=None,
    )
    if not candidate:
        return None
    score = _unordered_match_score(competition_names, _bout_fighter_names(candidate))
    return candidate if score >= 0.78 else None


def map_competitors_to_corners(
    competition: dict,
    existing_bout: dict | None = None,
) -> dict[str, dict]:
    competitors = sorted_competitors(competition)
    if len(competitors) != 2:
        return {}

    if existing_bout:
        existing_fighters = existing_bout.get("fighters") or {}
        available_corners = {"red", "blue"}
        mapping: dict[str, dict] = {}
        for competitor in competitors:
            athlete_name = (competitor.get("athlete") or {}).get("displayName")
            scored_corners = sorted(
                (
                    (
                        name_similarity(
                            athlete_name,
                            (existing_fighters.get(corner) or {}).get("fighter_name"),
                        ),
                        corner,
                    )
                    for corner in available_corners
                ),
                reverse=True,
            )
            if scored_corners and scored_corners[0][0] >= 0.72:
                corner = scored_corners[0][1]
                mapping[corner] = competitor
                available_corners.remove(corner)
        if len(mapping) == 2:
            return mapping

    return {"red": competitors[0], "blue": competitors[1]}


def normalize_result_method(detail_texts: list[str]) -> tuple[str, str | None]:
    combined = " ".join(detail_texts).lower()
    if "kotko" in combined or "ko/tko" in combined or "knockout" in combined:
        return "KO/TKO", "KO/TKO"
    if "submission" in combined:
        return "SUB", "Submission"
    if "decision" in combined:
        return "DEC", "Decision"
    if "no contest" in combined:
        return "NC", "No Contest"
    if "draw" in combined:
        return "DEC", "Draw"
    return "OTHER", None


def transform_result(
    competition: dict,
    corner_mapping: dict[str, dict],
) -> dict | None:
    status = competition.get("status") or {}
    status_type = status.get("type") or {}
    if not status_type.get("completed"):
        return None

    detail_texts = [
        str((detail.get("type") or {}).get("text") or "")
        for detail in competition.get("details") or []
    ]
    combined = " ".join(detail_texts).lower()
    method, method_detail = normalize_result_method(detail_texts)

    winner_corner = None
    winner_name = None
    for corner, competitor in corner_mapping.items():
        if competitor.get("winner") is True:
            winner_corner = corner
            athlete = competitor.get("athlete") or {}
            winner_name = athlete.get("displayName") or athlete.get("fullName")
            break

    if winner_corner:
        outcome = winner_corner
    elif "no contest" in combined:
        outcome = "nc"
    else:
        outcome = "draw"

    period = status.get("period")
    return {
        "winner": winner_corner,
        "winner_name": winner_name,
        "outcome": outcome,
        "method": method,
        "method_detail": method_detail,
        "round": int(period) if period else None,
        "time": status.get("displayClock"),
        "source": "espn",
        "espn_competition_id": str(competition.get("id")),
    }


def event_status(espn_event: dict) -> str:
    status = (espn_event.get("status") or {}).get("type") or {}
    if status.get("completed") or status.get("state") == "post":
        return "completed"
    if status.get("name") in {"STATUS_CANCELED", "STATUS_CANCELLED"}:
        return "cancelled"
    return "scheduled"


def transform_event(espn_event: dict, internal_id: int) -> dict:
    event_datetime = parse_espn_datetime(espn_event.get("date"))
    et_datetime = event_datetime.astimezone(ZoneInfo("America/New_York")) if event_datetime else None
    competitions = espn_event.get("competitions") or []
    venue = next(
        (competition.get("venue") for competition in competitions if competition.get("venue")),
        None,
    ) or next(iter(espn_event.get("venues") or []), {})
    address = venue.get("address") or {}
    espn_id = str(espn_event.get("id"))
    now = datetime.utcnow()
    return {
        "_id": internal_id,
        "id": internal_id,
        "source": "espn",
        "promotion": "UFC",
        "name": espn_event.get("name") or f"UFC event {espn_id}",
        "subtitle": None,
        "slug": f"espn-{espn_id}",
        "url": ESPN_FIGHTCENTER_URL.format(event_id=espn_id),
        "espn_event_id": espn_id,
        "espn_url": ESPN_FIGHTCENTER_URL.format(event_id=espn_id),
        "date": (
            datetime.combine(event_datetime.date(), datetime.min.time())
            if event_datetime
            else None
        ),
        "start_time_et": et_datetime.strftime("%H:%M") if et_datetime else None,
        "timezone": "ET",
        "location": {
            "venue": venue.get("fullName"),
            "city": address.get("city"),
            "country": address.get("country"),
        },
        "status": event_status(espn_event),
        "total_bouts": len(competitions),
        "main_event_bout_id": None,
        "scraped_at": now,
        "last_updated": now,
        "espn_last_updated": now,
    }


def infer_card_position(index: int, total: int) -> dict:
    overall = total - index
    if overall <= 5:
        section = "main"
        section_order = overall
    elif overall <= 10:
        section = "prelim"
        section_order = overall - 5
    else:
        section = "early_prelim"
        section_order = overall - 10
    return {
        "card_section": section,
        "card_order": section_order,
        "order_overall": overall,
        "order_section": section_order,
        "is_main_event": overall == 1,
        "is_co_main_event": overall == 2,
        "is_co_main": overall == 2,
    }


def transform_competition_metadata(payload: dict) -> dict:
    card_segment = payload.get("cardSegment") or {}
    segment_name = str(card_segment.get("name") or "").lower()
    if segment_name == "main":
        card_section = "main"
    elif segment_name == "prelims2":
        card_section = "early_prelim"
    else:
        card_section = "prelim"

    match_number = int(payload.get("matchNumber") or 0)
    rounds = int(
        ((payload.get("format") or {}).get("regulation") or {}).get("periods")
        or 3
    )
    weight_class = (payload.get("type") or {}).get("text") or (
        payload.get("type") or {}
    ).get("abbreviation")
    return {
        "card_section": card_section,
        "card_order": match_number,
        "is_main_event": match_number == 1,
        "is_co_main_event": match_number == 2,
        "rounds_scheduled": rounds,
        "weight_class": weight_class,
        "gender": (
            "female"
            if str(weight_class or "").lower().startswith(("women", "w "))
            else "male"
        ),
        "espn_match_number": match_number,
        "espn_card_segment": segment_name or None,
    }


def transform_new_bout(
    competition: dict,
    event_id: int,
    index: int,
    total: int,
) -> dict:
    competition_id = int(competition["id"])
    position = infer_card_position(index, total)
    corner_mapping = map_competitors_to_corners(competition)
    fighters = {}
    for corner, competitor in corner_mapping.items():
        fighters[corner] = {
            **competitor_snapshot(competitor),
            "corner": corner,
            "last_fights": [],
            "age_at_fight_years": 0,
            "height_cm": None,
            "reach_cm": None,
        }

    rounds = int(
        ((competition.get("format") or {}).get("regulation") or {}).get("periods")
        or (5 if position["is_main_event"] else 3)
    )
    result = transform_result(competition, corner_mapping)
    return {
        "_id": competition_id,
        "id": competition_id,
        "event_id": event_id,
        "source": "espn",
        "url": None,
        "slug": f"espn-{competition_id}",
        "espn_competition_id": str(competition_id),
        "weight_class": (competition.get("type") or {}).get("abbreviation"),
        "gender": (
            "female"
            if str((competition.get("type") or {}).get("abbreviation", "")).startswith("W ")
            else "male"
        ),
        "rounds_scheduled": rounds,
        "is_title_fight": bool(competition.get("titleFight")),
        "status": "completed" if result else "scheduled",
        "fighters": fighters,
        "result": result,
        **position,
        "scraped_at": datetime.utcnow(),
        "last_updated": datetime.utcnow(),
    }


def build_headshot_url(athlete_id: str | int) -> str:
    return ESPN_HEADSHOT_URL.format(athlete_id=athlete_id)


def transform_athlete_profile(payload: dict) -> dict:
    athlete_id = str(payload.get("id") or "")
    height_inches = payload.get("height")
    reach_inches = payload.get("reach")
    weight_lbs = payload.get("weight")
    association = payload.get("association") or {}
    headshot = payload.get("headshot") or {}
    date_of_birth = parse_espn_datetime(payload.get("dateOfBirth"))
    height_feet = int(float(height_inches) // 12) if height_inches else None
    height_remainder = int(round(float(height_inches) % 12)) if height_inches else None
    return {
        "espn_id": athlete_id,
        "fighter_name": payload.get("displayName") or payload.get("fullName"),
        "espn_url": athlete_profile_url(payload),
        "nickname": payload.get("nickname"),
        "nationality": payload.get("citizenship") or (payload.get("flag") or {}).get("alt"),
        "age_at_fight_years": payload.get("age"),
        "date_of_birth": date_of_birth.date().isoformat() if date_of_birth else None,
        "height_cm": round(float(height_inches) * 2.54) if height_inches else None,
        "height": (
            {
                "feet": height_feet,
                "inches": height_remainder,
                "cm": round(float(height_inches) * 2.54),
            }
            if height_inches
            else None
        ),
        "reach_cm": round(float(reach_inches) * 2.54) if reach_inches else None,
        "reach": (
            {"inches": float(reach_inches), "cm": round(float(reach_inches) * 2.54)}
            if reach_inches
            else None
        ),
        "latest_weight": (
            {"lbs": float(weight_lbs), "kgs": round(float(weight_lbs) * 0.453592, 1)}
            if weight_lbs
            else None
        ),
        "stance": (payload.get("stance") or {}).get("text"),
        "weight_class": (payload.get("weightClass") or {}).get("text"),
        "gym": (
            {"primary": association.get("name"), "other": []}
            if association.get("name")
            else None
        ),
        "espn_headshot_url": headshot.get("href"),
        "espn_headshot_download_url": (
            build_headshot_url(athlete_id) if headshot.get("href") and athlete_id else None
        ),
    }


def transform_athlete_records(payload: dict) -> dict:
    overall = next(
        (
            item
            for item in payload.get("items") or []
            if item
            and (item.get("type") == "total" or item.get("name") == "overall")
        ),
        {},
    )
    values = {
        stat.get("name"): int(stat.get("value") or 0)
        for stat in overall.get("stats") or []
        if stat.get("name")
    }
    return {
        "current_record": {
            "wins": values.get("wins", 0),
            "losses": values.get("losses", 0),
            "draws": values.get("draws", 0),
            "no_contests": values.get("noContests", 0),
        },
        "career_stats": {
            "wins_by_ko_tko": values.get("tkos", 0),
            "losses_by_ko_tko": values.get("tkoLosses", 0),
            "wins_by_submission": values.get("submissions", 0),
            "losses_by_submission": values.get("submissionLosses", 0),
            "title_wins": values.get("titleWins", 0),
            "title_losses": values.get("titleLosses", 0),
        },
    }


def normalize_pick_method(value: str | None) -> str:
    value = str(value or "").upper()
    if value in {"KO", "TKO", "KO/TKO"}:
        return "KO/TKO"
    if value in {"SUB", "SUBMISSION"}:
        return "SUB"
    if value in {"DEC", "DECISION"}:
        return "DEC"
    return value


def calculate_pick_points(pick: dict, result: dict) -> tuple[int, bool]:
    winner_name = result.get("winner_name")
    if not winner_name or result.get("outcome") in {"draw", "nc"}:
        return 0, False

    is_correct = normalize_text(pick.get("picked_fighter_name")) == normalize_text(winner_name)
    if not is_correct:
        return 0, False

    points = 1
    if normalize_pick_method(pick.get("picked_method")) == normalize_pick_method(
        result.get("method")
    ):
        points += 1
        if pick.get("picked_round") and pick.get("picked_round") == result.get("round"):
            points += 1
    return points, True


def safe_external_http_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname)
