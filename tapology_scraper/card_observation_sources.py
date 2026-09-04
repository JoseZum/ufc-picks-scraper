"""SCR-015/016 source adapters for the continuous CardData boundary.

Two ongoing sources feed :mod:`tapology_scraper.canonical_card_writer`:

* the continuous ESPN scoreboard/competition path, and
* Admin date/timing/lifecycle/title/result commands.

Both are converted here into typed :class:`CardDataObservation` proposals.  No
adapter writes a document, opens a connection or decides precedence: the
normalizer resolves ``Admin override > explicit ESPN metadata > scraper
inference > quarantined fallback`` from source rank and persisted evidence.

Three invariants are enforced at this layer because they are identity
decisions, not field precedence:

* an event is matched only by a trusted ESPN alias, never by date alone
  (WP-001);
* a bout is matched only by its source competition ID or canonical ID, never
  by fuzzy fighter names (WP-013);
* ESPN/Tapology TITLE signals are emitted as advisory suggestions only, so
  Admin remains the sole authority for ``is_title_fight``,
  ``is_bmf_title_fight`` and ``title_type`` in both the ``true`` and ``false``
  direction (WP-007).
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from tapology_scraper.canonical_card_writer import CanonicalCardState
from tapology_scraper.espn_etl import (
    infer_competition_sections,
    map_competitors_to_corners,
    name_similarity,
    normalize_weight_class,
    parse_espn_datetime,
    sorted_competitors,
)


OBSERVATION_SOURCE_VERSION = "continuous-card-observations/v1"
EVENT_TIMEZONE = "America/New_York"
TERMINAL_BOUT_STATUSES = {"cancelled", "replaced"}
CARD_SECTIONS = ("early_prelim", "prelim", "main")
FIGHTER_IDENTITY_THRESHOLD = 0.86

_METHOD_FAMILIES = {
    "ko_tko": ("ko", "tko", "knockout"),
    "submission": ("submission", "sub"),
    "decision": ("decision", "dec"),
    "dq": ("dq", "disqualification", "disqualified"),
}
_CANONICAL_OUTCOMES = {
    "red": "red_win",
    "blue": "blue_win",
    "red_win": "red_win",
    "blue_win": "blue_win",
    "draw": "draw",
    "nc": "no_contest",
    "no_contest": "no_contest",
}
ADMIN_COMMAND_KINDS = {
    "event_timing",
    "event_lifecycle",
    "bout_timing",
    "bout_structure",
    "bout_lifecycle",
    "title",
    "result",
    "clear_result",
}


class ObservationSourceError(ValueError):
    """Raised when a source payload cannot produce typed observations."""


@dataclass(frozen=True)
class SourceFinding:
    code: str
    severity: str
    scope: str
    example_ids: tuple[str, ...]
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "scope": self.scope,
            "example_ids": list(self.example_ids),
            "message": self.message,
        }


@dataclass(frozen=True)
class ObservationBatch:
    """Typed observations plus the operator-facing reasons for omissions."""

    source: str
    event_id: int
    observations: tuple[Mapping[str, Any], ...] = ()
    findings: tuple[SourceFinding, ...] = ()
    #: Canonical bouts this payload actually contained. The absence policy needs
    #: presence, not just the observations, because a bout that vanished is
    #: precisely the one with nothing to observe.
    present_bout_ids: tuple[int, ...] = ()

    @property
    def blocked(self) -> bool:
        return any(item.severity in {"blocking", "error"} for item in self.findings)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "event_id": self.event_id,
            "observation_count": len(self.observations),
            "blocked": self.blocked,
            "findings": [item.as_dict() for item in self.findings],
        }


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _finding(
    code: str,
    severity: str,
    scope: str,
    ids: Sequence[Any],
    message: str,
) -> SourceFinding:
    return SourceFinding(
        code=code,
        severity=severity,
        scope=scope,
        example_ids=tuple(sorted({str(item) for item in ids if item is not None})[:5]),
        message=message,
    )


def utc_string(value: Any) -> Optional[str]:
    """Return a canonical ``YYYY-MM-DDTHH:MM:SSZ`` string or ``None``."""

    if isinstance(value, datetime):
        parsed = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
    if not _nonempty(value):
        return None
    parsed = parse_espn_datetime(value)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") if parsed else None


def _observation(
    *,
    event_id: int,
    entity_type: str,
    entity_id: int,
    values: Mapping[str, Any],
    source_kind: str,
    source_event_id: str,
    observed_at: str,
    prefix: str,
    suffix: str,
    identity_basis: str = "canonical_id",
    reason: Optional[str] = None,
    clear_fields: Sequence[str] = (),
) -> dict[str, Any]:
    payload = {
        "event_id": event_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "values": copy.deepcopy(dict(values)),
        "clear_fields": list(clear_fields),
        "source_kind": source_kind,
        "source_event_id": source_event_id,
        "observed_at": observed_at,
    }
    return {
        "observation_id": f"{prefix}:{event_id}:{entity_type}:{entity_id}:{suffix}",
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": event_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"{prefix}:{entity_type}:{entity_id}:{suffix}",
        "source_event_id": source_event_id,
        "values": copy.deepcopy(dict(values)),
        "clear_fields": list(clear_fields),
        "identity_basis": identity_basis,
        "reason": reason,
        "payload_hash": _hash(payload),
    }


def _canonical_bout_sidecar(document: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(document.get("card_data_v1"))


def _competition_athlete_aliases(competition: Mapping[str, Any]) -> frozenset[str]:
    aliases = set()
    for competitor in sorted_competitors(dict(competition)):
        athlete = _mapping(competitor.get("athlete"))
        athlete_id = competitor.get("id") or athlete.get("id")
        if athlete_id is not None and str(athlete_id).strip():
            aliases.add(str(athlete_id))
    return frozenset(aliases)


def _persisted_athlete_aliases(document: Mapping[str, Any]) -> frozenset[str]:
    aliases = {
        str(alias)
        for fighter in _sequence(_canonical_bout_sidecar(document).get("fighters"))
        if isinstance(fighter, Mapping)
        if (alias := _mapping(fighter.get("source_ids")).get("espn_athlete_id"))
        is not None
    }
    if aliases:
        return frozenset(aliases)
    legacy = _mapping(document.get("fighters"))
    return frozenset(
        str(alias)
        for corner in ("red", "blue")
        if (alias := _mapping(legacy.get(corner)).get("espn_id")) is not None
    )


def _infer_espn_replacements(
    competitions: Sequence[Mapping[str, Any]],
    state: CanonicalCardState,
    legacy_by_competition: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    """Link a unique opponent swap without waiting for absence confirmation.

    ESPN assigns a new competition ID when one fighter stays on the card with
    a replacement opponent. It does not expose an explicit ``replaces`` field,
    but the shared ESPN athlete ID is stable. A one-to-one edge between one
    absent current bout and one present competition is stronger than an
    unexplained disappearance: it is an observed matchup replacement.

    Ambiguous edges are deliberately ignored and fall back to the conservative
    multi-pass absence policy.
    """

    present: dict[str, tuple[int, Mapping[str, Any] | None, frozenset[str]]] = {}
    for competition in competitions:
        competition_id = str(competition.get("id") or "")
        if not competition_id:
            continue
        legacy_bout = legacy_by_competition.get(competition_id)
        if legacy_bout is None and competition_id.isdigit():
            legacy_bout = state.bout_by_id.get(int(competition_id))
        if legacy_bout is None and not competition_id.isdigit():
            continue
        bout_id = (
            int(legacy_bout["id"])
            if legacy_bout is not None
            else int(competition_id)
        )
        aliases = _competition_athlete_aliases(competition)
        if len(aliases) == 2:
            present[competition_id] = (bout_id, legacy_bout, aliases)

    present_bout_ids = {item[0] for item in present.values()}
    absent: dict[int, frozenset[str]] = {}
    for bout_id, slot in state.slot_by_bout.items():
        if slot.get("is_current") is not True or bout_id in present_bout_ids:
            continue
        document = state.bout_by_id.get(bout_id)
        if document is None:
            continue
        status = _canonical_bout_sidecar(document).get("status") or document.get(
            "status"
        )
        aliases = _persisted_athlete_aliases(document)
        if status not in TERMINAL_BOUT_STATUSES and len(aliases) == 2:
            absent[bout_id] = aliases

    candidates_by_competition: dict[str, list[int]] = {}
    competitions_by_absent: dict[int, list[str]] = {}
    for competition_id, (_, document, aliases) in present.items():
        canonical = _canonical_bout_sidecar(document or {})
        if canonical.get("replaces_bout_id") is not None:
            continue
        candidates = [
            bout_id
            for bout_id, old_aliases in absent.items()
            if len(aliases & old_aliases) == 1
        ]
        candidates_by_competition[competition_id] = candidates
        for bout_id in candidates:
            competitions_by_absent.setdefault(bout_id, []).append(competition_id)

    return {
        competition_id: candidates[0]
        for competition_id, candidates in candidates_by_competition.items()
        if len(candidates) == 1
        and len(competitions_by_absent.get(candidates[0], ())) == 1
    }


def _persisted_fighter_id(
    legacy_bout: Optional[Mapping[str, Any]],
    corner: str,
    espn_athlete_id: str,
    display_name: Optional[str],
) -> Optional[str]:
    """Reuse an already-canonical fighter ID for the same human.

    Matching by source alias first and display name second preserves a stable
    ``fighter_id`` across sources so a Tapology-seeded corner does not look
    like a fighter replacement the first time ESPN observes it.  This never
    *matches bouts* by name; it only keeps an existing identity attached.
    """

    if legacy_bout is None:
        return None
    canonical = _canonical_bout_sidecar(legacy_bout)
    refs = [item for item in _sequence(canonical.get("fighters")) if isinstance(item, Mapping)]
    for ref in refs:
        alias = _mapping(ref.get("source_ids")).get("espn_athlete_id")
        if alias is not None and str(alias) == espn_athlete_id:
            return ref.get("fighter_id")
    if not _nonempty(display_name):
        return None
    best_score = 0.0
    best_id = None
    for ref in refs:
        score = name_similarity(display_name, ref.get("display_name"))
        if score > best_score:
            best_score = score
            best_id = ref.get("fighter_id")
    if best_score >= FIGHTER_IDENTITY_THRESHOLD:
        return best_id
    same_corner = next((ref for ref in refs if ref.get("corner") == corner), None)
    if same_corner is not None:
        legacy_fighters = _mapping(legacy_bout.get("fighters"))
        legacy_corner = _mapping(legacy_fighters.get(corner))
        if str(legacy_corner.get("espn_id") or "") == espn_athlete_id:
            return same_corner.get("fighter_id")
    return None


def _fighter_reference(
    competitor: Mapping[str, Any],
    corner: str,
    legacy_bout: Optional[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    athlete = _mapping(competitor.get("athlete"))
    athlete_id = str(competitor.get("id") or athlete.get("id") or "")
    display_name = athlete.get("displayName") or athlete.get("fullName")
    if not _nonempty(athlete_id) or not _nonempty(display_name):
        return None
    persisted = _persisted_fighter_id(legacy_bout, corner, athlete_id, display_name)
    return {
        "fighter_id": persisted or f"espn:{athlete_id}",
        "display_name": display_name,
        "corner": corner,
        "source_ids": {"espn_athlete_id": athlete_id},
        "identity_confidence": "exact_source",
    }


def _scheduled_rounds(competition: Mapping[str, Any]) -> Optional[int]:
    periods = _mapping(_mapping(competition.get("format")).get("regulation")).get(
        "periods"
    )
    return periods if periods in {3, 5} else None


def _method_family(detail_texts: Sequence[str]) -> str:
    combined = " ".join(str(item) for item in detail_texts).lower()
    for family, needles in _METHOD_FAMILIES.items():
        if any(needle in combined for needle in needles):
            return family
    return "other"


def _ending_seconds(display_clock: Any) -> Optional[int]:
    if not _nonempty(display_clock) or ":" not in display_clock:
        return None
    minutes, _, seconds = display_clock.partition(":")
    try:
        total = int(minutes) * 60 + int(seconds)
    except ValueError:
        return None
    return total if 0 <= total <= 300 else None


def _espn_result_values(
    competition: Mapping[str, Any],
    fighters: Sequence[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    status = _mapping(competition.get("status"))
    if not _mapping(status.get("type")).get("completed"):
        return None
    detail_texts = [
        str(_mapping(detail.get("type")).get("text") or "")
        for detail in _sequence(competition.get("details"))
        if isinstance(detail, Mapping)
    ]
    combined = " ".join(detail_texts).lower()
    winner_corner = None
    for fighter in fighters:
        corner = fighter.get("corner")
        competitor = fighter.get("_competitor")
        if isinstance(competitor, Mapping) and competitor.get("winner") is True:
            winner_corner = corner
    if winner_corner == "red":
        outcome = "red_win"
    elif winner_corner == "blue":
        outcome = "blue_win"
    elif "no contest" in combined:
        outcome = "no_contest"
    else:
        outcome = "draw"
    winner_id = next(
        (
            fighter.get("fighter_id")
            for fighter in fighters
            if fighter.get("corner") == winner_corner
        ),
        None,
    )
    family = _method_family(detail_texts)
    period = status.get("period")
    return {
        "outcome": outcome,
        "winner_fighter_id": winner_id if outcome in {"red_win", "blue_win"} else None,
        "method_family": family,
        "method_detail": detail_texts[0] if detail_texts else None,
        "ending_round": int(period) if _positive_int(period) else None,
        "ending_time_seconds": _ending_seconds(status.get("displayClock")),
    }


def _official_event_date(section_starts: Mapping[str, str]) -> Optional[str]:
    if not section_starts:
        return None
    earliest = min(section_starts.values())
    parsed = parse_espn_datetime(earliest)
    if parsed is None:
        return None
    return parsed.astimezone(ZoneInfo(EVENT_TIMEZONE)).date().isoformat()


def build_espn_card_observations(
    espn_event: Mapping[str, Any],
    state: CanonicalCardState,
    *,
    observed_at: str,
    competition_metadata: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> ObservationBatch:
    """Convert one ESPN scoreboard event into canonical observations.

    ``competition_metadata`` carries the richer per-competition detail payload
    keyed by ESPN competition ID.  Its presence upgrades the affected facts
    from ``espn_summary`` to ``espn_detail`` so explicit ESPN metadata outranks
    scoreboard inference exactly as the contract requires.
    """

    if not isinstance(espn_event, Mapping):
        raise ObservationSourceError("espn_event must be a payload mapping.")
    if not isinstance(state, CanonicalCardState):
        raise ObservationSourceError("state must be a CanonicalCardState.")
    if utc_string(observed_at) is None:
        raise ObservationSourceError("observed_at must be an ISO-8601 UTC instant.")
    observed_at = utc_string(observed_at)
    details = {str(key): _mapping(value) for key, value in _mapping(competition_metadata).items()}

    event_id = state.event_id
    source_event_id = str(espn_event.get("id") or "")
    findings: list[SourceFinding] = []
    if not _nonempty(source_event_id):
        return ObservationBatch(
            source="espn",
            event_id=event_id,
            findings=(
                _finding(
                    "ESPN_EVENT_ID_MISSING",
                    "error",
                    "events",
                    (event_id,),
                    "An ESPN payload without an event ID cannot be attributed.",
                ),
            ),
        )
    alias = state.espn_event_alias
    if alias is not None and alias != source_event_id:
        return ObservationBatch(
            source="espn",
            event_id=event_id,
            findings=(
                _finding(
                    "ESPN_EVENT_ALIAS_CONFLICT",
                    "error",
                    "events",
                    (event_id,),
                    "A contradicting ESPN alias must be quarantined, never applied.",
                ),
            ),
        )

    competitions = [
        item for item in _sequence(espn_event.get("competitions")) if isinstance(item, Mapping)
    ]
    if not competitions:
        return ObservationBatch(
            source="espn",
            event_id=event_id,
            findings=(
                _finding(
                    "ESPN_CARD_EMPTY",
                    "warning",
                    "events",
                    (event_id,),
                    "An ESPN payload without competitions retains the canonical card.",
                ),
            ),
        )

    sections = infer_competition_sections(competitions)
    legacy_by_competition: dict[str, Mapping[str, Any]] = {}
    legacy_by_id = state.bout_by_id
    for document in state.bouts:
        competition_id = document.get("espn_competition_id")
        if competition_id is not None:
            legacy_by_competition[str(competition_id)] = document
        canonical = _canonical_bout_sidecar(document)
        alias_id = _mapping(canonical.get("source_ids")).get("espn_competition_id")
        if alias_id is not None:
            legacy_by_competition[str(alias_id)] = document

    inferred_replacements = _infer_espn_replacements(
        competitions, state, legacy_by_competition
    )
    if inferred_replacements:
        findings.append(
            _finding(
                "ESPN_MATCHUP_REPLACEMENT_INFERRED",
                "info",
                "bouts",
                (
                    f"{old_bout_id}->{competition_id}"
                    for competition_id, old_bout_id in inferred_replacements.items()
                ),
                "A unique shared ESPN athlete links an absent matchup to its replacement.",
            )
        )

    total = len(competitions)
    observations: list[dict[str, Any]] = []
    section_starts: dict[str, str] = {}
    section_locks: dict[str, str] = {}
    matched_bout_ids: set[int] = set()
    unmatched_competitions: list[str] = []
    relisted_terminal: list[int] = []

    for index, competition in enumerate(competitions):
        competition_id = str(competition.get("id") or "")
        if not _nonempty(competition_id):
            unmatched_competitions.append(f"index-{index}")
            continue
        legacy_bout = legacy_by_competition.get(competition_id)
        if legacy_bout is None and competition_id.isdigit():
            # Historic deterministic allocation: an ESPN-created bout reuses
            # its competition ID as the canonical bout ID.  Nothing here
            # matches by fighter names.
            candidate = legacy_by_id.get(int(competition_id))
            if candidate is not None:
                legacy_bout = candidate
        if legacy_bout is None and not competition_id.isdigit():
            unmatched_competitions.append(competition_id)
            continue
        bout_id = (
            int(legacy_bout["id"]) if legacy_bout is not None else int(competition_id)
        )
        matched_bout_ids.add(bout_id)

        competitors = sorted_competitors(dict(competition))
        if len(competitors) != 2:
            unmatched_competitions.append(competition_id)
            continue
        corner_mapping = map_competitors_to_corners(
            dict(competition), dict(legacy_bout) if legacy_bout is not None else None
        )
        fighters = []
        for corner in ("red", "blue"):
            competitor = corner_mapping.get(corner)
            if not isinstance(competitor, Mapping):
                fighters = []
                break
            reference = _fighter_reference(competitor, corner, legacy_bout)
            if reference is None:
                fighters = []
                break
            reference["_competitor"] = competitor
            fighters.append(reference)
        if len(fighters) != 2:
            unmatched_competitions.append(competition_id)
            continue

        detail = details.get(competition_id, {})
        has_detail = bool(detail)
        bout_source_kind = "espn_detail" if has_detail else "espn_summary"
        canonical_previous = _canonical_bout_sidecar(legacy_bout or {})
        previous_status = canonical_previous.get("status") or (
            _mapping(legacy_bout).get("status") if legacy_bout is not None else None
        )
        competition_status = _mapping(
            _mapping(competition.get("status")).get("type")
        )
        completed = bool(competition_status.get("completed"))
        explicitly_cancelled = competition_status.get("name") in {
            "STATUS_CANCELED",
            "STATUS_CANCELLED",
        }
        bout_values: dict[str, Any] = {
            "source_ids": {"espn_competition_id": competition_id},
            "fighters": [
                {key: value for key, value in fighter.items() if key != "_competitor"}
                for fighter in fighters
            ],
        }
        if competition_id in inferred_replacements:
            bout_values["replaces_bout_id"] = inferred_replacements[competition_id]
        weight_class = normalize_weight_class(
            detail.get("weight_class")
            or _mapping(competition.get("type")).get("text")
            or _mapping(competition.get("type")).get("abbreviation")
        )
        if _nonempty(weight_class):
            bout_values["weight_class"] = weight_class
            bout_values["gender"] = (
                "female"
                if weight_class.lower().startswith(("women", "w "))
                else "male"
            )
        rounds = detail.get("rounds_scheduled") or _scheduled_rounds(competition)
        if rounds in {3, 5}:
            bout_values["scheduled_rounds"] = rounds
        if explicitly_cancelled:
            bout_values["status"] = "cancelled"
        elif previous_status in TERMINAL_BOUT_STATUSES and not completed:
            # A terminal lifecycle is never revived by a refetch.
            relisted_terminal.append(bout_id)
        elif not completed:
            bout_values["status"] = "scheduled"
        if "titleFight" in competition:
            # Advisory only: the normalizer records external title signals as
            # suggestions and resolves canonical TITLE from Admin alone.
            bout_values["is_title_fight"] = bool(competition.get("titleFight"))
        observations.append(
            _observation(
                event_id=event_id,
                entity_type="bout",
                entity_id=bout_id,
                values=bout_values,
                source_kind=bout_source_kind,
                source_event_id=source_event_id,
                observed_at=observed_at,
                prefix="espn",
                suffix=f"bout-{competition_id}",
                identity_basis="source_competition_id",
            )
        )

        section = (
            detail.get("card_section")
            if detail.get("card_section") in CARD_SECTIONS
            else sections.get(competition_id)
        )
        start = utc_string(competition.get("date"))
        lock = utc_string(detail.get("automatic_lock_time_utc")) or start
        if bout_id not in relisted_terminal and not explicitly_cancelled:
            slot_values: dict[str, Any] = {
                "is_current": True,
                "order_overall": total - index,
            }
            if section in CARD_SECTIONS:
                slot_values["card_section"] = section
                if start is not None:
                    section_starts[section] = min(
                        [start, section_starts.get(section, start)]
                    )
                if lock is not None:
                    section_locks[section] = min(
                        [lock, section_locks.get(section, lock)]
                    )
            if start is not None:
                slot_values["scheduled_start_time_utc"] = start
            if lock is not None:
                slot_values["automatic_lock_time_utc"] = lock
            observations.append(
                _observation(
                    event_id=event_id,
                    entity_type="slot",
                    entity_id=bout_id,
                    values=slot_values,
                    source_kind="espn_detail" if has_detail else "espn_summary",
                    source_event_id=source_event_id,
                    observed_at=observed_at,
                    prefix="espn",
                    suffix=f"slot-{competition_id}",
                )
            )

        result_values = _espn_result_values(competition, fighters)
        if result_values is not None:
            observations.append(
                _observation(
                    event_id=event_id,
                    entity_type="result",
                    entity_id=bout_id,
                    values=result_values,
                    source_kind="espn_detail",
                    source_event_id=source_event_id,
                    observed_at=observed_at,
                    prefix="espn",
                    suffix=f"result-{competition_id}",
                    identity_basis="source_competition_id",
                )
            )

    event_values: dict[str, Any] = {
        "source_ids": {"espn_event_id": source_event_id},
        "promotion": "UFC",
    }
    if _nonempty(espn_event.get("name")):
        event_values["name"] = espn_event["name"]
    status_type = _mapping(_mapping(espn_event.get("status")).get("type"))
    if status_type.get("completed") or status_type.get("state") == "post":
        event_values["status"] = "completed"
    elif status_type.get("name") in {"STATUS_CANCELED", "STATUS_CANCELLED"}:
        event_values["status"] = "cancelled"
    else:
        event_values["status"] = "scheduled"
    official_date = _official_event_date(section_starts)
    if official_date is not None:
        event_values["official_event_date"] = official_date
        event_values["official_date_timezone"] = EVENT_TIMEZONE
    if section_starts:
        event_values["section_start_times_utc"] = dict(sorted(section_starts.items()))
    if section_locks:
        event_values["section_lock_times_utc"] = dict(sorted(section_locks.items()))
    observations.append(
        _observation(
            event_id=event_id,
            entity_type="event",
            entity_id=event_id,
            values=event_values,
            source_kind="espn_detail" if details else "espn_summary",
            source_event_id=source_event_id,
            observed_at=observed_at,
            prefix="espn",
            suffix="event",
            identity_basis="source_alias",
        )
    )

    absent = sorted(
        bout_id
        for bout_id, slot in state.slot_by_bout.items()
        if slot.get("is_current") is True and bout_id not in matched_bout_ids
    )
    if absent:
        findings.append(
            _finding(
                "BOUT_ABSENT_FROM_ESPN_PAYLOAD",
                "warning",
                "bouts",
                absent,
                "A bout missing from this payload is retained; removal needs the change policy.",
            )
        )
    if unmatched_competitions:
        findings.append(
            _finding(
                "ESPN_COMPETITION_UNMATCHED",
                "warning",
                "bouts",
                unmatched_competitions,
                "An ESPN competition without a canonical identity was skipped.",
            )
        )
    if relisted_terminal:
        findings.append(
            _finding(
                "TERMINAL_BOUT_RELISTED",
                "warning",
                "bouts",
                relisted_terminal,
                "A cancelled/replaced bout reappeared on the ESPN card and was not revived.",
            )
        )
    observations.sort(key=lambda item: item["observation_id"])
    return ObservationBatch(
        source="espn",
        event_id=event_id,
        observations=tuple(observations),
        findings=tuple(findings),
        present_bout_ids=tuple(sorted(matched_bout_ids)),
    )


@dataclass(frozen=True)
class AdminCardCommand:
    """One Admin CardData command awaiting canonical resolution."""

    command_id: str
    kind: str
    event_id: int
    observed_at: str
    reason: str
    bout_id: Optional[int] = None
    values: Mapping[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not _nonempty(self.command_id):
            raise ObservationSourceError("command_id must be a non-empty string.")
        if self.kind not in ADMIN_COMMAND_KINDS:
            raise ObservationSourceError(f"Unsupported Admin kind {self.kind!r}.")
        if not _positive_int(self.event_id):
            raise ObservationSourceError("event_id must be a positive integer.")
        if utc_string(self.observed_at) is None:
            raise ObservationSourceError("observed_at must be an ISO-8601 UTC instant.")
        if not _nonempty(self.reason):
            raise ObservationSourceError("Every Admin command requires a reason.")
        if self.kind not in {"event_timing", "event_lifecycle"} and not _positive_int(
            self.bout_id
        ):
            raise ObservationSourceError(f"{self.kind} requires a positive bout_id.")
        if not isinstance(self.values, Mapping):
            raise ObservationSourceError("values must be a mapping.")


def _admin_event_timing_values(command: AdminCardCommand) -> dict[str, Any]:
    values: dict[str, Any] = {}
    raw_date = command.values.get("official_event_date")
    if isinstance(raw_date, date) and not isinstance(raw_date, datetime):
        values["official_event_date"] = raw_date.isoformat()
    elif _nonempty(raw_date):
        values["official_event_date"] = raw_date[:10]
    if _nonempty(command.values.get("official_date_timezone")):
        values["official_date_timezone"] = command.values["official_date_timezone"]
    elif "official_event_date" in values:
        values["official_date_timezone"] = EVENT_TIMEZONE
    for map_field in ("section_start_times_utc", "section_lock_times_utc"):
        raw_map = _mapping(command.values.get(map_field))
        resolved = {
            section: utc_string(value)
            for section, value in sorted(raw_map.items())
            if section in CARD_SECTIONS and utc_string(value) is not None
        }
        if resolved:
            values[map_field] = resolved
    if not values:
        raise ObservationSourceError(
            "An Admin timing command must change at least one timing fact."
        )
    return values


def _admin_result_values(command: AdminCardCommand) -> dict[str, Any]:
    raw = _mapping(command.values)
    outcome = _CANONICAL_OUTCOMES.get(str(raw.get("outcome") or "").lower())
    if outcome is None:
        raise ObservationSourceError("An Admin result requires a canonical outcome.")
    family = str(raw.get("method_family") or "").lower()
    if family not in {"ko_tko", "submission", "decision", "dq", "other"}:
        family = _method_family([str(raw.get("method") or raw.get("method_detail") or "")])
    winner_id = raw.get("winner_fighter_id")
    if outcome in {"draw", "no_contest"}:
        winner_id = None
    elif not _nonempty(winner_id):
        raise ObservationSourceError(
            "An Admin decisive result requires winner_fighter_id."
        )
    ending_round = raw.get("ending_round")
    ending_seconds = raw.get("ending_time_seconds")
    return {
        "outcome": outcome,
        "winner_fighter_id": winner_id,
        "method_family": family,
        "method_detail": raw.get("method_detail") or raw.get("method"),
        "ending_round": ending_round if _positive_int(ending_round) else None,
        "ending_time_seconds": (
            ending_seconds
            if isinstance(ending_seconds, int)
            and not isinstance(ending_seconds, bool)
            and 0 <= ending_seconds <= 300
            else None
        ),
    }


def _admin_title_values(command: AdminCardCommand) -> dict[str, Any]:
    raw = _mapping(command.values)
    is_title = raw.get("is_title_fight")
    if is_title not in {True, False}:
        raise ObservationSourceError(
            "Admin TITLE authority requires an explicit true or false."
        )
    is_bmf = bool(raw.get("is_bmf_title_fight"))
    title_type = raw.get("title_type")
    if is_title is False:
        return {
            "is_title_fight": False,
            "is_bmf_title_fight": False,
            "title_type": "none",
        }
    if is_bmf:
        title_type = "bmf"
    if title_type not in {"undisputed", "interim", "bmf", "other", "unknown"}:
        title_type = "unknown"
    return {
        "is_title_fight": True,
        "is_bmf_title_fight": title_type == "bmf",
        "title_type": title_type,
    }


def build_admin_card_observations(
    command: AdminCardCommand,
    state: CanonicalCardState,
) -> ObservationBatch:
    """Convert one Admin command into a durable ``admin_override`` proposal.

    Every Admin fact is emitted at the highest source rank with explicit
    evidence, so a later ESPN or Tapology observation of the same field is
    refused by the normalizer instead of silently winning.
    """

    if not isinstance(state, CanonicalCardState):
        raise ObservationSourceError("state must be a CanonicalCardState.")
    command.validate()
    if command.event_id != state.event_id:
        raise ObservationSourceError(
            "An Admin command must target the loaded canonical event."
        )
    observed_at = utc_string(command.observed_at)
    source_event_id = state.espn_event_alias or f"admin-event-{state.event_id}"
    suffix = f"{command.kind}-{command.command_id}"
    findings: list[SourceFinding] = []

    if command.kind in {"event_timing", "event_lifecycle"}:
        if command.kind == "event_timing":
            values = _admin_event_timing_values(command)
        else:
            status = command.values.get("status")
            if status not in {"scheduled", "live", "completed", "cancelled", "postponed"}:
                raise ObservationSourceError(
                    "An Admin event lifecycle change requires a contract status."
                )
            values = {"status": status}
        observation = _observation(
            event_id=state.event_id,
            entity_type="event",
            entity_id=state.event_id,
            values=values,
            source_kind="admin_override",
            source_event_id=source_event_id,
            observed_at=observed_at,
            prefix="admin",
            suffix=suffix,
            identity_basis="admin",
            reason=command.reason,
        )
        return ObservationBatch(
            source="admin",
            event_id=state.event_id,
            observations=(observation,),
            findings=tuple(findings),
        )

    bout_id = int(command.bout_id)
    if bout_id not in state.bout_by_id:
        findings.append(
            _finding(
                "ADMIN_BOUT_NOT_PERSISTED",
                "warning",
                "bouts",
                (bout_id,),
                "The Admin command targets a bout with no persisted document.",
            )
        )

    if command.kind == "title":
        values = _admin_title_values(command)
        entity_type = "bout"
        clear_fields: tuple[str, ...] = ()
    elif command.kind == "bout_lifecycle":
        status = command.values.get("status")
        if status not in {"scheduled", "cancelled", "postponed", "replaced", "completed"}:
            raise ObservationSourceError(
                "An Admin bout lifecycle change requires a contract status."
            )
        values = {"status": status}
        if _positive_int(command.values.get("replaced_by_bout_id")):
            values["replaced_by_bout_id"] = command.values["replaced_by_bout_id"]
        entity_type = "bout"
        clear_fields = ()
    elif command.kind == "bout_timing":
        values = {}
        for timing_field in ("scheduled_start_time_utc", "automatic_lock_time_utc"):
            resolved = utc_string(command.values.get(timing_field))
            if resolved is not None:
                values[timing_field] = resolved
        if not values:
            raise ObservationSourceError(
                "An Admin bout timing change requires at least one instant."
            )
        entity_type = "slot"
        clear_fields = ()
    elif command.kind == "bout_structure":
        values = {}
        if command.values.get("card_section") in CARD_SECTIONS:
            values["card_section"] = command.values["card_section"]
        if _positive_int(command.values.get("order_overall")):
            values["order_overall"] = command.values["order_overall"]
        if isinstance(command.values.get("is_current"), bool):
            values["is_current"] = command.values["is_current"]
        if not values:
            raise ObservationSourceError(
                "An Admin structure change requires section, order or currency."
            )
        entity_type = "slot"
        clear_fields = ()
    elif command.kind == "result":
        values = _admin_result_values(command)
        entity_type = "result"
        clear_fields = ()
    else:  # clear_result
        values = {}
        entity_type = "result"
        clear_fields = ("result",)

    observation = _observation(
        event_id=state.event_id,
        entity_type=entity_type,
        entity_id=bout_id,
        values=values,
        source_kind="admin_override",
        source_event_id=source_event_id,
        observed_at=observed_at,
        prefix="admin",
        suffix=suffix,
        identity_basis="admin",
        reason=command.reason,
        clear_fields=clear_fields,
    )
    return ObservationBatch(
        source="admin",
        event_id=state.event_id,
        observations=(observation,),
        findings=tuple(findings),
    )


__all__ = [
    "ADMIN_COMMAND_KINDS",
    "EVENT_TIMEZONE",
    "OBSERVATION_SOURCE_VERSION",
    "AdminCardCommand",
    "ObservationBatch",
    "ObservationSourceError",
    "SourceFinding",
    "build_admin_card_observations",
    "build_espn_card_observations",
    "utc_string",
]
