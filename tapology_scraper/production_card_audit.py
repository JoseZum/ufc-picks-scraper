"""Strict read-only audit of the Q-014 production golden-card set.

This module is intentionally a legacy-data inventory, not the future SCR-008
normalizer.  Mongo access is limited to projected ``find`` operations against
``events``, ``bouts`` and ``event_card_slots`` for four allowlisted event IDs.
The report contains aggregate counts, issue codes and at most three entity IDs
per issue; raw documents, fighter names, URLs and credentials are never emitted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional, TextIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import dotenv_values

from tapology_scraper.card_data_contract import CAPABILITIES


AUDIT_SCHEMA_VERSION = "production-card-audit/v1"
AUDITED_COLLECTIONS = ("events", "bouts", "event_card_slots")
CAPABILITY_STATES = ("ready", "pending", "degraded", "blocked", "quarantined")
SECTIONS = {"early_prelim", "prelim", "main"}
ACTIVE_BOUT_STATUSES = {"scheduled", "live", "completed"}
KNOWN_BOUT_STATUSES = ACTIVE_BOUT_STATUSES | {"cancelled", "postponed", "replaced"}
BLOCKING_SEVERITIES = {"blocking", "error"}
EXIT_PASS = 0
EXIT_FINDINGS = 1
EXIT_CONFIGURATION_ERROR = 2


@dataclass(frozen=True)
class GoldenCardSpec:
    event_id: int
    expected_name: str
    scenario_tags: tuple[str, ...]
    expected_espn_event_id: Optional[str] = None
    expected_sections: tuple[str, ...] = ()
    minimum_title_bouts: int = 0
    minimum_retained_cancelled_bouts: int = 0
    expected_completed: bool = False


GOLDEN_CARD_SPECS = (
    GoldenCardSpec(
        event_id=135755,
        expected_name="UFC Fight Night: Medic vs. Rodriguez",
        scenario_tags=("fight_night", "pre_event", "two_section"),
        expected_espn_event_id="600059339",
        expected_sections=("prelim", "main"),
    ),
    GoldenCardSpec(
        event_id=142341,
        expected_name="UFC 330: Makhachev vs. Machado Garry",
        scenario_tags=("numbered", "three_section", "title_heavy", "pre_event"),
        expected_espn_event_id="600059185",
        expected_sections=("early_prelim", "prelim", "main"),
        minimum_title_bouts=2,
    ),
    GoldenCardSpec(
        event_id=136871,
        expected_name="UFC 326: Holloway vs. Oliveira 2",
        scenario_tags=("numbered", "three_section", "completed", "late_cancellation"),
        expected_sections=("early_prelim", "prelim", "main"),
        minimum_title_bouts=1,
        minimum_retained_cancelled_bouts=1,
        expected_completed=True,
    ),
    GoldenCardSpec(
        event_id=142997,
        expected_name="UFC Fight Night: Medic vs. Rodriguez",
        scenario_tags=(
            "fight_night",
            "two_section",
            "positive_identity",
            "pre_event",
        ),
        expected_espn_event_id="600059339",
        expected_sections=("prelim", "main"),
    ),
)
GOLDEN_EVENT_IDS = tuple(spec.event_id for spec in GOLDEN_CARD_SPECS)


EVENT_PROJECTION = {
    "_id": 0,
    "id": 1,
    "source": 1,
    "promotion": 1,
    "name": 1,
    "date": 1,
    "official_event_date": 1,
    "official_date_timezone": 1,
    "month_key": 1,
    "timezone": 1,
    "status": 1,
    "total_bouts": 1,
    "listed_bout_count": 1,
    "mission_eligible_bout_count": 1,
    "main_event_bout_id": 1,
    "espn_event_id": 1,
    "card_start_time_utc": 1,
    "picks_lock_time_utc": 1,
    "section_start_times_utc": 1,
    "section_lock_times_utc": 1,
    "first_final_result_at": 1,
    "lifecycle_revision": 1,
    "structure_revision": 1,
    "timing_revision": 1,
    "current_eligibility": 1,
    "evidence": 1,
}

BOUT_PROJECTION = {
    "_id": 0,
    "id": 1,
    "event_id": 1,
    "source": 1,
    "espn_competition_id": 1,
    "lineage_id": 1,
    "matchup_revision": 1,
    "replaces_bout_id": 1,
    "replaced_by_bout_id": 1,
    "status": 1,
    "rounds_scheduled": 1,
    "scheduled_rounds": 1,
    "is_title_fight": 1,
    "is_bmf_title_fight": 1,
    "title_type": 1,
    "is_main_event": 1,
    "is_co_main_event": 1,
    "card_section": 1,
    "card_order": 1,
    "order_overall": 1,
    "order_section": 1,
    "automatic_lock_time_utc": 1,
    "result_revision": 1,
    "result.revision": 1,
    "result.status": 1,
    "result.outcome": 1,
    "result.winner": 1,
    "result.winner_fighter_id": 1,
    "result.method": 1,
    "result.method_family": 1,
    "result.round": 1,
    "result.ending_round": 1,
    "result.time": 1,
    "result.ending_time_seconds": 1,
    "fighters.red.fighter_id": 1,
    "fighters.red.espn_id": 1,
    "fighters.red.tapology_id": 1,
    "fighters.blue.fighter_id": 1,
    "fighters.blue.espn_id": 1,
    "fighters.blue.tapology_id": 1,
    "evidence": 1,
}

SLOT_PROJECTION = {
    "_id": 0,
    "id": 1,
    "slot_id": 1,
    "event_id": 1,
    "bout_id": 1,
    "is_current": 1,
    "card_section": 1,
    "order_overall": 1,
    "order_section": 1,
    "is_main_event": 1,
    "is_co_main": 1,
    "role": 1,
    "scheduled_start_time_utc": 1,
    "automatic_lock_time_utc": 1,
    "structure_revision": 1,
    "source": 1,
    "evidence": 1,
}


class ProductionAuditConfigurationError(ValueError):
    """Safe operator-facing configuration error without secret contents."""


@dataclass(frozen=True)
class LegacyCardDocuments:
    spec: GoldenCardSpec
    event: Optional[Mapping[str, Any]]
    bouts: tuple[Mapping[str, Any], ...]
    slots: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class ProductionAuditIssue:
    code: str
    severity: str
    capability: str
    collection: str
    affected_count: int
    example_ids: tuple[str, ...]
    message: str

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["example_ids"] = list(self.example_ids)
        return value


@dataclass(frozen=True)
class ProductionCapabilityAudit:
    capability: str
    state: str
    issue_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "state": self.state,
            "issue_codes": list(self.issue_codes),
        }


@dataclass(frozen=True)
class ProductionCardAudit:
    event_id: int
    expected_name: str
    observed_name_matches: bool
    scenario_tags: tuple[str, ...]
    event_found: bool
    observed_status: str
    observed_date: Optional[str]
    counts: tuple[tuple[str, int], ...]
    section_counts: tuple[tuple[str, int], ...]
    result_method_counts: tuple[tuple[str, int], ...]
    capabilities: tuple[ProductionCapabilityAudit, ...]
    issues: tuple[ProductionAuditIssue, ...]

    @property
    def has_blocking_findings(self) -> bool:
        return any(issue.severity in BLOCKING_SEVERITIES for issue in self.issues)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "expected_name": self.expected_name,
            "observed_name_matches": self.observed_name_matches,
            "scenario_tags": list(self.scenario_tags),
            "event_found": self.event_found,
            "observed_status": self.observed_status,
            "observed_date": self.observed_date,
            "counts": dict(self.counts),
            "section_counts": dict(self.section_counts),
            "result_method_counts": dict(self.result_method_counts),
            "capabilities": [item.as_dict() for item in self.capabilities],
            "issues": [item.as_dict() for item in self.issues],
        }


@dataclass(frozen=True)
class ProductionCardAuditReport:
    cards: tuple[ProductionCardAudit, ...]

    @property
    def passed(self) -> bool:
        return bool(self.cards) and all(
            card.event_found and not card.has_blocking_findings for card in self.cards
        )

    @property
    def exit_code(self) -> int:
        return EXIT_PASS if self.passed else EXIT_FINDINGS

    def summary(self) -> dict[str, Any]:
        state_counts = {
            capability: {
                state: sum(
                    1
                    for card in self.cards
                    for item in card.capabilities
                    if item.capability == capability and item.state == state
                )
                for state in CAPABILITY_STATES
            }
            for capability in CAPABILITIES
        }
        issue_counts = Counter(
            issue.code for card in self.cards for issue in card.issues
        )
        counts = Counter()
        for card in self.cards:
            counts.update(dict(card.counts))
        return {
            "card_count": len(self.cards),
            "event_found_count": sum(card.event_found for card in self.cards),
            "cards_with_blocking_findings": sum(
                card.has_blocking_findings for card in self.cards
            ),
            "document_counts": {
                "events": sum(card.event_found for card in self.cards),
                "bouts": counts["bouts"],
                "event_card_slots": counts["slots"],
            },
            "capability_state_counts": state_counts,
            "issue_code_counts": dict(sorted(issue_counts.items())),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "source": "production_mongodb_strict_read_only",
            "collections": list(AUDITED_COLLECTIONS),
            "event_allowlist": list(GOLDEN_EVENT_IDS),
            "raw_documents_retained": False,
            "contains_credentials": False,
            "passed": self.passed,
            "summary": self.summary(),
            "cards": [card.as_dict() for card in self.cards],
        }


class _Issues:
    def __init__(self) -> None:
        self.items: list[ProductionAuditIssue] = []

    def add(
        self,
        code: str,
        severity: str,
        capability: str,
        collection: str,
        affected_ids: Iterable[Any],
        message: str,
        *,
        affected_count: Optional[int] = None,
    ) -> None:
        ids = sorted({str(value) for value in affected_ids if value is not None})
        self.items.append(
            ProductionAuditIssue(
                code=code,
                severity=severity,
                capability=capability,
                collection=collection,
                affected_count=len(ids) if affected_count is None else affected_count,
                example_ids=tuple(ids[:3]),
                message=message,
            )
        )

    def finish(self) -> tuple[ProductionAuditIssue, ...]:
        return tuple(
            sorted(
                set(self.items),
                key=lambda issue: (
                    CAPABILITIES.index(issue.capability),
                    issue.collection,
                    issue.code,
                    issue.example_ids,
                    issue.message,
                ),
            )
        )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    return (
        value
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
        else ()
    )


def _positive_int(value: Any) -> bool:
    return type(value) is int and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _normalized_identity_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    ascii_like = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return " ".join(
        "".join(character if character.isalnum() else " " for character in ascii_like)
        .split()
    )


def _document_id(document: Mapping[str, Any]) -> Any:
    return document.get("id") or document.get("bout_id") or document.get("slot_id")


def _date_string(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10]).isoformat()
        except ValueError:
            return None
    return None


def _iana_timezone(value: Any) -> bool:
    if not _nonempty(value):
        return False
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    return True


def _fighter_identity(fighter: Any) -> Optional[str]:
    fighter = _mapping(fighter)
    for field in ("fighter_id", "espn_id", "tapology_id"):
        value = fighter.get(field)
        if value is not None and str(value).strip():
            return f"{field}:{value}"
    return None


def _result(document: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(document.get("result"))


def _role(slot: Mapping[str, Any]) -> str:
    if slot.get("role") in {"main_event", "co_main", "regular"}:
        return slot["role"]
    if slot.get("is_main_event") is True:
        return "main_event"
    if slot.get("is_co_main") is True:
        return "co_main"
    return "regular"


def _current_slots(
    slots: Sequence[Mapping[str, Any]],
    bout_by_id: Mapping[int, Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    current = []
    for slot in slots:
        bout = bout_by_id.get(slot.get("bout_id"))
        explicit_current = slot.get("is_current")
        if explicit_current is False:
            continue
        if bout is not None and bout.get("status") in {"cancelled", "replaced"}:
            continue
        current.append(slot)
    return current


def _validate_orders(
    slots: Sequence[Mapping[str, Any]],
    field: str,
    duplicate_code: str,
    gap_code: str,
    issues: _Issues,
) -> None:
    values = [slot.get(field) for slot in slots]
    invalid = [
        _document_id(slot)
        for slot in slots
        if not _positive_int(slot.get(field))
    ]
    if invalid:
        issues.add(
            "ORDER_MISSING",
            "blocking",
            "STRUCT",
            "event_card_slots",
            invalid,
            f"Current slots require a positive {field}.",
        )
    valid = [value for value in values if _positive_int(value)]
    duplicates = sorted(value for value, count in Counter(valid).items() if count > 1)
    if duplicates:
        issues.add(
            duplicate_code,
            "error",
            "STRUCT",
            "event_card_slots",
            duplicates,
            f"Current slots contain duplicate {field} values.",
        )
    if sorted(set(valid)) != list(range(1, len(slots) + 1)):
        issues.add(
            gap_code,
            "error",
            "STRUCT",
            "event_card_slots",
            valid,
            f"Current {field} values are not contiguous 1..N.",
            affected_count=len(slots),
        )


def _audit_event(
    card: LegacyCardDocuments,
    issues: _Issues,
) -> tuple[str, Optional[str], bool]:
    event = card.event
    if event is None:
        for capability in CAPABILITIES:
            issues.add(
                "EVENT_NOT_FOUND",
                "error",
                capability,
                "events",
                (card.spec.event_id,),
                "The allowlisted production event document was not found.",
            )
        return "missing", None, False

    event_id = event.get("id")
    if event_id != card.spec.event_id:
        issues.add(
            "EVENT_ID_MISMATCH",
            "error",
            "EVT",
            "events",
            (event_id,),
            "The fetched event identity does not match the allowlisted ID.",
        )
    observed_name = event.get("name")
    name_matches = (
        _normalized_identity_text(observed_name)
        == _normalized_identity_text(card.spec.expected_name)
    )
    if not name_matches:
        issues.add(
            "EVENT_NAME_MISMATCH",
            "warning",
            "EVT",
            "events",
            (card.spec.event_id,),
            "Observed event name does not match the approved golden-card identity.",
        )
    status = event.get("status")
    if status not in {"scheduled", "live", "completed", "cancelled", "postponed"}:
        issues.add(
            "EVENT_STATUS_INVALID",
            "blocking",
            "EVT",
            "events",
            (card.spec.event_id,),
            "Event status is absent or outside the canonical lifecycle enum.",
        )
    if card.spec.expected_completed and status != "completed":
        issues.add(
            "EXPECTED_COMPLETED_EVENT_NOT_COMPLETED",
            "blocking",
            "EVT",
            "events",
            (card.spec.event_id,),
            "The completed golden card is not marked completed in production.",
        )
    if not all(
        _positive_int(event.get(field))
        for field in ("lifecycle_revision", "structure_revision", "timing_revision")
    ):
        issues.add(
            "EVENT_REVISIONS_MISSING",
            "warning",
            "EVT",
            "events",
            (card.spec.event_id,),
            "Legacy event facts are usable but lifecycle/structure/timing revisions are absent.",
        )

    observed_date = _date_string(
        event.get("official_event_date") or event.get("date")
    )
    if observed_date is None:
        issues.add(
            "OFFICIAL_DATE_MISSING",
            "blocking",
            "EVT_DATE",
            "events",
            (card.spec.event_id,),
            "No parseable event date is available.",
        )
    elif "official_event_date" not in event:
        issues.add(
            "OFFICIAL_DATE_NOT_CANONICAL",
            "warning",
            "EVT_DATE",
            "events",
            (card.spec.event_id,),
            "Legacy date exists but official_event_date has not been persisted/frozen.",
        )
    timezone_value = event.get("official_date_timezone") or event.get("timezone")
    if not _iana_timezone(timezone_value):
        issues.add(
            "TIMEZONE_AMBIGUOUS",
            "blocking",
            "EVT_DATE",
            "events",
            (card.spec.event_id,),
            "No valid IANA timezone is available for the official event date.",
        )
    expected_month = observed_date[:7] if observed_date else None
    if event.get("month_key") != expected_month:
        issues.add(
            "MONTH_KEY_MISSING_OR_MISMATCHED",
            "warning" if expected_month else "blocking",
            "EVT_DATE",
            "events",
            (card.spec.event_id,),
            "month_key is absent or does not match the observed event date.",
        )
    if (
        card.spec.expected_espn_event_id is not None
        and str(event.get("espn_event_id") or "") != card.spec.expected_espn_event_id
    ):
        issues.add(
            "EXPECTED_ESPN_EVENT_ALIAS_MISSING",
            "blocking",
            "EVT",
            "events",
            (card.spec.event_id,),
            "The approved ESPN event alias is missing or mismatched.",
        )
    return str(status or "unknown"), observed_date, name_matches


def _audit_bouts(card: LegacyCardDocuments, issues: _Issues) -> dict[str, Any]:
    bouts = list(card.bouts)
    if not bouts:
        issues.add(
            "BOUTS_MISSING",
            "blocking",
            "BOUT",
            "bouts",
            (card.spec.event_id,),
            "No bout documents were found for the golden card.",
        )
    wrong_event = [
        _document_id(bout) for bout in bouts if bout.get("event_id") != card.spec.event_id
    ]
    if wrong_event:
        issues.add(
            "BOUT_EVENT_MISMATCH",
            "error",
            "BOUT",
            "bouts",
            wrong_event,
            "Bout documents reference a different event.",
        )
    duplicate_ids = [
        bout_id
        for bout_id, count in Counter(_document_id(bout) for bout in bouts).items()
        if count > 1
    ]
    if duplicate_ids:
        issues.add(
            "BOUT_ID_DUPLICATE",
            "error",
            "BOUT",
            "bouts",
            duplicate_ids,
            "Bout IDs are not unique inside the event.",
        )
    invalid_status = [
        _document_id(bout) for bout in bouts if bout.get("status") not in KNOWN_BOUT_STATUSES
    ]
    if invalid_status:
        issues.add(
            "BOUT_STATUS_INVALID",
            "blocking",
            "BOUT",
            "bouts",
            invalid_status,
            "Bout status is absent or outside the canonical lifecycle enum.",
        )

    unresolved = []
    for bout in bouts:
        fighters = _mapping(bout.get("fighters"))
        red = _fighter_identity(fighters.get("red"))
        blue = _fighter_identity(fighters.get("blue"))
        if red is None or blue is None or red == blue:
            unresolved.append(_document_id(bout))
    if unresolved:
        issues.add(
            "FIGHTER_ID_UNRESOLVED",
            "blocking",
            "BOUT",
            "bouts",
            unresolved,
            "At least one matchup lacks two distinct stable fighter source/internal IDs.",
        )
    missing_lineage = [
        _document_id(bout) for bout in bouts if not _nonempty(bout.get("lineage_id"))
    ]
    if missing_lineage:
        issues.add(
            "LINEAGE_MISSING",
            "warning",
            "BOUT",
            "bouts",
            missing_lineage,
            "Legacy bouts do not persist replacement lineage.",
        )
    missing_revision = [
        _document_id(bout)
        for bout in bouts
        if not _positive_int(bout.get("matchup_revision"))
    ]
    if missing_revision:
        issues.add(
            "MATCHUP_REVISION_MISSING",
            "warning",
            "BOUT",
            "bouts",
            missing_revision,
            "Legacy bouts do not persist matchup_revision.",
        )

    cancelled = [bout for bout in bouts if bout.get("status") == "cancelled"]
    if len(cancelled) < card.spec.minimum_retained_cancelled_bouts:
        issues.add(
            "KNOWN_CANCELLATION_NOT_RETAINED",
            "blocking",
            "BOUT",
            "bouts",
            (card.spec.event_id,),
            "Production does not retain the minimum known cancelled matchup for this card.",
            affected_count=card.spec.minimum_retained_cancelled_bouts - len(cancelled),
        )
    active = [bout for bout in bouts if bout.get("status") in ACTIVE_BOUT_STATUSES]
    return {
        "bouts": bouts,
        "active": active,
        "cancelled": cancelled,
        "completed": [bout for bout in bouts if bout.get("status") == "completed"],
        "with_results": [bout for bout in bouts if bool(_result(bout))],
        "resolved_fighter_refs": 2 * len(bouts) - 2 * len(unresolved),
        "fighter_ref_total": 2 * len(bouts),
    }


def _audit_eligibility(
    card: LegacyCardDocuments,
    bout_facts: Mapping[str, Any],
    issues: _Issues,
) -> int:
    event = card.event or {}
    snapshot = _mapping(event.get("current_eligibility"))
    if not snapshot:
        issues.add(
            "ELIGIBILITY_SNAPSHOT_MISSING",
            "blocking",
            "ELIG",
            "events",
            (card.spec.event_id,),
            "No immutable eligibility targets, exclusions, denominator or fingerprint exist.",
        )
    elif not (
        isinstance(snapshot.get("eligible_targets"), list)
        and isinstance(snapshot.get("excluded_targets"), list)
        and type(snapshot.get("denominator")) is int
        and _nonempty(snapshot.get("fingerprint"))
    ):
        issues.add(
            "ELIGIBILITY_SNAPSHOT_INVALID",
            "error",
            "ELIG",
            "events",
            (card.spec.event_id,),
            "Persisted eligibility snapshot is incomplete or malformed.",
        )
    candidate_count = 0
    for bout in bout_facts["active"]:
        fighters = _mapping(bout.get("fighters"))
        identities = (
            _fighter_identity(fighters.get("red")),
            _fighter_identity(fighters.get("blue")),
        )
        if all(identities) and identities[0] != identities[1]:
            candidate_count += 1
    return candidate_count


def _audit_structure(
    card: LegacyCardDocuments,
    bout_facts: Mapping[str, Any],
    issues: _Issues,
) -> tuple[dict[str, int], list[Mapping[str, Any]]]:
    slots = list(card.slots)
    bout_by_id = {
        _document_id(bout): bout
        for bout in bout_facts["bouts"]
        if _document_id(bout) is not None
    }
    current_slots = _current_slots(slots, bout_by_id)
    if not slots:
        issues.add(
            "SLOTS_MISSING",
            "blocking",
            "STRUCT",
            "event_card_slots",
            (card.spec.event_id,),
            "No card-slot documents were found.",
        )
    active_bout_ids = {_document_id(bout) for bout in bout_facts["active"]}
    current_slot_bout_ids = {slot.get("bout_id") for slot in current_slots}
    missing_slots = sorted(active_bout_ids - current_slot_bout_ids)
    orphan_slots = sorted(current_slot_bout_ids - active_bout_ids)
    if missing_slots:
        issues.add(
            "ACTIVE_BOUT_SLOT_MISSING",
            "error",
            "STRUCT",
            "event_card_slots",
            missing_slots,
            "Active bouts are missing current card slots.",
        )
    if orphan_slots:
        issues.add(
            "SLOT_BOUT_MISMATCH",
            "error",
            "STRUCT",
            "event_card_slots",
            orphan_slots,
            "Current slots do not reference an active known bout.",
        )
    duplicate_slot_bouts = [
        bout_id
        for bout_id, count in Counter(slot.get("bout_id") for slot in slots).items()
        if count > 1
    ]
    if duplicate_slot_bouts:
        issues.add(
            "SLOT_BOUT_DUPLICATE",
            "error",
            "STRUCT",
            "event_card_slots",
            duplicate_slot_bouts,
            "A bout occupies more than one slot.",
        )
    invalid_sections = [
        _document_id(slot)
        for slot in current_slots
        if slot.get("card_section") not in SECTIONS
    ]
    if invalid_sections:
        issues.add(
            "SECTION_UNKNOWN",
            "error",
            "STRUCT",
            "event_card_slots",
            invalid_sections,
            "Current slots contain missing or unknown sections.",
        )
    section_counts = Counter(
        slot.get("card_section")
        for slot in current_slots
        if slot.get("card_section") in SECTIONS
    )
    missing_expected_sections = [
        section for section in card.spec.expected_sections if section_counts[section] == 0
    ]
    if missing_expected_sections:
        issues.add(
            "EXPECTED_SECTION_MISSING",
            "blocking",
            "STRUCT",
            "event_card_slots",
            missing_expected_sections,
            "The observed card does not contain every section required by its golden scenario.",
        )
    unexpected_sections = sorted(
        set(section_counts) - set(card.spec.expected_sections)
    )
    if unexpected_sections:
        issues.add(
            "UNEXPECTED_SECTION_PRESENT",
            "blocking",
            "STRUCT",
            "event_card_slots",
            unexpected_sections,
            "The observed card contains sections outside its approved golden scenario.",
        )
    _validate_orders(
        current_slots,
        "order_overall",
        "ORDER_DUPLICATE",
        "ORDER_GAP",
        issues,
    )
    by_section: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for slot in current_slots:
        if slot.get("card_section") in SECTIONS:
            by_section[slot["card_section"]].append(slot)
    for section in sorted(by_section):
        _validate_orders(
            by_section[section],
            "order_section",
            "SECTION_ORDER_DUPLICATE",
            "SECTION_ORDER_GAP",
            issues,
        )
    mains = [slot for slot in current_slots if _role(slot) == "main_event"]
    co_mains = [slot for slot in current_slots if _role(slot) == "co_main"]
    if len(mains) != 1:
        issues.add(
            "MAIN_EVENT_MISSING" if not mains else "MAIN_EVENT_MULTIPLE",
            "error",
            "STRUCT",
            "event_card_slots",
            (_document_id(slot) for slot in mains),
            "The current card requires exactly one main-event slot.",
            affected_count=len(mains),
        )
    elif not (
        mains[0].get("card_section") == "main"
        and mains[0].get("order_overall") == 1
        and mains[0].get("order_section") == 1
    ):
        issues.add(
            "MAIN_EVENT_INVALID",
            "error",
            "STRUCT",
            "event_card_slots",
            (_document_id(mains[0]),),
            "Main event is not main-section rank 1 overall/section.",
        )
    if len(co_mains) > 1 or any(
        slot.get("card_section") != "main" or slot.get("order_overall") != 2
        for slot in co_mains
    ):
        issues.add(
            "CO_MAIN_INVALID",
            "error",
            "STRUCT",
            "event_card_slots",
            (_document_id(slot) for slot in co_mains),
            "Co-main role is duplicated or not main-section rank 2 overall.",
            affected_count=len(co_mains),
        )
    event_main = (card.event or {}).get("main_event_bout_id")
    slot_main = mains[0].get("bout_id") if len(mains) == 1 else None
    if event_main != slot_main:
        issues.add(
            "EVENT_MAIN_SLOT_MISMATCH",
            "error",
            "STRUCT",
            "events",
            (event_main, slot_main),
            "Event main_event_bout_id disagrees with the current main-event slot.",
        )
    no_current_flag = [
        _document_id(slot) for slot in slots if "is_current" not in slot
    ]
    if no_current_flag:
        issues.add(
            "SLOT_CURRENT_FLAG_MISSING",
            "warning",
            "STRUCT",
            "event_card_slots",
            no_current_flag,
            "Legacy slots cannot distinguish current from historical placement explicitly.",
        )
    no_revision = [
        _document_id(slot)
        for slot in slots
        if not _positive_int(slot.get("structure_revision"))
    ]
    if no_revision:
        issues.add(
            "SLOT_STRUCTURE_REVISION_MISSING",
            "warning",
            "STRUCT",
            "event_card_slots",
            no_revision,
            "Legacy slots are not tied to a structure revision.",
        )
    flat_disagreements = []
    for slot in current_slots:
        bout = bout_by_id.get(slot.get("bout_id"))
        if bout is None:
            continue
        bout_role = (
            "main_event"
            if bout.get("is_main_event") is True
            else "co_main"
            if bout.get("is_co_main_event") is True
            else "regular"
        )
        if (
            bout.get("card_section") != slot.get("card_section")
            or (bout.get("order_overall") or bout.get("card_order"))
            != slot.get("order_overall")
            or bout_role != _role(slot)
        ):
            flat_disagreements.append(slot.get("bout_id"))
    if flat_disagreements:
        issues.add(
            "FLATTENED_STRUCTURE_DISAGREEMENT",
            "error",
            "STRUCT",
            "bouts,event_card_slots",
            flat_disagreements,
            "Flattened bout structure disagrees with event_card_slots.",
        )
    return dict(sorted(section_counts.items())), current_slots


def _audit_title(card: LegacyCardDocuments, issues: _Issues) -> int:
    bouts = list(card.bouts)
    title_bouts = [
        bout
        for bout in bouts
        if bout.get("is_title_fight") is True
        or bout.get("is_bmf_title_fight") is True
    ]
    if len(title_bouts) < card.spec.minimum_title_bouts:
        issues.add(
            "EXPECTED_TITLE_BOUTS_MISSING",
            "blocking",
            "TITLE",
            "bouts",
            (card.spec.event_id,),
            "Production title flags do not satisfy the approved golden-card expectation.",
            affected_count=card.spec.minimum_title_bouts - len(title_bouts),
        )
    missing_type = [
        _document_id(bout)
        for bout in title_bouts
        if bout.get("title_type") not in {
            "undisputed",
            "interim",
            "bmf",
            "other",
            "unknown",
        }
    ]
    if missing_type:
        issues.add(
            "TITLE_TYPE_MISSING",
            "blocking",
            "TITLE",
            "bouts",
            missing_type,
            "Title bouts do not carry a canonical title_type.",
        )
    no_evidence = [
        _document_id(bout)
        for bout in bouts
        if not _mapping(bout.get("evidence")).get("is_title_fight")
    ]
    if no_evidence:
        issues.add(
            "TITLE_EVIDENCE_MISSING",
            "blocking",
            "TITLE",
            "bouts",
            no_evidence,
            "Legacy title booleans cannot distinguish trusted false from unknown without evidence.",
        )
    return len(title_bouts)


def _normalized_method(result: Mapping[str, Any]) -> str:
    value = result.get("method_family") or result.get("method")
    if not _nonempty(value):
        return "missing"
    lowered = value.lower().replace("/", "_").replace("-", "_").replace(" ", "_")
    if "ko" in lowered or "tko" in lowered:
        return "ko_tko"
    if "sub" in lowered:
        return "submission"
    if (
        "decision" in lowered
        or lowered == "dec"
        or lowered.endswith("_dec")
        or lowered in {"ud", "sd", "md"}
    ):
        return "decision"
    if "dq" in lowered or "disqual" in lowered:
        return "dq"
    return "other"


def _audit_results(
    card: LegacyCardDocuments,
    bout_facts: Mapping[str, Any],
    observed_status: str,
    issues: _Issues,
) -> dict[str, int]:
    active = list(bout_facts["active"])
    with_results = list(bout_facts["with_results"])
    if observed_status in {"scheduled", "live", "postponed"} and not with_results:
        return {}
    missing_results = [
        _document_id(bout)
        for bout in active
        if bout.get("status") == "completed" and not _result(bout)
    ]
    if observed_status == "completed":
        missing_results.extend(
            _document_id(bout)
            for bout in active
            if bout.get("status") != "completed" or not _result(bout)
        )
    missing_results = sorted(set(missing_results))
    if missing_results:
        issues.add(
            "RESULT_NOT_FINAL",
            "blocking",
            "RES",
            "bouts",
            missing_results,
            "Completed scope contains active bouts without a stored final result.",
        )
    invalid_winner = []
    invalid_round = []
    invalid_method = []
    unversioned = []
    legacy_time = []
    method_counts = Counter()
    for bout in with_results:
        result = _result(bout)
        method = _normalized_method(result)
        method_counts[method] += 1
        outcome = result.get("outcome")
        winner = result.get("winner") or result.get("winner_fighter_id")
        if outcome in {"draw", "nc", "no_contest"}:
            if winner not in {None, "draw", "nc"}:
                invalid_winner.append(_document_id(bout))
        elif winner not in {"red", "blue"} and not _nonempty(
            result.get("winner_fighter_id")
        ):
            invalid_winner.append(_document_id(bout))
        round_number = result.get("ending_round") or result.get("round")
        scheduled = bout.get("scheduled_rounds") or bout.get("rounds_scheduled")
        if round_number is not None and (
            not _positive_int(round_number)
            or not _positive_int(scheduled)
            or round_number > scheduled
        ):
            invalid_round.append(_document_id(bout))
        if method == "missing":
            invalid_method.append(_document_id(bout))
        if not _positive_int(bout.get("result_revision")) or result.get(
            "revision"
        ) != bout.get("result_revision"):
            unversioned.append(_document_id(bout))
        if "ending_time_seconds" not in result and result.get("time") is not None:
            legacy_time.append(_document_id(bout))
    if invalid_winner:
        issues.add(
            "RESULT_WINNER_INVALID",
            "error",
            "RES",
            "bouts",
            invalid_winner,
            "Stored result winner/outcome cannot be resolved canonically.",
        )
    if invalid_round:
        issues.add(
            "RESULT_ROUND_INVALID",
            "error",
            "RES",
            "bouts",
            invalid_round,
            "Stored result round is missing or exceeds scheduled rounds.",
        )
    if invalid_method:
        issues.add(
            "RESULT_METHOD_UNKNOWN",
            "blocking",
            "RES",
            "bouts",
            invalid_method,
            "Stored result method is missing.",
        )
    if unversioned:
        issues.add(
            "RESULT_REVISION_MISSING",
            "warning",
            "RES",
            "bouts",
            unversioned,
            "Legacy results are usable but not versioned for correction/retry safety.",
        )
    if legacy_time:
        issues.add(
            "RESULT_TIME_NOT_NORMALIZED",
            "warning",
            "RES",
            "bouts",
            legacy_time,
            "Result time is still a legacy string instead of seconds within round.",
        )
    if with_results and not (card.event or {}).get("first_final_result_at"):
        issues.add(
            "FIRST_FINAL_RESULT_AT_MISSING",
            "warning",
            "RES",
            "events",
            (card.spec.event_id,),
            "The first final-result boundary needed for mission locking is not persisted.",
        )
    return dict(sorted(method_counts.items()))


def _capability_audits(
    issues: Sequence[ProductionAuditIssue],
    observed_status: str,
    result_count: int,
) -> tuple[ProductionCapabilityAudit, ...]:
    audits = []
    severity_rank = {"info": 0, "warning": 1, "blocking": 2, "error": 3}
    for capability in CAPABILITIES:
        capability_issues = [issue for issue in issues if issue.capability == capability]
        maximum = max(
            (severity_rank.get(issue.severity, 3) for issue in capability_issues),
            default=0,
        )
        if capability == "RES" and observed_status in {
            "scheduled",
            "live",
            "postponed",
        } and result_count == 0 and maximum == 0:
            state = "pending"
        elif maximum >= 3:
            state = "quarantined"
        elif maximum == 2:
            state = "blocked"
        elif maximum == 1:
            state = "degraded"
        else:
            state = "ready"
        audits.append(
            ProductionCapabilityAudit(
                capability=capability,
                state=state,
                issue_codes=tuple(sorted({issue.code for issue in capability_issues})),
            )
        )
    return tuple(audits)


def audit_legacy_card(card: LegacyCardDocuments) -> ProductionCardAudit:
    """Audit projected documents without mutation or source access."""

    issues = _Issues()
    observed_status, observed_date, name_matches = _audit_event(card, issues)
    bout_facts = _audit_bouts(card, issues)
    eligible_candidates = _audit_eligibility(card, bout_facts, issues)
    section_counts, current_slots = _audit_structure(card, bout_facts, issues)
    title_count = _audit_title(card, issues)
    result_method_counts = _audit_results(
        card, bout_facts, observed_status, issues
    )
    final_issues = issues.finish()
    capabilities = _capability_audits(
        final_issues, observed_status, len(bout_facts["with_results"])
    )
    counts = (
        ("bouts", len(card.bouts)),
        ("active_bouts", len(bout_facts["active"])),
        ("cancelled_bouts", len(bout_facts["cancelled"])),
        ("completed_bouts", len(bout_facts["completed"])),
        ("bouts_with_results", len(bout_facts["with_results"])),
        ("slots", len(card.slots)),
        ("current_slots_inferred", len(current_slots)),
        ("resolved_fighter_refs", bout_facts["resolved_fighter_refs"]),
        ("fighter_ref_total", bout_facts["fighter_ref_total"]),
        ("candidate_eligible_bouts", eligible_candidates),
        ("title_bouts", title_count),
    )
    return ProductionCardAudit(
        event_id=card.spec.event_id,
        expected_name=card.spec.expected_name,
        observed_name_matches=name_matches,
        scenario_tags=card.spec.scenario_tags,
        event_found=card.event is not None,
        observed_status=observed_status,
        observed_date=observed_date,
        counts=counts,
        section_counts=tuple(sorted(section_counts.items())),
        result_method_counts=tuple(sorted(result_method_counts.items())),
        capabilities=capabilities,
        issues=final_issues,
    )


def audit_legacy_cards(
    cards: Iterable[LegacyCardDocuments],
) -> ProductionCardAuditReport:
    audits = sorted(
        (audit_legacy_card(card) for card in cards), key=lambda card: card.event_id
    )
    return ProductionCardAuditReport(tuple(audits))


def fetch_allowlisted_cards(
    database: Any,
    specs: Sequence[GoldenCardSpec] = GOLDEN_CARD_SPECS,
) -> tuple[LegacyCardDocuments, ...]:
    """Execute only projected reads for explicitly allowlisted event IDs."""

    unknown_ids = sorted({spec.event_id for spec in specs} - set(GOLDEN_EVENT_IDS))
    if unknown_ids:
        raise ProductionAuditConfigurationError(
            "Audit specs include event IDs outside the approved Q-014 allowlist."
        )
    cards = []
    for spec in sorted(specs, key=lambda item: item.event_id):
        event = database["events"].find_one(
            {"id": spec.event_id}, EVENT_PROJECTION, max_time_ms=10_000
        )
        bout_cursor = database["bouts"].find(
            {"event_id": spec.event_id}, BOUT_PROJECTION
        )
        slot_cursor = database["event_card_slots"].find(
            {"event_id": spec.event_id}, SLOT_PROJECTION
        )
        bouts = tuple(bout_cursor.sort("id", 1).max_time_ms(10_000))
        slots = tuple(slot_cursor.sort("bout_id", 1).max_time_ms(10_000))
        cards.append(
            LegacyCardDocuments(
                spec=spec,
                event=event,
                bouts=bouts,
                slots=slots,
            )
        )
    return tuple(cards)


def load_mongo_settings(env_file: Optional[Path] = None) -> tuple[str, str]:
    """Load credentials into memory without ever returning them in a report."""

    values: Mapping[str, Any] = {}
    if env_file is not None:
        if not env_file.is_file():
            raise ProductionAuditConfigurationError(
                "The explicit environment file does not exist."
            )
        values = dotenv_values(env_file)
    uri = os.environ.get("MONGODB_URI") or values.get("MONGODB_URI")
    database_name = (
        os.environ.get("MONGODB_DB_NAME")
        or values.get("MONGODB_DB_NAME")
        or "ufc_picks"
    )
    if not _nonempty(uri):
        raise ProductionAuditConfigurationError(
            "MONGODB_URI is not configured; provide it through the process environment "
            "or an explicit --env-file."
        )
    if not _nonempty(database_name):
        raise ProductionAuditConfigurationError("MONGODB_DB_NAME is invalid.")
    return uri, database_name


def run_production_audit(
    uri: str,
    database_name: str,
    specs: Sequence[GoldenCardSpec] = GOLDEN_CARD_SPECS,
) -> ProductionCardAuditReport:
    """Open a bounded majority-read connection and return only sanitized findings."""

    from pymongo import MongoClient, ReadPreference
    from pymongo.read_concern import ReadConcern

    client = MongoClient(
        uri,
        appname="ufc-picks-production-card-audit",
        connectTimeoutMS=10_000,
        serverSelectionTimeoutMS=10_000,
        retryWrites=False,
        read_preference=ReadPreference.PRIMARY,
    )
    try:
        database = client.get_database(
            database_name,
            read_concern=ReadConcern("majority"),
        )
        cards = fetch_allowlisted_cards(database, specs)
        return audit_legacy_cards(cards)
    finally:
        client.close()


def render_json(report: ProductionCardAuditReport) -> str:
    return json.dumps(
        report.as_dict(), ensure_ascii=False, indent=2, sort_keys=True, default=str
    ) + "\n"


def _markdown(value: Any) -> str:
    if value is None:
        return "—"
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def render_markdown(report: ProductionCardAuditReport) -> str:
    summary = report.summary()
    lines = [
        "# Golden Production Card Audit",
        "",
        f"- Schema: `{AUDIT_SCHEMA_VERSION}`",
        "- Source: `production MongoDB / strict read-only`",
        f"- Result: `{'PASS' if report.passed else 'FINDINGS'}`",
        f"- Allowlist: `{', '.join(str(value) for value in GOLDEN_EVENT_IDS)}`",
        f"- Collections: `{', '.join(AUDITED_COLLECTIONS)}`",
        "- Raw documents retained: `NO`",
        "- Credentials included: `NO`",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|---|---:|",
        f"| Cards | {summary['card_count']} |",
        f"| Events found | {summary['event_found_count']} |",
        f"| Cards with blocking findings | {summary['cards_with_blocking_findings']} |",
        f"| Bout documents | {summary['document_counts']['bouts']} |",
        f"| Slot documents | {summary['document_counts']['event_card_slots']} |",
        "",
        "## Capability states",
        "",
        "| Capability | Ready | Pending | Degraded | Blocked | Quarantined |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for capability in CAPABILITIES:
        counts = summary["capability_state_counts"][capability]
        lines.append(
            f"| {capability} | {counts['ready']} | {counts['pending']} | "
            f"{counts['degraded']} | {counts['blocked']} | {counts['quarantined']} |"
        )

    lines.extend(["", "## Cards"])
    for card in report.cards:
        counts = dict(card.counts)
        capability_states = ", ".join(
            f"{item.capability}={item.state}" for item in card.capabilities
        )
        lines.extend(
            [
                "",
                f"### {card.event_id} — {_markdown(card.expected_name)}",
                "",
                f"- Scenarios: `{', '.join(card.scenario_tags)}`",
                f"- Event found/name matched: `{'YES' if card.event_found else 'NO'}` / "
                f"`{'YES' if card.observed_name_matches else 'NO'}`",
                f"- Status/date: `{_markdown(card.observed_status)}` / "
                f"`{_markdown(card.observed_date)}`",
                f"- Capabilities: `{capability_states}`",
                f"- Documents: bouts={counts['bouts']}, slots={counts['slots']}, "
                f"active={counts['active_bouts']}, cancelled={counts['cancelled_bouts']}, "
                f"results={counts['bouts_with_results']}",
                f"- Mission facts: eligible candidates={counts['candidate_eligible_bouts']}, "
                f"resolved fighter refs={counts['resolved_fighter_refs']}/"
                f"{counts['fighter_ref_total']}, title bouts={counts['title_bouts']}",
                f"- Sections: `{dict(card.section_counts)}`",
                f"- Result methods: `{dict(card.result_method_counts)}`",
                "",
                "| Severity | Capability | Code | Collection | Affected | Examples |",
                "|---|---|---|---|---:|---|",
            ]
        )
        if card.issues:
            for issue in card.issues:
                examples = ", ".join(issue.example_ids) or "—"
                lines.append(
                    f"| {issue.severity} | {issue.capability} | {issue.code} | "
                    f"{issue.collection} | {issue.affected_count} | {_markdown(examples)} |"
                )
        else:
            lines.append("| — | — | No findings | — | 0 | — |")

    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "This is an aggregated legacy-readiness inventory, not a CardData snapshot "
            "normalizer and not a reconciliation/write preview. `degraded` means the fact "
            "is usable but lacks V1 authority/versioning; `blocked` means a required fact is "
            "absent; `quarantined` means observed values contradict an invariant.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit the approved Q-014 production cards using projected, strict read-only "
            "MongoDB queries and emit only aggregate/sanitized findings."
        )
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional dotenv file. The URI is never printed or included in reports.",
    )
    parser.add_argument(
        "--event-id",
        type=int,
        action="append",
        choices=GOLDEN_EVENT_IDS,
        default=None,
        help="Audit an allowlisted card; repeatable. Default: all Q-014 cards.",
    )
    parser.add_argument(
        "--format", choices=("json", "markdown"), default="markdown"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Explicit sanitized report path; default writes only to stdout.",
    )
    return parser


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.output is not None and args.env_file is not None:
            if args.output.resolve() == args.env_file.resolve():
                raise ProductionAuditConfigurationError(
                    "The report path must not overwrite the environment file."
                )
        selected_ids = set(args.event_id or GOLDEN_EVENT_IDS)
        specs = tuple(
            spec for spec in GOLDEN_CARD_SPECS if spec.event_id in selected_ids
        )
        uri, database_name = load_mongo_settings(args.env_file)
        report = run_production_audit(uri, database_name, specs)
        rendered = render_json(report) if args.format == "json" else render_markdown(report)
        if args.output is None:
            stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        return report.exit_code
    except ProductionAuditConfigurationError as exc:
        stderr.write(f"error: {exc}\n")
        return EXIT_CONFIGURATION_ERROR
    except Exception as exc:  # Never echo driver errors; they may contain connection details.
        stderr.write(
            "error: production read-only audit failed "
            f"({type(exc).__name__}); no raw data was retained.\n"
        )
        return EXIT_CONFIGURATION_ERROR


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA_VERSION",
    "AUDITED_COLLECTIONS",
    "BOUT_PROJECTION",
    "EVENT_PROJECTION",
    "GOLDEN_CARD_SPECS",
    "GOLDEN_EVENT_IDS",
    "SLOT_PROJECTION",
    "GoldenCardSpec",
    "LegacyCardDocuments",
    "ProductionAuditConfigurationError",
    "ProductionCardAuditReport",
    "audit_legacy_card",
    "audit_legacy_cards",
    "fetch_allowlisted_cards",
    "load_mongo_settings",
    "main",
    "render_json",
    "render_markdown",
    "run_production_audit",
]
