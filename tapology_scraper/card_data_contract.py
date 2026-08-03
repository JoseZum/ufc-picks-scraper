"""Pure validation for the CardDataContractV1 snapshot boundary.

This module deliberately does not normalize source payloads, reconcile two
snapshots, or persist anything.  It accepts an already-normalized in-memory
mapping and reports deterministic contract issues without mutating the input.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


CONTRACT_VERSION = "card-data/v1"

EVENT_STATUSES = {"scheduled", "live", "completed", "cancelled", "postponed"}
BOUT_STATUSES = EVENT_STATUSES | {"replaced"}
CARD_SECTIONS = {"early_prelim", "prelim", "main"}
SLOT_ROLES = {"main_event", "co_main", "regular"}
TITLE_TYPES = {"none", "undisputed", "interim", "bmf", "other", "unknown"}
RESULT_STATUSES = {"final", "corrected"}
RESULT_OUTCOMES = {"red_win", "blue_win", "draw", "no_contest"}
RESULT_METHODS = {"ko_tko", "submission", "decision", "dq", "other"}
ELIGIBILITY_KINDS = {
    "current",
    "mission_assignment",
    "card_streak_pick_close",
    "monthly_event_set",
}
QUALITY_OVERALL_STATES = {"ready", "degraded", "quarantined"}
CAPABILITY_STATES = {"ready", "pending", "blocked", "quarantined"}
CAPABILITIES = ("EVT", "EVT_DATE", "BOUT", "ELIG", "STRUCT", "TITLE", "RES")

_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)


@dataclass(frozen=True)
class ContractIssue:
    """One deterministic, actionable violation of CardDataContractV1."""

    code: str
    severity: str
    scope_type: str
    scope_id: Optional[str]
    field: str
    message: str
    blocks_capabilities: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["blocks_capabilities"] = list(self.blocks_capabilities)
        return value


@dataclass(frozen=True)
class ContractValidation:
    """Immutable validation result returned by :func:`validate_card_data_v1`."""

    issues: tuple[ContractIssue, ...]

    @property
    def is_valid(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(issue.code for issue in self.issues)

    def as_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "issues": [issue.as_dict() for issue in self.issues],
        }


class CardDataValidationError(ValueError):
    """Raised by the strict validation boundary when a snapshot is invalid."""

    def __init__(self, validation: ContractValidation):
        self.validation = validation
        codes = ", ".join(validation.codes) or "unknown"
        super().__init__(f"Invalid {CONTRACT_VERSION} snapshot: {codes}")


class _Issues:
    def __init__(self) -> None:
        self._items: list[ContractIssue] = []

    def add(
        self,
        code: str,
        scope_type: str,
        scope_id: Any,
        field: str,
        message: str,
        blocks: Sequence[str],
        severity: str = "error",
    ) -> None:
        self._items.append(
            ContractIssue(
                code=code,
                severity=severity,
                scope_type=scope_type,
                scope_id=None if scope_id is None else str(scope_id),
                field=field,
                message=message,
                blocks_capabilities=tuple(sorted(set(blocks))),
            )
        )

    def finish(self) -> ContractValidation:
        unique = set(self._items)
        ordered = sorted(
            unique,
            key=lambda item: (
                0 if item.severity == "error" else 1,
                item.scope_type,
                "" if item.scope_id is None else item.scope_id,
                item.field,
                item.code,
                item.message,
                item.blocks_capabilities,
            ),
        )
        return ContractValidation(tuple(ordered))


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


def _is_list(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _positive_int(value: Any) -> bool:
    return _is_int(value) and value >= 1


def _nonnegative_int(value: Any) -> bool:
    return _is_int(value) and value >= 0


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _utc_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return parsed


def _iso_date(value: Any) -> Optional[date]:
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _require_mapping(
    parent: Mapping[str, Any],
    field: str,
    issues: _Issues,
    scope_type: str,
    scope_id: Any,
    blocks: Sequence[str],
) -> Optional[Mapping[str, Any]]:
    value = parent.get(field)
    if not _is_mapping(value):
        issues.add(
            "FIELD_REQUIRED",
            scope_type,
            scope_id,
            field,
            f"{field} must be an object.",
            blocks,
        )
        return None
    return value


def _require_list(
    parent: Mapping[str, Any],
    field: str,
    issues: _Issues,
    scope_type: str,
    scope_id: Any,
    blocks: Sequence[str],
) -> Optional[Sequence[Any]]:
    value = parent.get(field)
    if not _is_list(value):
        issues.add(
            "FIELD_REQUIRED",
            scope_type,
            scope_id,
            field,
            f"{field} must be an array.",
            blocks,
        )
        return None
    return value


def _validate_datetime_field(
    parent: Mapping[str, Any],
    field: str,
    issues: _Issues,
    scope_type: str,
    scope_id: Any,
    blocks: Sequence[str],
    *,
    nullable: bool = False,
) -> Optional[datetime]:
    value = parent.get(field)
    if nullable and value is None:
        return None
    parsed = _utc_datetime(value)
    if parsed is None:
        issues.add(
            "DATETIME_INVALID",
            scope_type,
            scope_id,
            field,
            f"{field} must be an aware UTC ISO-8601 datetime.",
            blocks,
        )
    return parsed


def _validate_revision(
    parent: Mapping[str, Any],
    field: str,
    minimum: int,
    issues: _Issues,
    scope_type: str,
    scope_id: Any,
    blocks: Sequence[str],
) -> None:
    value = parent.get(field)
    if not _is_int(value) or value < minimum:
        issues.add(
            "REVISION_INVALID",
            scope_type,
            scope_id,
            field,
            f"{field} must be an integer >= {minimum}.",
            blocks,
        )


def _validate_source_run(snapshot: Mapping[str, Any], issues: _Issues) -> None:
    source_run = _require_mapping(
        snapshot, "source_run", issues, "snapshot", snapshot.get("snapshot_id"), ("EVT",)
    )
    if source_run is None:
        return
    if not _nonempty_string(source_run.get("run_id")):
        issues.add(
            "FIELD_REQUIRED", "source_run", None, "run_id", "run_id is required.", ("EVT",)
        )
    _validate_datetime_field(
        source_run, "observed_at", issues, "source_run", source_run.get("run_id"), ("EVT",)
    )
    previous = source_run.get("previous_snapshot_revision")
    if previous is not None and not _nonnegative_int(previous):
        issues.add(
            "REVISION_INVALID",
            "source_run",
            source_run.get("run_id"),
            "previous_snapshot_revision",
            "previous_snapshot_revision must be null or an integer >= 0.",
            ("EVT",),
        )
    sources = _require_list(
        source_run, "sources", issues, "source_run", source_run.get("run_id"), ("EVT",)
    )
    if sources is None:
        return
    for index, source in enumerate(sources):
        if not _is_mapping(source):
            issues.add(
                "FIELD_REQUIRED",
                "source_run",
                source_run.get("run_id"),
                f"sources[{index}]",
                "Each source observation must be an object.",
                ("EVT",),
            )
            continue
        for field in ("source", "source_event_id", "payload_hash"):
            if not _nonempty_string(source.get(field)):
                issues.add(
                    "FIELD_REQUIRED",
                    "source_run",
                    source_run.get("run_id"),
                    f"sources[{index}].{field}",
                    f"{field} is required for every source observation.",
                    ("EVT",),
                )
        _validate_datetime_field(
            source,
            "observed_at",
            issues,
            "source_run",
            source_run.get("run_id"),
            ("EVT",),
        )


def _validate_event(
    snapshot: Mapping[str, Any], issues: _Issues
) -> tuple[Optional[Mapping[str, Any]], Optional[int]]:
    event = _require_mapping(
        snapshot, "event", issues, "snapshot", snapshot.get("snapshot_id"), ("EVT",)
    )
    if event is None:
        return None, None

    event_id = event.get("event_id")
    if not _positive_int(event_id):
        issues.add(
            "EVENT_ID_INVALID",
            "event",
            event_id,
            "event_id",
            "event_id must be a positive integer.",
            ("EVT",),
        )
        canonical_event_id = None
    else:
        canonical_event_id = event_id

    source_ids = event.get("source_ids")
    if not _is_mapping(source_ids) or any(
        not isinstance(value, str) for value in source_ids.values()
    ):
        issues.add(
            "FIELD_REQUIRED",
            "event",
            event_id,
            "source_ids",
            "source_ids must be an object whose values are strings.",
            ("EVT",),
        )
    for field in ("promotion", "name"):
        if not _nonempty_string(event.get(field)):
            issues.add(
                "FIELD_REQUIRED",
                "event",
                event_id,
                field,
                f"{field} is required.",
                ("EVT",),
            )
    if event.get("status") not in EVENT_STATUSES:
        issues.add(
            "EVENT_STATUS_INVALID",
            "event",
            event_id,
            "status",
            f"status must be one of {sorted(EVENT_STATUSES)}.",
            ("EVT", "RES"),
        )

    official_date = _iso_date(event.get("official_event_date"))
    if official_date is None:
        issues.add(
            "OFFICIAL_DATE_MISSING",
            "event",
            event_id,
            "official_event_date",
            "official_event_date must be an ISO date.",
            ("EVT_DATE",),
        )
    elif event.get("month_key") != official_date.isoformat()[:7]:
        issues.add(
            "MONTH_KEY_MISMATCH",
            "event",
            event_id,
            "month_key",
            "month_key must match official_event_date.",
            ("EVT_DATE",),
        )

    timezone_name = event.get("official_date_timezone")
    try:
        if not _nonempty_string(timezone_name):
            raise ZoneInfoNotFoundError
        ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        issues.add(
            "TIMEZONE_AMBIGUOUS",
            "event",
            event_id,
            "official_date_timezone",
            "official_date_timezone must be a valid IANA timezone.",
            ("EVT_DATE",),
        )

    for field in ("created_at", "canonical_updated_at"):
        _validate_datetime_field(event, field, issues, "event", event_id, ("EVT",))
    for field in ("card_start_time_utc", "picks_lock_time_utc"):
        _validate_datetime_field(
            event, field, issues, "event", event_id, ("STRUCT",), nullable=True
        )
    _validate_datetime_field(
        event,
        "first_final_result_at",
        issues,
        "event",
        event_id,
        ("RES",),
        nullable=True,
    )
    for field in ("lifecycle_revision", "structure_revision", "timing_revision"):
        _validate_revision(event, field, 1, issues, "event", event_id, ("EVT",))
    for field in ("listed_bout_count", "mission_eligible_bout_count"):
        if not _nonnegative_int(event.get(field)):
            issues.add(
                "COUNT_MISMATCH",
                "event",
                event_id,
                field,
                f"{field} must be an integer >= 0.",
                ("ELIG", "STRUCT"),
            )

    _validate_event_section_times(event, issues)
    return event, canonical_event_id


def _validate_event_section_times(event: Mapping[str, Any], issues: _Issues) -> None:
    event_id = event.get("event_id")
    starts = event.get("section_start_times_utc")
    locks = event.get("section_lock_times_utc")
    parsed_starts: dict[str, datetime] = {}
    parsed_locks: dict[str, datetime] = {}

    for field, value, parsed_values in (
        ("section_start_times_utc", starts, parsed_starts),
        ("section_lock_times_utc", locks, parsed_locks),
    ):
        if not _is_mapping(value):
            issues.add(
                "FIELD_REQUIRED",
                "event",
                event_id,
                field,
                f"{field} must be an object.",
                ("STRUCT",),
            )
            continue
        for section, raw_time in value.items():
            if section not in CARD_SECTIONS:
                issues.add(
                    "SECTION_UNKNOWN",
                    "event",
                    event_id,
                    f"{field}.{section}",
                    f"Unknown card section {section!r}.",
                    ("STRUCT",),
                )
                continue
            parsed = _utc_datetime(raw_time)
            if parsed is None:
                issues.add(
                    "DATETIME_INVALID",
                    "event",
                    event_id,
                    f"{field}.{section}",
                    "Section times must be aware UTC ISO-8601 datetimes.",
                    ("STRUCT",),
                )
            else:
                parsed_values[section] = parsed

    for section in sorted(parsed_starts.keys() & parsed_locks.keys()):
        if parsed_locks[section] > parsed_starts[section]:
            issues.add(
                "LOCK_AFTER_START",
                "event",
                event_id,
                f"section_lock_times_utc.{section}",
                f"The {section} lock occurs after its section start.",
                ("STRUCT",),
            )


def _index_bouts(
    snapshot: Mapping[str, Any], event_id: Optional[int], issues: _Issues
) -> tuple[list[Mapping[str, Any]], dict[int, Mapping[str, Any]]]:
    raw_bouts = _require_list(
        snapshot, "bouts", issues, "snapshot", snapshot.get("snapshot_id"), ("BOUT",)
    )
    if raw_bouts is None:
        return [], {}
    bouts: list[Mapping[str, Any]] = []
    by_id: dict[int, Mapping[str, Any]] = {}
    for index, raw_bout in enumerate(raw_bouts):
        if not _is_mapping(raw_bout):
            issues.add(
                "FIELD_REQUIRED",
                "snapshot",
                snapshot.get("snapshot_id"),
                f"bouts[{index}]",
                "Each bout must be an object.",
                ("BOUT",),
            )
            continue
        bouts.append(raw_bout)
        bout_id = raw_bout.get("bout_id")
        if not _positive_int(bout_id):
            issues.add(
                "BOUT_ID_AMBIGUOUS",
                "bout",
                bout_id,
                "bout_id",
                "bout_id must be a positive integer.",
                ("BOUT",),
            )
            continue
        if bout_id in by_id:
            issues.add(
                "BOUT_ID_AMBIGUOUS",
                "bout",
                bout_id,
                "bout_id",
                "bout_id must be unique within a snapshot.",
                ("BOUT",),
            )
        else:
            by_id[bout_id] = raw_bout
        if event_id is not None and raw_bout.get("event_id") != event_id:
            issues.add(
                "SLOT_BOUT_MISMATCH",
                "bout",
                bout_id,
                "event_id",
                "Bout event_id must match the snapshot event.",
                ("BOUT", "STRUCT"),
            )
    return bouts, by_id


def _validate_bouts(
    bouts: Sequence[Mapping[str, Any]],
    by_id: Mapping[int, Mapping[str, Any]],
    issues: _Issues,
) -> None:
    for bout in bouts:
        bout_id = bout.get("bout_id")
        if bout.get("status") not in BOUT_STATUSES:
            issues.add(
                "BOUT_STATUS_INVALID",
                "bout",
                bout_id,
                "status",
                f"status must be one of {sorted(BOUT_STATUSES)}.",
                ("BOUT", "RES"),
            )
        for field in ("lineage_id",):
            if not _nonempty_string(bout.get(field)):
                issues.add(
                    "FIELD_REQUIRED",
                    "bout",
                    bout_id,
                    field,
                    f"{field} is required.",
                    ("BOUT",),
                )
        _validate_revision(bout, "matchup_revision", 1, issues, "bout", bout_id, ("BOUT",))
        _validate_revision(bout, "result_revision", 0, issues, "bout", bout_id, ("RES",))
        _validate_datetime_field(
            bout,
            "source_updated_at",
            issues,
            "bout",
            bout_id,
            ("BOUT",),
            nullable=True,
        )
        _validate_datetime_field(
            bout, "canonical_updated_at", issues, "bout", bout_id, ("BOUT",)
        )

        fighters = _validate_fighters(bout, issues)
        if bout.get("scheduled_rounds") not in {3, 5}:
            issues.add(
                "ROUNDS_INVALID",
                "bout",
                bout_id,
                "scheduled_rounds",
                "scheduled_rounds must be 3 or 5.",
                ("BOUT", "RES"),
            )
        _validate_title(bout, issues)
        _validate_result(bout, fighters, issues)
        _validate_replacement_links(bout, by_id, issues)


def _validate_fighters(
    bout: Mapping[str, Any], issues: _Issues
) -> list[Mapping[str, Any]]:
    bout_id = bout.get("bout_id")
    raw_fighters = bout.get("fighters")
    if not _is_list(raw_fighters) or len(raw_fighters) != 2:
        issues.add(
            "FIGHTER_SET_INVALID",
            "bout",
            bout_id,
            "fighters",
            "fighters must contain exactly two fighter references.",
            ("BOUT",),
        )
        return []
    fighters = [fighter for fighter in raw_fighters if _is_mapping(fighter)]
    if len(fighters) != 2:
        issues.add(
            "FIGHTER_SET_INVALID",
            "bout",
            bout_id,
            "fighters",
            "Every fighter reference must be an object.",
            ("BOUT",),
        )
        return fighters
    corners = [fighter.get("corner") for fighter in fighters]
    if Counter(corners) != Counter({"red": 1, "blue": 1}):
        issues.add(
            "FIGHTER_SET_INVALID",
            "bout",
            bout_id,
            "fighters.corner",
            "A matchup requires exactly one red and one blue corner.",
            ("BOUT",),
        )
    fighter_ids = []
    for index, fighter in enumerate(fighters):
        fighter_id = fighter.get("fighter_id")
        if not _nonempty_string(fighter_id):
            issues.add(
                "FIGHTER_ID_UNRESOLVED",
                "bout",
                bout_id,
                f"fighters[{index}].fighter_id",
                "Both fighter IDs must be resolved for CardData readiness.",
                ("BOUT", "ELIG"),
            )
        else:
            fighter_ids.append(fighter_id)
        if not _nonempty_string(fighter.get("display_name")):
            issues.add(
                "FIELD_REQUIRED",
                "bout",
                bout_id,
                f"fighters[{index}].display_name",
                "display_name is required for historical UI context.",
                ("BOUT",),
            )
        if fighter.get("identity_confidence") not in {
            "exact_source",
            "resolved_alias",
            "admin_resolved",
            "unresolved",
        }:
            issues.add(
                "FIGHTER_ID_UNRESOLVED",
                "bout",
                bout_id,
                f"fighters[{index}].identity_confidence",
                "identity_confidence is missing or unknown.",
                ("BOUT", "ELIG"),
            )
    if len(fighter_ids) == 2 and fighter_ids[0] == fighter_ids[1]:
        issues.add(
            "FIGHTER_SET_INVALID",
            "bout",
            bout_id,
            "fighters.fighter_id",
            "Resolved fighter IDs must be distinct.",
            ("BOUT", "ELIG"),
        )
    return fighters


def _validate_title(bout: Mapping[str, Any], issues: _Issues) -> None:
    bout_id = bout.get("bout_id")
    is_title = bout.get("is_title_fight")
    title_type = bout.get("title_type")
    if (
        not (is_title is True or is_title is False or is_title is None)
        or title_type not in TITLE_TYPES
    ):
        issues.add(
            "TITLE_TYPE_CONFLICT",
            "bout",
            bout_id,
            "title_type",
            "Title status/type values are outside the V1 enums.",
            ("TITLE",),
        )
        return
    valid_pair = (
        (is_title is False and title_type == "none")
        or (is_title is True and title_type != "none")
        or (is_title is None and title_type == "unknown")
    )
    if not valid_pair:
        issues.add(
            "TITLE_TYPE_CONFLICT",
            "bout",
            bout_id,
            "title_type",
            "is_title_fight and title_type contradict each other.",
            ("TITLE",),
        )


def _validate_result(
    bout: Mapping[str, Any],
    fighters: Sequence[Mapping[str, Any]],
    issues: _Issues,
) -> None:
    bout_id = bout.get("bout_id")
    result = bout.get("result")
    if result is None:
        if bout.get("status") == "completed":
            issues.add(
                "RESULT_NOT_FINAL",
                "bout",
                bout_id,
                "result",
                "A completed bout requires a canonical final result.",
                ("RES",),
            )
        return
    if not _is_mapping(result):
        issues.add(
            "RESULT_NOT_FINAL",
            "bout",
            bout_id,
            "result",
            "result must be null or a ResultV1 object.",
            ("RES",),
        )
        return
    revision = result.get("revision")
    if not _positive_int(revision) or revision != bout.get("result_revision"):
        issues.add(
            "RESULT_REVISION_MISMATCH",
            "bout",
            bout_id,
            "result.revision",
            "Result revision must be >= 1 and equal bout.result_revision.",
            ("RES",),
        )
    if result.get("status") not in RESULT_STATUSES:
        issues.add(
            "RESULT_NOT_FINAL",
            "bout",
            bout_id,
            "result.status",
            "Canonical result status must be final or corrected.",
            ("RES",),
        )
    if bout.get("status") != "completed":
        issues.add(
            "RESULT_NOT_FINAL",
            "bout",
            bout_id,
            "status",
            "A bout carrying a canonical result must be completed.",
            ("RES",),
        )

    outcome = result.get("outcome")
    if outcome not in RESULT_OUTCOMES:
        issues.add(
            "RESULT_WINNER_INVALID",
            "bout",
            bout_id,
            "result.outcome",
            f"outcome must be one of {sorted(RESULT_OUTCOMES)}.",
            ("RES",),
        )
    by_corner = {
        fighter.get("corner"): fighter.get("fighter_id")
        for fighter in fighters
        if fighter.get("corner") in {"red", "blue"}
    }
    expected_winner = {
        "red_win": by_corner.get("red"),
        "blue_win": by_corner.get("blue"),
    }.get(outcome)
    winner = result.get("winner_fighter_id")
    winner_valid = (
        outcome in {"red_win", "blue_win"}
        and _nonempty_string(expected_winner)
        and winner == expected_winner
    ) or (outcome in {"draw", "no_contest"} and winner is None)
    if outcome in RESULT_OUTCOMES and not winner_valid:
        issues.add(
            "RESULT_WINNER_INVALID",
            "bout",
            bout_id,
            "result.winner_fighter_id",
            "Winner must match the outcome corner, or be null for draw/no contest.",
            ("RES",),
        )
    if result.get("method_family") not in RESULT_METHODS:
        issues.add(
            "RESULT_METHOD_UNKNOWN",
            "bout",
            bout_id,
            "result.method_family",
            f"method_family must be one of {sorted(RESULT_METHODS)}.",
            ("RES",),
        )
    round_number = result.get("ending_round")
    scheduled_rounds = bout.get("scheduled_rounds")
    if round_number is not None and (
        not _positive_int(round_number)
        or not _positive_int(scheduled_rounds)
        or round_number > scheduled_rounds
    ):
        issues.add(
            "RESULT_ROUND_INVALID",
            "bout",
            bout_id,
            "result.ending_round",
            "ending_round must be within the scheduled round count.",
            ("RES",),
        )
    ending_time = result.get("ending_time_seconds")
    if ending_time is not None and (
        not _nonnegative_int(ending_time) or ending_time > 300
    ):
        issues.add(
            "RESULT_TIME_INVALID",
            "bout",
            bout_id,
            "result.ending_time_seconds",
            "ending_time_seconds must be between 0 and 300.",
            ("RES",),
        )
    _validate_datetime_field(
        result, "recorded_at", issues, "bout", bout_id, ("RES",)
    )
    _validate_datetime_field(
        result,
        "source_updated_at",
        issues,
        "bout",
        bout_id,
        ("RES",),
        nullable=True,
    )
    _validate_datetime_field(
        result,
        "corrected_at",
        issues,
        "bout",
        bout_id,
        ("RES",),
        nullable=True,
    )
    if result.get("status") == "corrected" and result.get("corrected_at") is None:
        issues.add(
            "RESULT_NOT_FINAL",
            "bout",
            bout_id,
            "result.corrected_at",
            "A corrected result requires corrected_at.",
            ("RES",),
        )


def _validate_replacement_links(
    bout: Mapping[str, Any],
    by_id: Mapping[int, Mapping[str, Any]],
    issues: _Issues,
) -> None:
    bout_id = bout.get("bout_id")
    for field, reverse_field, revision_delta in (
        ("replaces_bout_id", "replaced_by_bout_id", 1),
        ("replaced_by_bout_id", "replaces_bout_id", -1),
    ):
        related_id = bout.get(field)
        if related_id is None:
            continue
        related = by_id.get(related_id) if _is_int(related_id) else None
        expected_revision = (
            related.get("matchup_revision") + revision_delta
            if related is not None and _is_int(related.get("matchup_revision"))
            else None
        )
        if (
            related is None
            or related.get(reverse_field) != bout_id
            or related.get("lineage_id") != bout.get("lineage_id")
            or bout.get("matchup_revision") != expected_revision
        ):
            issues.add(
                "RELATIONSHIP_INVALID",
                "bout",
                bout_id,
                field,
                "Replacement links must be reciprocal, same-lineage and one revision apart.",
                ("BOUT", "ELIG"),
            )


def _index_slots(
    snapshot: Mapping[str, Any],
    event_id: Optional[int],
    by_bout_id: Mapping[int, Mapping[str, Any]],
    issues: _Issues,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    raw_slots = _require_list(
        snapshot,
        "card_slots",
        issues,
        "snapshot",
        snapshot.get("snapshot_id"),
        ("STRUCT",),
    )
    if raw_slots is None:
        return [], []
    slots: list[Mapping[str, Any]] = []
    current: list[Mapping[str, Any]] = []
    slot_ids: set[str] = set()
    slot_bout_ids: set[int] = set()
    for index, raw_slot in enumerate(raw_slots):
        if not _is_mapping(raw_slot):
            issues.add(
                "FIELD_REQUIRED",
                "snapshot",
                snapshot.get("snapshot_id"),
                f"card_slots[{index}]",
                "Each card slot must be an object.",
                ("STRUCT",),
            )
            continue
        slot = raw_slot
        slots.append(slot)
        slot_id = slot.get("slot_id")
        bout_id = slot.get("bout_id")
        if not _nonempty_string(slot_id) or slot_id in slot_ids:
            issues.add(
                "SLOT_BOUT_MISMATCH",
                "slot",
                slot_id,
                "slot_id",
                "slot_id must be non-empty and unique.",
                ("STRUCT",),
            )
        else:
            slot_ids.add(slot_id)
        if _is_int(bout_id) and bout_id in slot_bout_ids:
            issues.add(
                "SLOT_BOUT_MISMATCH",
                "slot",
                slot_id,
                "bout_id",
                "A bout may have only one slot in an event snapshot.",
                ("STRUCT",),
            )
        elif _is_int(bout_id):
            slot_bout_ids.add(bout_id)
        expected_slot_id = (
            f"{event_id}:{bout_id}"
            if event_id is not None and _positive_int(bout_id)
            else None
        )
        if (
            event_id is None
            or slot.get("event_id") != event_id
            or bout_id not in by_bout_id
            or slot_id != expected_slot_id
            or (
                bout_id in by_bout_id
                and by_bout_id[bout_id].get("event_id") != slot.get("event_id")
            )
        ):
            issues.add(
                "SLOT_BOUT_MISMATCH",
                "slot",
                slot_id,
                "bout_id",
                "Slot ID/event/bout relationships must reference the same known event and bout.",
                ("BOUT", "STRUCT"),
            )
        if not isinstance(slot.get("is_current"), bool):
            issues.add(
                "FIELD_REQUIRED",
                "slot",
                slot_id,
                "is_current",
                "is_current must be a boolean.",
                ("STRUCT",),
            )
        if slot.get("card_section") not in CARD_SECTIONS:
            issues.add(
                "SECTION_UNKNOWN",
                "slot",
                slot_id,
                "card_section",
                "card_section must be early_prelim, prelim or main.",
                ("STRUCT",),
            )
        if slot.get("role") not in SLOT_ROLES:
            issues.add(
                "HEADLINE_ROLE_INVALID",
                "slot",
                slot_id,
                "role",
                f"role must be one of {sorted(SLOT_ROLES)}.",
                ("STRUCT",),
            )
        _validate_revision(
            slot, "structure_revision", 1, issues, "slot", slot_id, ("STRUCT",)
        )
        start = _validate_datetime_field(
            slot,
            "scheduled_start_time_utc",
            issues,
            "slot",
            slot_id,
            ("STRUCT",),
            nullable=True,
        )
        lock = _validate_datetime_field(
            slot,
            "automatic_lock_time_utc",
            issues,
            "slot",
            slot_id,
            ("STRUCT",),
            nullable=True,
        )
        if start is not None and lock is not None and lock > start:
            issues.add(
                "LOCK_AFTER_START",
                "slot",
                slot_id,
                "automatic_lock_time_utc",
                "The automatic lock occurs after the scheduled start.",
                ("STRUCT",),
            )
        if slot.get("is_current") is True:
            current.append(slot)
            bout = by_bout_id.get(bout_id)
            if bout is not None and bout.get("status") in {"cancelled", "replaced"}:
                issues.add(
                    "SLOT_BOUT_MISMATCH",
                    "slot",
                    slot_id,
                    "is_current",
                    "Cancelled or replaced bouts cannot occupy a current slot.",
                    ("ELIG", "STRUCT"),
                )
    return slots, current


def _validate_current_structure(
    event: Optional[Mapping[str, Any]],
    current_slots: Sequence[Mapping[str, Any]],
    issues: _Issues,
) -> None:
    event_id = event.get("event_id") if event is not None else None
    _validate_contiguous_orders(current_slots, "order_overall", "ORDER", issues)
    by_section: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for slot in current_slots:
        by_section[slot.get("card_section")].append(slot)
    for section in sorted(by_section, key=lambda value: str(value)):
        _validate_contiguous_orders(
            by_section[section], "order_section", "SECTION_ORDER", issues
        )

    mains = [slot for slot in current_slots if slot.get("role") == "main_event"]
    co_mains = [slot for slot in current_slots if slot.get("role") == "co_main"]
    if current_slots and not mains:
        issues.add(
            "MAIN_EVENT_MISSING",
            "event",
            event_id,
            "main_event_bout_id",
            "A non-empty current card requires exactly one main event.",
            ("STRUCT",),
        )
    if len(mains) > 1:
        issues.add(
            "MAIN_EVENT_MULTIPLE",
            "event",
            event_id,
            "main_event_bout_id",
            "A card may have only one current main event.",
            ("STRUCT",),
        )
    for main in mains:
        if not (
            main.get("card_section") == "main"
            and main.get("order_overall") == 1
            and main.get("order_section") == 1
        ):
            issues.add(
                "MAIN_EVENT_INVALID",
                "slot",
                main.get("slot_id"),
                "role",
                "The main event must be main section and rank 1 overall/section.",
                ("STRUCT",),
            )
    if len(co_mains) > 1:
        issues.add(
            "CO_MAIN_INVALID",
            "event",
            event_id,
            "card_slots.role",
            "A card may have at most one co-main event.",
            ("STRUCT",),
        )
    for co_main in co_mains:
        if not (
            co_main.get("card_section") == "main"
            and co_main.get("order_overall") == 2
            and (not mains or co_main.get("bout_id") != mains[0].get("bout_id"))
        ):
            issues.add(
                "CO_MAIN_INVALID",
                "slot",
                co_main.get("slot_id"),
                "role",
                "The co-main must be a distinct main-section bout ranked 2 overall.",
                ("STRUCT",),
            )

    if event is not None:
        expected_main_id = mains[0].get("bout_id") if len(mains) == 1 else None
        if event.get("main_event_bout_id") != expected_main_id:
            issues.add(
                "MAIN_EVENT_INVALID",
                "event",
                event_id,
                "main_event_bout_id",
                "main_event_bout_id must match the unique current main-event slot.",
                ("STRUCT",),
            )
        if event.get("listed_bout_count") != len(current_slots):
            issues.add(
                "COUNT_MISMATCH",
                "event",
                event_id,
                "listed_bout_count",
                "listed_bout_count must equal the number of current slots.",
                ("STRUCT",),
            )
        _validate_aggregate_timing(event, current_slots, issues)


def _validate_contiguous_orders(
    slots: Sequence[Mapping[str, Any]],
    field: str,
    code_prefix: str,
    issues: _Issues,
) -> None:
    valid_orders: list[int] = []
    for slot in slots:
        value = slot.get(field)
        if not _positive_int(value):
            issues.add(
                "ORDER_MISSING",
                "slot",
                slot.get("slot_id"),
                field,
                f"Current slot {field} must be a positive integer.",
                ("STRUCT",),
            )
        else:
            valid_orders.append(value)
    duplicates = sorted(order for order, count in Counter(valid_orders).items() if count > 1)
    if duplicates:
        issues.add(
            f"{code_prefix}_DUPLICATE",
            "card_structure",
            None,
            field,
            f"Duplicate {field} values: {duplicates}.",
            ("STRUCT",),
        )
    expected = list(range(1, len(slots) + 1))
    if sorted(set(valid_orders)) != expected:
        issues.add(
            f"{code_prefix}_GAP",
            "card_structure",
            None,
            field,
            f"Current {field} values must be contiguous 1..{len(slots)}.",
            ("STRUCT",),
        )


def _validate_aggregate_timing(
    event: Mapping[str, Any],
    current_slots: Sequence[Mapping[str, Any]],
    issues: _Issues,
) -> None:
    event_id = event.get("event_id")
    starts = [
        parsed
        for parsed in (
            _utc_datetime(slot.get("scheduled_start_time_utc")) for slot in current_slots
        )
        if parsed is not None
    ]
    locks = [
        parsed
        for parsed in (
            _utc_datetime(slot.get("automatic_lock_time_utc")) for slot in current_slots
        )
        if parsed is not None
    ]
    event_start = _utc_datetime(event.get("card_start_time_utc"))
    event_lock = _utc_datetime(event.get("picks_lock_time_utc"))
    if (min(starts) if starts else None) != event_start:
        issues.add(
            "SECTION_TIME_MISSING",
            "event",
            event_id,
            "card_start_time_utc",
            "card_start_time_utc must equal the earliest known current slot start.",
            ("STRUCT",),
        )
    if (min(locks) if locks else None) != event_lock:
        issues.add(
            "SECTION_TIME_MISSING",
            "event",
            event_id,
            "picks_lock_time_utc",
            "picks_lock_time_utc must equal the earliest known current slot lock.",
            ("STRUCT",),
        )


def _validate_eligibility(
    snapshot: Mapping[str, Any],
    event: Optional[Mapping[str, Any]],
    by_bout_id: Mapping[int, Mapping[str, Any]],
    current_slots: Sequence[Mapping[str, Any]],
    issues: _Issues,
) -> None:
    eligibility = _require_mapping(
        snapshot,
        "current_eligibility",
        issues,
        "snapshot",
        snapshot.get("snapshot_id"),
        ("ELIG",),
    )
    if eligibility is None:
        return
    event_id = event.get("event_id") if event is not None else None
    if not _nonempty_string(eligibility.get("eligibility_snapshot_id")):
        issues.add(
            "FIELD_REQUIRED",
            "eligibility",
            None,
            "eligibility_snapshot_id",
            "eligibility_snapshot_id is required.",
            ("ELIG",),
        )
    if eligibility.get("kind") not in ELIGIBILITY_KINDS:
        issues.add(
            "ELIGIBILITY_TARGET_INVALID",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "kind",
            f"kind must be one of {sorted(ELIGIBILITY_KINDS)}.",
            ("ELIG",),
        )
    elif eligibility.get("kind") != "current":
        issues.add(
            "ELIGIBILITY_TARGET_INVALID",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "kind",
            "The snapshot current_eligibility field must use kind='current'.",
            ("ELIG",),
        )
    if eligibility.get("event_id") != event_id:
        issues.add(
            "ELIGIBILITY_TARGET_INVALID",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "event_id",
            "Eligibility event_id must match the snapshot event.",
            ("ELIG",),
        )
    if eligibility.get("card_snapshot_revision") != snapshot.get("snapshot_revision"):
        issues.add(
            "ELIGIBILITY_TARGET_INVALID",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "card_snapshot_revision",
            "Eligibility must reference the containing snapshot revision.",
            ("ELIG",),
        )
    _validate_datetime_field(
        eligibility,
        "created_at",
        issues,
        "eligibility",
        eligibility.get("eligibility_snapshot_id"),
        ("ELIG",),
    )
    if not _nonempty_string(eligibility.get("fingerprint")):
        issues.add(
            "FIELD_REQUIRED",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "fingerprint",
            "A deterministic eligibility fingerprint is required.",
            ("ELIG",),
        )

    eligible = _require_list(
        eligibility,
        "eligible_targets",
        issues,
        "eligibility",
        eligibility.get("eligibility_snapshot_id"),
        ("ELIG",),
    )
    excluded = _require_list(
        eligibility,
        "excluded_targets",
        issues,
        "eligibility",
        eligibility.get("eligibility_snapshot_id"),
        ("ELIG",),
    )
    eligible_keys = _validate_targets(
        eligible or [], by_bout_id, current_slots, issues, excluded=False
    )
    excluded_keys = _validate_targets(
        excluded or [], by_bout_id, current_slots, issues, excluded=True
    )
    overlap = sorted(eligible_keys & excluded_keys)
    if overlap:
        issues.add(
            "ELIGIBILITY_OVERLAP",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "eligible_targets",
            f"Targets cannot be both eligible and excluded: {overlap}.",
            ("ELIG",),
        )
    denominator = eligibility.get("denominator")
    if not _nonnegative_int(denominator) or denominator != len(eligible_keys):
        issues.add(
            "ELIGIBILITY_DENOMINATOR_MISMATCH",
            "eligibility",
            eligibility.get("eligibility_snapshot_id"),
            "denominator",
            "denominator must equal the number of unique eligible targets.",
            ("ELIG",),
        )
    if event is not None and event.get("mission_eligible_bout_count") != denominator:
        issues.add(
            "COUNT_MISMATCH",
            "event",
            event_id,
            "mission_eligible_bout_count",
            "mission_eligible_bout_count must equal the eligibility denominator.",
            ("ELIG",),
        )


def _validate_targets(
    targets: Sequence[Any],
    by_bout_id: Mapping[int, Mapping[str, Any]],
    current_slots: Sequence[Mapping[str, Any]],
    issues: _Issues,
    *,
    excluded: bool,
) -> set[tuple[int, int]]:
    current_slot_by_bout_id = {slot.get("bout_id"): slot for slot in current_slots}
    current_bout_ids = set(current_slot_by_bout_id)
    keys: list[tuple[int, int]] = []
    for index, target in enumerate(targets):
        if not _is_mapping(target):
            issues.add(
                "ELIGIBILITY_TARGET_INVALID",
                "eligibility",
                None,
                f"{'excluded' if excluded else 'eligible'}_targets[{index}]",
                "Each eligibility target must be an object.",
                ("ELIG",),
            )
            continue
        bout_id = target.get("bout_id")
        revision = target.get("matchup_revision")
        if not _positive_int(bout_id) or not _positive_int(revision):
            issues.add(
                "ELIGIBILITY_TARGET_INVALID",
                "bout",
                bout_id,
                "matchup_revision",
                "Eligibility targets require positive bout and matchup revision IDs.",
                ("ELIG",),
            )
            continue
        key = (bout_id, revision)
        keys.append(key)
        bout = by_bout_id.get(bout_id)
        is_valid_reference = (
            bout is not None
            and bout.get("matchup_revision") == revision
            and (excluded or bout_id in current_bout_ids)
        )
        if not is_valid_reference:
            issues.add(
                "ELIGIBILITY_TARGET_INVALID",
                "bout",
                bout_id,
                "matchup_revision",
                "Eligibility target must reference the exact known matchup revision; eligible targets must be current.",
                ("ELIG",),
            )
        elif not excluded:
            fighters = bout.get("fighters")
            resolved_fighter_ids = (
                [fighter.get("fighter_id") for fighter in fighters]
                if _is_list(fighters)
                and len(fighters) == 2
                and all(_is_mapping(fighter) for fighter in fighters)
                else []
            )
            slot = current_slot_by_bout_id[bout_id]
            if (
                bout.get("status") in {"cancelled", "postponed", "replaced"}
                or len(resolved_fighter_ids) != 2
                or any(not _nonempty_string(value) for value in resolved_fighter_ids)
                or len(set(resolved_fighter_ids)) != 2
                or _utc_datetime(slot.get("scheduled_start_time_utc")) is None
                or _utc_datetime(slot.get("automatic_lock_time_utc")) is None
            ):
                issues.add(
                    "ELIGIBILITY_TARGET_INVALID",
                    "bout",
                    bout_id,
                    "eligible_targets",
                    "Eligible targets must be active, current, resolved and have usable timing/lock data.",
                    ("ELIG",),
                )
        if excluded:
            reasons = target.get("reason_codes")
            if not _is_list(reasons) or not reasons or any(
                not _nonempty_string(reason) for reason in reasons
            ):
                issues.add(
                    "ELIGIBILITY_TARGET_INVALID",
                    "bout",
                    bout_id,
                    "reason_codes",
                    "Excluded targets require at least one non-empty reason code.",
                    ("ELIG",),
                )
    duplicate_keys = sorted(key for key, count in Counter(keys).items() if count > 1)
    if duplicate_keys:
        issues.add(
            "ELIGIBILITY_DUPLICATE",
            "eligibility",
            None,
            "excluded_targets" if excluded else "eligible_targets",
            f"Duplicate eligibility targets: {duplicate_keys}.",
            ("ELIG",),
        )
    if not excluded and keys != sorted(keys):
        issues.add(
            "ELIGIBILITY_ORDER_INVALID",
            "eligibility",
            None,
            "eligible_targets",
            "Eligible targets must use canonical bout/revision order.",
            ("ELIG",),
        )
    return set(keys)


def _validate_quality(snapshot: Mapping[str, Any], issues: _Issues) -> None:
    quality = _require_mapping(
        snapshot,
        "quality",
        issues,
        "snapshot",
        snapshot.get("snapshot_id"),
        CAPABILITIES,
    )
    if quality is None:
        return
    if quality.get("overall") not in QUALITY_OVERALL_STATES:
        issues.add(
            "QUALITY_STATE_INVALID",
            "quality",
            None,
            "overall",
            f"overall must be one of {sorted(QUALITY_OVERALL_STATES)}.",
            CAPABILITIES,
        )
    capabilities = quality.get("capabilities")
    if not _is_mapping(capabilities):
        issues.add(
            "QUALITY_STATE_INVALID",
            "quality",
            None,
            "capabilities",
            "capabilities must be an object.",
            CAPABILITIES,
        )
    else:
        for capability in CAPABILITIES:
            if capabilities.get(capability) not in CAPABILITY_STATES:
                issues.add(
                    "QUALITY_STATE_INVALID",
                    "quality",
                    None,
                    f"capabilities.{capability}",
                    f"Capability {capability} requires a valid readiness state.",
                    (capability,),
                )
        for capability in sorted(set(capabilities) - set(CAPABILITIES)):
            issues.add(
                "QUALITY_STATE_INVALID",
                "quality",
                None,
                f"capabilities.{capability}",
                f"Unknown CardData capability {capability!r}.",
                CAPABILITIES,
            )
    declared_issues = quality.get("issues")
    if not _is_list(declared_issues):
        issues.add(
            "QUALITY_STATE_INVALID",
            "quality",
            None,
            "issues",
            "quality.issues must be an array.",
            CAPABILITIES,
        )
        return
    for index, declared in enumerate(declared_issues):
        if not _is_mapping(declared):
            issues.add(
                "QUALITY_STATE_INVALID",
                "quality",
                None,
                f"issues[{index}]",
                "Each declared quality issue must be an object.",
                CAPABILITIES,
            )
            continue
        required_strings = ("code", "severity", "scope_type", "field", "message")
        if any(not _nonempty_string(declared.get(field)) for field in required_strings):
            issues.add(
                "QUALITY_STATE_INVALID",
                "quality",
                None,
                f"issues[{index}]",
                "Declared quality issues require code, severity, scope_type, field and message.",
                CAPABILITIES,
            )
        blocks = declared.get("blocks_capabilities")
        if not _is_list(blocks) or any(block not in CAPABILITIES for block in blocks):
            issues.add(
                "QUALITY_STATE_INVALID",
                "quality",
                None,
                f"issues[{index}].blocks_capabilities",
                "Declared issue capability blocks must use CardData capability IDs.",
                CAPABILITIES,
            )
        if declared.get("severity") in {"error", "blocking"} and not blocks:
            issues.add(
                "QUALITY_STATE_INVALID",
                "quality",
                None,
                f"issues[{index}].blocks_capabilities",
                "Every blocking quality issue must name blocked capabilities.",
                CAPABILITIES,
            )


def _validate_cross_entity_lifecycle(
    event: Optional[Mapping[str, Any]],
    by_bout_id: Mapping[int, Mapping[str, Any]],
    current_slots: Sequence[Mapping[str, Any]],
    issues: _Issues,
) -> None:
    if event is None:
        return
    event_id = event.get("event_id")
    current_bouts = [
        by_bout_id.get(slot.get("bout_id"))
        for slot in current_slots
        if slot.get("bout_id") in by_bout_id
    ]
    if event.get("status") == "completed":
        final_count = sum(
            _is_mapping(bout.get("result"))
            and bout.get("result", {}).get("status") in RESULT_STATUSES
            for bout in current_bouts
        )
        every_current_completed = bool(current_bouts) and all(
            bout.get("status") == "completed" for bout in current_bouts
        )
        if final_count < 1 or not every_current_completed:
            issues.add(
                "EVENT_COMPLETION_INCOMPLETE",
                "event",
                event_id,
                "status",
                "A completed event needs at least one final result and every current bout completed.",
                ("EVT", "RES"),
            )

    current_by_lineage: dict[Any, int] = Counter(
        bout.get("lineage_id") for bout in current_bouts if bout is not None
    )
    for lineage_id, count in sorted(current_by_lineage.items(), key=lambda item: str(item[0])):
        if count > 1:
            issues.add(
                "FIGHTER_SET_INVALID",
                "lineage",
                lineage_id,
                "lineage_id",
                "Only one bout per lineage may occupy a current slot.",
                ("BOUT", "ELIG"),
            )


def validate_card_data_v1(snapshot: Mapping[str, Any]) -> ContractValidation:
    """Validate one normalized snapshot without mutation or side effects.

    The returned issue tuple is de-duplicated and sorted by stable fields so the
    same semantic input produces byte-for-byte equivalent validation output.
    """

    issues = _Issues()
    if not _is_mapping(snapshot):
        issues.add(
            "FIELD_REQUIRED",
            "snapshot",
            None,
            "$",
            "CardDataContractV1 input must be an object.",
            CAPABILITIES,
        )
        return issues.finish()

    if snapshot.get("contract_version") != CONTRACT_VERSION:
        issues.add(
            "CONTRACT_VERSION_UNSUPPORTED",
            "snapshot",
            snapshot.get("snapshot_id"),
            "contract_version",
            f"contract_version must equal {CONTRACT_VERSION!r}.",
            CAPABILITIES,
        )
    normalizer_version = snapshot.get("normalizer_version")
    if not isinstance(normalizer_version, str) or not _SEMVER_RE.fullmatch(
        normalizer_version
    ):
        issues.add(
            "NORMALIZER_VERSION_INVALID",
            "snapshot",
            snapshot.get("snapshot_id"),
            "normalizer_version",
            "normalizer_version must be a semantic version string.",
            CAPABILITIES,
        )
    revision = snapshot.get("snapshot_revision")
    if not _positive_int(revision):
        issues.add(
            "REVISION_INVALID",
            "snapshot",
            snapshot.get("snapshot_id"),
            "snapshot_revision",
            "snapshot_revision must be an integer >= 1.",
            CAPABILITIES,
        )
    _validate_datetime_field(
        snapshot,
        "generated_at",
        issues,
        "snapshot",
        snapshot.get("snapshot_id"),
        CAPABILITIES,
    )
    _validate_source_run(snapshot, issues)
    event, event_id = _validate_event(snapshot, issues)
    if event_id is not None and _positive_int(revision):
        expected_snapshot_id = f"{event_id}:{revision}"
        if snapshot.get("snapshot_id") != expected_snapshot_id:
            issues.add(
                "SNAPSHOT_ID_MISMATCH",
                "snapshot",
                snapshot.get("snapshot_id"),
                "snapshot_id",
                f"snapshot_id must equal {expected_snapshot_id!r}.",
                CAPABILITIES,
            )

    bouts, by_bout_id = _index_bouts(snapshot, event_id, issues)
    _validate_bouts(bouts, by_bout_id, issues)
    _, current_slots = _index_slots(snapshot, event_id, by_bout_id, issues)
    _validate_current_structure(event, current_slots, issues)
    _validate_eligibility(snapshot, event, by_bout_id, current_slots, issues)
    _validate_quality(snapshot, issues)
    _validate_cross_entity_lifecycle(event, by_bout_id, current_slots, issues)
    return issues.finish()


def validate_card_data_v1_or_raise(snapshot: Mapping[str, Any]) -> None:
    """Raise :class:`CardDataValidationError` for an invalid snapshot."""

    validation = validate_card_data_v1(snapshot)
    if not validation.is_valid:
        raise CardDataValidationError(validation)


__all__ = [
    "CAPABILITIES",
    "CAPABILITY_STATES",
    "CONTRACT_VERSION",
    "CardDataValidationError",
    "ContractIssue",
    "ContractValidation",
    "validate_card_data_v1",
    "validate_card_data_v1_or_raise",
]
