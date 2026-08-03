"""Pure CardData V1 observation normalizer.

This module is the SCR-008 boundary between source-specific adapters and
future persistence.  It accepts already-sanitized observations plus an
optional previous CardData snapshot and returns a deterministic desired
snapshot, semantic change set, and quarantines.  It has no database, network,
Scrapy, or application-writer dependency.

The core intentionally does not allocate canonical IDs.  An adapter must
provide the UFC Picks event/bout IDs it is proposing.  Identity evidence that
is unsafe (date-only event matching, names-only bout matching, or a changed
fighter set reusing an existing bout ID) is quarantined rather than guessed.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Optional

from tapology_scraper.card_data_contract import CAPABILITIES, CONTRACT_VERSION


NORMALIZER_VERSION = "1.0.0"
ELIGIBILITY_RULES_VERSION = "card-eligibility/v1"

SOURCE_RANKS = {
    "admin_override": 500,
    "espn_detail": 400,
    "espn_summary": 350,
    "tapology_explicit": 300,
    "time_group_inference": 200,
    "deterministic_fallback": 100,
    "derived": 50,
}
SOURCE_CONFIDENCE = {
    "admin_override": "high",
    "espn_detail": "high",
    "espn_summary": "high",
    "tapology_explicit": "medium",
    "time_group_inference": "medium",
    "deterministic_fallback": "quarantined",
    "derived": "high",
}
ENTITY_TYPES = {"event", "bout", "slot", "result"}
IDENTITY_BASES = {
    "canonical_id",
    "source_alias",
    "date_and_name",
    "date_only",
    "fighter_ids",
    "source_competition_id",
    "names_only",
    "admin",
}
TITLE_FIELDS = ("is_title_fight", "is_bmf_title_fight", "title_type")
TIMING_EVENT_FIELDS = (
    "official_event_date",
    "official_date_timezone",
    "card_start_time_utc",
    "picks_lock_time_utc",
    "section_start_times_utc",
    "section_lock_times_utc",
)
EVENT_FIELDS = (
    "promotion",
    "name",
    "subtitle",
    "official_event_date",
    "official_date_timezone",
    "status",
    "card_start_time_utc",
    "picks_lock_time_utc",
    "section_start_times_utc",
    "section_lock_times_utc",
    "first_final_result_at",
)
DERIVED_EVENT_FIELDS = {"card_start_time_utc", "picks_lock_time_utc"}
BOUT_FIELDS = (
    "lineage_id",
    "replaces_bout_id",
    "replaced_by_bout_id",
    "status",
    "fighters",
    "weight_class",
    "gender",
    "scheduled_rounds",
)
SLOT_FIELDS = (
    "is_current",
    "card_section",
    "order_overall",
    "scheduled_start_time_utc",
    "automatic_lock_time_utc",
)
NESTED_MAP_FIELDS = {
    "event": ("source_ids", "section_start_times_utc", "section_lock_times_utc"),
    "bout": ("source_ids",),
    "slot": (),
}


class NormalizationInputError(ValueError):
    """Raised when the caller violates the typed observation boundary."""


@dataclass(frozen=True)
class CardDataObservation:
    """One normalized fact proposal from a source-specific adapter."""

    observation_id: str
    source_kind: str
    observed_at: str
    event_id: int
    entity_type: str
    entity_id: int
    source_ref: str
    source_event_id: str
    values: Mapping[str, Any]
    clear_fields: tuple[str, ...] = ()
    identity_basis: str = "canonical_id"
    reason: Optional[str] = None
    payload_hash: Optional[str] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CardDataObservation":
        if not isinstance(value, Mapping):
            raise NormalizationInputError("Every observation must be an object.")
        clear_fields = value.get("clear_fields", ())
        if not _is_sequence(clear_fields):
            raise NormalizationInputError("clear_fields must be an array of strings.")
        values = value.get("values", {})
        if not isinstance(values, Mapping):
            raise NormalizationInputError("values must be an object.")
        return cls(
            observation_id=value.get("observation_id"),
            source_kind=value.get("source_kind"),
            observed_at=value.get("observed_at"),
            event_id=value.get("event_id"),
            entity_type=value.get("entity_type"),
            entity_id=value.get("entity_id"),
            source_ref=value.get("source_ref"),
            source_event_id=value.get("source_event_id"),
            values=copy.deepcopy(dict(values)),
            clear_fields=tuple(clear_fields),
            identity_basis=value.get("identity_basis", "canonical_id"),
            reason=value.get("reason"),
            payload_hash=value.get("payload_hash"),
        )

    def validate(self) -> None:
        if not _nonempty(self.observation_id):
            raise NormalizationInputError("observation_id must be a non-empty string.")
        if self.source_kind not in SOURCE_RANKS:
            raise NormalizationInputError(
                f"Unsupported source_kind {self.source_kind!r}."
            )
        if _parse_utc(self.observed_at) is None:
            raise NormalizationInputError("observed_at must be an aware UTC datetime.")
        if not _positive_int(self.event_id):
            raise NormalizationInputError("event_id must be a positive integer.")
        if self.entity_type not in ENTITY_TYPES:
            raise NormalizationInputError(
                f"Unsupported entity_type {self.entity_type!r}."
            )
        if not _positive_int(self.entity_id):
            raise NormalizationInputError("entity_id must be a positive integer.")
        if self.entity_type == "event" and self.entity_id != self.event_id:
            raise NormalizationInputError(
                "An event observation entity_id must equal event_id."
            )
        if not _nonempty(self.source_ref):
            raise NormalizationInputError("source_ref must be a non-empty string.")
        if not _nonempty(self.source_event_id):
            raise NormalizationInputError(
                "source_event_id must be a non-empty source identifier."
            )
        if self.identity_basis not in IDENTITY_BASES:
            raise NormalizationInputError(
                f"Unsupported identity_basis {self.identity_basis!r}."
            )
        if any(not _nonempty(item) for item in self.clear_fields):
            raise NormalizationInputError(
                "clear_fields must contain only non-empty strings."
            )
        if self.source_kind in {
            "admin_override",
            "time_group_inference",
            "deterministic_fallback",
        } and not _nonempty(self.reason):
            raise NormalizationInputError(
                f"{self.source_kind} observations require a reason."
            )

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["values"] = copy.deepcopy(dict(self.values))
        value["clear_fields"] = list(self.clear_fields)
        return value


@dataclass(frozen=True)
class Quarantine:
    """A deterministic conflict that the normalizer refused to guess through."""

    code: str
    observation_ids: tuple[str, ...]
    entity_type: str
    entity_id: Optional[int]
    field: str
    message: str
    blocks_capabilities: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["observation_ids"] = list(self.observation_ids)
        value["blocks_capabilities"] = list(self.blocks_capabilities)
        return value

    def as_quality_issue(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": "error",
            "scope_type": self.entity_type,
            "scope_id": None if self.entity_id is None else str(self.entity_id),
            "field": self.field,
            "message": self.message,
            "blocks_capabilities": list(self.blocks_capabilities),
        }


@dataclass(frozen=True)
class NormalizationResult:
    """Immutable result envelope for the pure normalizer."""

    snapshot: Mapping[str, Any]
    change_set: Mapping[str, Any]
    quarantines: tuple[Quarantine, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot": copy.deepcopy(dict(self.snapshot)),
            "change_set": copy.deepcopy(dict(self.change_set)),
            "quarantines": [item.as_dict() for item in self.quarantines],
        }


@dataclass(frozen=True)
class _Candidate:
    observation: CardDataObservation
    value: Any
    cleared: bool = False


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _parse_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return parsed


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    digest = hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _observation_sort_key(item: CardDataObservation) -> tuple[Any, ...]:
    return (
        item.event_id,
        item.entity_type,
        item.entity_id,
        item.observed_at,
        item.source_kind,
        item.observation_id,
    )


def _evidence(item: CardDataObservation, *, cleared: bool = False) -> dict[str, Any]:
    value = {
        "observation_id": item.observation_id,
        "source_kind": item.source_kind,
        "confidence": SOURCE_CONFIDENCE[item.source_kind],
        "observed_at": item.observed_at,
        "source_ref": item.source_ref,
        "reason": item.reason,
    }
    if cleared:
        value["cleared"] = True
    return value


def _evidence_rank(value: Any) -> int:
    if not isinstance(value, Mapping):
        return 0
    return SOURCE_RANKS.get(value.get("source_kind"), 0)


def _evidence_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, Mapping):
        return None
    return _parse_utc(value.get("observed_at"))


def _quarantine(
    items: list[Quarantine],
    code: str,
    observations: Sequence[CardDataObservation],
    entity_type: str,
    entity_id: Optional[int],
    field: str,
    message: str,
    capabilities: Sequence[str],
) -> None:
    items.append(
        Quarantine(
            code=code,
            observation_ids=tuple(
                sorted({item.observation_id for item in observations})
            ),
            entity_type=entity_type,
            entity_id=entity_id,
            field=field,
            message=message,
            blocks_capabilities=tuple(sorted(set(capabilities))),
        )
    )


def _select_candidate(
    candidates: Sequence[_Candidate],
    previous_evidence: Any,
    previous_value: Any,
    quarantines: list[Quarantine],
    entity_type: str,
    entity_id: int,
    field: str,
    capabilities: Sequence[str],
) -> Optional[_Candidate]:
    if not candidates:
        return None
    highest_rank = max(SOURCE_RANKS[item.observation.source_kind] for item in candidates)
    ranked = [
        item
        for item in candidates
        if SOURCE_RANKS[item.observation.source_kind] == highest_rank
    ]
    latest_at = max(_parse_utc(item.observation.observed_at) for item in ranked)
    latest = [
        item
        for item in ranked
        if _parse_utc(item.observation.observed_at) == latest_at
    ]
    distinct = {"__clear__" if item.cleared else _canonical(item.value) for item in latest}
    if len(distinct) > 1:
        _quarantine(
            quarantines,
            "SOURCE_VALUE_CONFLICT",
            [item.observation for item in latest],
            entity_type,
            entity_id,
            field,
            "Equal-rank observations at the same time propose incompatible values.",
            capabilities,
        )
        earlier = [
            item
            for item in ranked
            if _parse_utc(item.observation.observed_at) < latest_at
        ]
        return _select_candidate(
            earlier,
            previous_evidence,
            previous_value,
            quarantines,
            entity_type,
            entity_id,
            field,
            capabilities,
        )
    selected = sorted(latest, key=lambda item: item.observation.observation_id)[0]

    prior_rank = _evidence_rank(previous_evidence)
    prior_time = _evidence_time(previous_evidence)
    selected_rank = SOURCE_RANKS[selected.observation.source_kind]
    selected_time = _parse_utc(selected.observation.observed_at)
    if selected_rank < prior_rank:
        return None
    if selected_rank == prior_rank and prior_time is not None:
        if selected_time < prior_time:
            _quarantine(
                quarantines,
                "STALE_OBSERVATION",
                [selected.observation],
                entity_type,
                entity_id,
                field,
                "An older observation cannot replace the current same-rank winner.",
                capabilities,
            )
            return None
        if selected_time == prior_time:
            if (
                isinstance(previous_evidence, Mapping)
                and previous_evidence.get("observation_id")
                == selected.observation.observation_id
            ):
                return selected
            selected_value = None if selected.cleared else selected.value
            if _canonical(selected_value) != _canonical(previous_value):
                _quarantine(
                    quarantines,
                    "SOURCE_VALUE_CONFLICT",
                    [selected.observation],
                    entity_type,
                    entity_id,
                    field,
                    "The same source rank and timestamp conflict with prior evidence.",
                    capabilities,
                )
                return None
    return selected


def _resolve_field(
    target: dict[str, Any],
    evidence: dict[str, Any],
    observations: Sequence[CardDataObservation],
    field: str,
    quarantines: list[Quarantine],
    entity_type: str,
    entity_id: int,
    capabilities: Sequence[str],
) -> None:
    candidates = []
    for item in observations:
        if field in item.clear_fields:
            candidates.append(_Candidate(item, None, True))
        elif field in item.values and item.values[field] is not None:
            candidates.append(_Candidate(item, copy.deepcopy(item.values[field])))
    selected = _select_candidate(
        candidates,
        evidence.get(field),
        target.get(field),
        quarantines,
        entity_type,
        entity_id,
        field,
        capabilities,
    )
    if selected is None:
        return
    target[field] = None if selected.cleared else copy.deepcopy(selected.value)
    evidence[field] = _evidence(selected.observation, cleared=selected.cleared)


def _resolve_nested_map(
    target: dict[str, Any],
    evidence: dict[str, Any],
    observations: Sequence[CardDataObservation],
    field: str,
    quarantines: list[Quarantine],
    entity_type: str,
    entity_id: int,
    capabilities: Sequence[str],
    *,
    protect_source_aliases: bool = False,
) -> None:
    current = copy.deepcopy(target.get(field))
    if not isinstance(current, Mapping):
        current = {}
    current = dict(current)
    keys = set(current)
    for item in observations:
        proposed = item.values.get(field)
        if isinstance(proposed, Mapping):
            keys.update(proposed)
        prefix = f"{field}."
        keys.update(
            clear[len(prefix) :]
            for clear in item.clear_fields
            if clear.startswith(prefix)
        )

    for key in sorted(keys):
        path = f"{field}.{key}"
        candidates = []
        for item in observations:
            proposed = item.values.get(field)
            if path in item.clear_fields:
                candidates.append(_Candidate(item, None, True))
            elif isinstance(proposed, Mapping) and proposed.get(key) is not None:
                proposed_value = copy.deepcopy(proposed[key])
                if (
                    protect_source_aliases
                    and key in current
                    and current[key] != proposed_value
                    and item.source_kind != "admin_override"
                ):
                    _quarantine(
                        quarantines,
                        "SOURCE_ALIAS_CONFLICT",
                        [item],
                        entity_type,
                        entity_id,
                        path,
                        "A non-Admin observation cannot replace an existing source alias.",
                        capabilities,
                    )
                    continue
                candidates.append(_Candidate(item, proposed_value))
        if protect_source_aliases and key not in current:
            external_candidates = [
                candidate
                for candidate in candidates
                if not candidate.cleared
                and candidate.observation.source_kind != "admin_override"
            ]
            if len({_canonical(item.value) for item in external_candidates}) > 1:
                _quarantine(
                    quarantines,
                    "SOURCE_ALIAS_CONFLICT",
                    [item.observation for item in external_candidates],
                    entity_type,
                    entity_id,
                    path,
                    "Conflicting external aliases cannot establish canonical identity.",
                    capabilities,
                )
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.observation.source_kind == "admin_override"
                ]
        selected = _select_candidate(
            candidates,
            evidence.get(path),
            current.get(key),
            quarantines,
            entity_type,
            entity_id,
            path,
            capabilities,
        )
        if selected is None:
            continue
        if selected.cleared:
            current.pop(key, None)
        else:
            current[key] = copy.deepcopy(selected.value)
        evidence[path] = _evidence(selected.observation, cleared=selected.cleared)
    target[field] = current


def _identity_filter(
    observations: Sequence[CardDataObservation],
    previous_snapshot: Optional[Mapping[str, Any]],
    quarantines: list[Quarantine],
) -> tuple[CardDataObservation, ...]:
    previous_event = (
        previous_snapshot.get("event", {})
        if isinstance(previous_snapshot, Mapping)
        else {}
    )
    previous_bouts = _index(
        previous_snapshot.get("bouts", ())
        if isinstance(previous_snapshot, Mapping)
        else (),
        "bout_id",
    )
    accepted = []
    for item in observations:
        is_admin = item.source_kind == "admin_override"
        if (
            not is_admin
            and item.entity_type == "event"
            and item.identity_basis == "date_only"
        ):
            _quarantine(
                quarantines,
                "EVENT_IDENTITY_EVIDENCE_INSUFFICIENT",
                [item],
                "event",
                item.entity_id,
                "identity",
                "A same-date match without a trusted alias or compatible name is advisory only.",
                ("EVT", "BOUT"),
            )
            continue
        if (
            not is_admin
            and item.entity_type in {"bout", "slot", "result"}
            and item.identity_basis == "names_only"
        ):
            _quarantine(
                quarantines,
                "BOUT_IDENTITY_EVIDENCE_INSUFFICIENT",
                [item],
                item.entity_type,
                item.entity_id,
                "identity",
                "Fighter names alone cannot attach data to a canonical bout.",
                ("BOUT",),
            )
            continue
        previous_entity = (
            previous_event
            if item.entity_type == "event"
            else previous_bouts.get(item.entity_id, {})
            if item.entity_type == "bout"
            else {}
        )
        proposed_aliases = item.values.get("source_ids")
        existing_aliases = previous_entity.get("source_ids", {})
        if (
            not is_admin
            and
            isinstance(proposed_aliases, Mapping)
            and isinstance(existing_aliases, Mapping)
            and any(
                key in existing_aliases and existing_aliases[key] != proposed
                for key, proposed in proposed_aliases.items()
            )
        ):
            _quarantine(
                quarantines,
                "SOURCE_ALIAS_CONFLICT",
                [item],
                item.entity_type,
                item.entity_id,
                "source_ids",
                "A conflicting source alias rejects the entire external observation.",
                ("EVT", "BOUT") if item.entity_type == "event" else ("BOUT",),
            )
            continue
        if item.entity_type == "bout" and item.entity_id in previous_bouts:
            old_ids = _fighter_ids(previous_bouts[item.entity_id].get("fighters"))
            new_ids = _fighter_ids(item.values.get("fighters"))
            if old_ids is not None and new_ids is not None and old_ids != new_ids:
                _quarantine(
                    quarantines,
                    "MATCHUP_ID_REUSE_FORBIDDEN",
                    [item],
                    "bout",
                    item.entity_id,
                    "fighters",
                    "A changed fighter set needs a separately allocated replacement bout ID.",
                    ("BOUT", "ELIG"),
                )
                continue
        if (
            not is_admin
            and
            item.entity_type == "event"
            and item.identity_basis == "date_and_name"
            and previous_event
        ):
            proposed_name = item.values.get("name")
            proposed_date = item.values.get("official_event_date")
            previous_name = previous_event.get("name")
            previous_date = previous_event.get("official_event_date")
            same_date = proposed_date is not None and proposed_date == previous_date
            similarity = _name_similarity(proposed_name, previous_name)
            if not same_date or similarity < 0.6:
                _quarantine(
                    quarantines,
                    "EVENT_IDENTITY_CONFLICT",
                    [item],
                    "event",
                    item.entity_id,
                    "identity",
                    "Date-and-name evidence contradicts the current canonical event.",
                    ("EVT", "BOUT"),
                )
                continue
        accepted.append(item)
    return tuple(accepted)


def _name_similarity(left: Any, right: Any) -> float:
    if not _nonempty(left) or not _nonempty(right):
        return 0.0

    def normalize(value: str) -> str:
        cleaned = "".join(
            character.lower() if character.isalnum() else " "
            for character in value
        )
        return " ".join(cleaned.split())

    return SequenceMatcher(None, normalize(left), normalize(right)).ratio()


def _new_event(event_id: int, observed_at: str) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "source_ids": {},
        "promotion": "UFC",
        "name": f"UFC event {event_id}",
        "subtitle": None,
        "official_event_date": None,
        "official_date_timezone": None,
        "month_key": None,
        "status": "scheduled",
        "card_start_time_utc": None,
        "picks_lock_time_utc": None,
        "section_start_times_utc": {},
        "section_lock_times_utc": {},
        "first_final_result_at": None,
        "main_event_bout_id": None,
        "listed_bout_count": 0,
        "mission_eligible_bout_count": 0,
        "lifecycle_revision": 1,
        "structure_revision": 1,
        "timing_revision": 1,
        "created_at": observed_at,
        "canonical_updated_at": observed_at,
        "evidence": {},
    }


def _new_bout(event_id: int, bout_id: int, observed_at: str) -> dict[str, Any]:
    return {
        "bout_id": bout_id,
        "event_id": event_id,
        "source_ids": {},
        "lineage_id": f"lineage_{bout_id}",
        "matchup_revision": 1,
        "replaces_bout_id": None,
        "replaced_by_bout_id": None,
        "status": "scheduled",
        "fighters": [],
        "weight_class": None,
        "gender": "unknown",
        "scheduled_rounds": 3,
        "is_title_fight": None,
        "is_bmf_title_fight": None,
        "title_type": "unknown",
        "result_revision": 0,
        "result": None,
        "source_updated_at": observed_at,
        "canonical_updated_at": observed_at,
        "evidence": {},
    }


def _new_slot(event_id: int, bout_id: int) -> dict[str, Any]:
    return {
        "slot_id": f"{event_id}:{bout_id}",
        "event_id": event_id,
        "bout_id": bout_id,
        "is_current": True,
        "card_section": None,
        "order_overall": None,
        "order_section": None,
        "role": "regular",
        "scheduled_start_time_utc": None,
        "automatic_lock_time_utc": None,
        "structure_revision": 1,
        "evidence": {},
    }


def _fighter_ids(value: Any) -> Optional[frozenset[str]]:
    if not _is_sequence(value):
        return None
    ids = []
    for fighter in value:
        if not isinstance(fighter, Mapping) or not _nonempty(fighter.get("fighter_id")):
            return None
        ids.append(fighter["fighter_id"])
    if len(ids) != 2 or len(set(ids)) != 2:
        return None
    return frozenset(ids)


def _resolve_title(
    bout: dict[str, Any],
    observations: Sequence[CardDataObservation],
    quarantines: list[Quarantine],
) -> None:
    evidence = bout["evidence"]
    external = []
    admin = []
    for item in observations:
        has_title = any(field in item.values for field in TITLE_FIELDS)
        clears_title = "title" in item.clear_fields or any(
            field in item.clear_fields for field in TITLE_FIELDS
        )
        if not has_title and not clears_title:
            continue
        if item.source_kind == "admin_override":
            admin.append(_Candidate(item, dict(item.values), clears_title))
        else:
            external.append(
                {
                    "observation_id": item.observation_id,
                    "source_kind": item.source_kind,
                    "observed_at": item.observed_at,
                    "source_ref": item.source_ref,
                    "suggested": {
                        field: copy.deepcopy(item.values[field])
                        for field in TITLE_FIELDS
                        if field in item.values
                    },
                }
            )
    if external:
        existing = evidence.get("title_suggestions", [])
        combined = {
            _canonical(item): copy.deepcopy(item)
            for item in [*existing, *external]
            if isinstance(item, Mapping)
        }
        evidence["title_suggestions"] = [combined[key] for key in sorted(combined)]

    previous = {
        field: bout.get(field)
        for field in TITLE_FIELDS
    }
    selected = _select_candidate(
        admin,
        evidence.get("is_title_fight"),
        previous,
        quarantines,
        "bout",
        bout["bout_id"],
        "title",
        ("TITLE",),
    )
    if selected is None:
        return
    if selected.cleared:
        resolved = {
            "is_title_fight": None,
            "is_bmf_title_fight": None,
            "title_type": "unknown",
        }
    else:
        values = selected.value
        is_title = values.get("is_title_fight")
        is_bmf = values.get("is_bmf_title_fight")
        title_type = values.get("title_type")
        if is_title is None and title_type in {"none"}:
            is_title = False
        if is_title is None and title_type in {
            "undisputed",
            "interim",
            "bmf",
            "other",
            "unknown",
        }:
            is_title = True
        if is_bmf is True:
            is_title = True
            title_type = "bmf"
        if is_title is False:
            if title_type not in {None, "none"} or is_bmf is True:
                _quarantine(
                    quarantines,
                    "TITLE_OVERRIDE_CONFLICT",
                    [selected.observation],
                    "bout",
                    bout["bout_id"],
                    "title",
                    "An Admin non-title override conflicts with its title type/BMF flag.",
                    ("TITLE",),
                )
                return
            resolved = {
                "is_title_fight": False,
                "is_bmf_title_fight": False,
                "title_type": "none",
            }
        elif is_title is True:
            if title_type == "none" or (title_type == "bmf" and is_bmf is False):
                _quarantine(
                    quarantines,
                    "TITLE_OVERRIDE_CONFLICT",
                    [selected.observation],
                    "bout",
                    bout["bout_id"],
                    "title",
                    "An Admin title override has an incompatible type/BMF flag.",
                    ("TITLE",),
                )
                return
            if title_type is None:
                title_type = (
                    previous["title_type"]
                    if previous["is_title_fight"] is True
                    else "unknown"
                )
            resolved = {
                "is_title_fight": True,
                "is_bmf_title_fight": title_type == "bmf",
                "title_type": title_type,
            }
        else:
            _quarantine(
                quarantines,
                "TITLE_OVERRIDE_INCOMPLETE",
                [selected.observation],
                "bout",
                bout["bout_id"],
                "title",
                "Admin TITLE input must explicitly resolve title, type, or clear.",
                ("TITLE",),
            )
            return
    winning_evidence = _evidence(selected.observation, cleared=selected.cleared)
    for field, value in resolved.items():
        bout[field] = value
        evidence[field] = copy.deepcopy(winning_evidence)


def _resolve_result(
    bout: dict[str, Any],
    observations: Sequence[CardDataObservation],
    quarantines: list[Quarantine],
) -> None:
    def mark_completed_from_result() -> None:
        if not isinstance(bout.get("result"), Mapping):
            return
        bout["status"] = "completed"
        result_evidence = bout["evidence"].get("result")
        if isinstance(result_evidence, Mapping):
            status_evidence = copy.deepcopy(dict(result_evidence))
            status_evidence["reason"] = "Derived from the canonical final result."
            bout["evidence"]["status"] = status_evidence

    candidates = []
    for item in observations:
        if item.entity_type == "result":
            if "result" in item.clear_fields or "$" in item.clear_fields:
                candidates.append(_Candidate(item, None, True))
            elif item.values:
                candidates.append(_Candidate(item, copy.deepcopy(dict(item.values))))
        elif item.entity_type == "bout":
            if "result" in item.clear_fields:
                candidates.append(_Candidate(item, None, True))
            elif isinstance(item.values.get("result"), Mapping):
                candidates.append(
                    _Candidate(item, copy.deepcopy(dict(item.values["result"])))
                )
    previous_result = bout.get("result")
    previous_semantic = _result_semantic(previous_result)
    selected = _select_candidate(
        candidates,
        bout["evidence"].get("result"),
        previous_semantic,
        quarantines,
        "bout",
        bout["bout_id"],
        "result",
        ("RES",),
    )
    if selected is None:
        mark_completed_from_result()
        return
    proposed_semantic = None if selected.cleared else _result_semantic(selected.value)
    if proposed_semantic == previous_semantic:
        mark_completed_from_result()
        return
    next_revision = int(bout.get("result_revision", 0)) + 1
    bout["result_revision"] = next_revision
    bout["evidence"]["result"] = _evidence(
        selected.observation, cleared=selected.cleared
    )
    if selected.cleared:
        bout["result"] = None
        if bout.get("status") == "completed":
            bout["status"] = "scheduled"
        bout["evidence"]["status"] = _evidence(
            selected.observation, cleared=True
        )
        return
    result = copy.deepcopy(dict(selected.value))
    for metadata_field in (
        "revision",
        "status",
        "recorded_at",
        "source_updated_at",
        "corrected_at",
        "correction_reason",
        "evidence",
    ):
        result.pop(metadata_field, None)
    corrected = previous_result is not None
    result.update(
        {
            "revision": next_revision,
            "status": "corrected" if corrected else "final",
            "recorded_at": selected.observation.observed_at,
            "source_updated_at": selected.observation.observed_at,
            "corrected_at": selected.observation.observed_at if corrected else None,
            "correction_reason": selected.observation.reason if corrected else None,
            "evidence": {
                field: _evidence(selected.observation)
                for field in sorted(result)
                if field not in {"evidence"}
            },
        }
    )
    bout["result"] = result
    bout["status"] = "completed"
    status_evidence = _evidence(selected.observation)
    status_evidence["reason"] = "Derived from the canonical final result."
    bout["evidence"]["status"] = status_evidence


def _result_semantic(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return None
    ignored = {
        "revision",
        "status",
        "recorded_at",
        "source_updated_at",
        "corrected_at",
        "correction_reason",
        "evidence",
    }
    return {key: copy.deepcopy(value[key]) for key in sorted(value) if key not in ignored}


def _normalize_slots(
    slots: dict[int, dict[str, Any]],
    bouts: Mapping[int, Mapping[str, Any]],
    quarantines: list[Quarantine],
) -> None:
    for bout_id, slot in slots.items():
        bout = bouts.get(bout_id)
        if bout is None:
            slot["is_current"] = False
            continue
        if bout.get("status") in {"cancelled", "replaced"}:
            slot["is_current"] = False

    current = [slot for slot in slots.values() if slot.get("is_current") is True]
    current.sort(
        key=lambda slot: (
            slot.get("order_overall")
            if _positive_int(slot.get("order_overall"))
            else 10**9,
            slot["bout_id"],
        )
    )
    section_counts: defaultdict[str, int] = defaultdict(int)
    for index, slot in enumerate(current, start=1):
        slot["order_overall"] = index
        section = slot.get("card_section")
        if section in {"early_prelim", "prelim", "main"}:
            section_counts[section] += 1
            slot["order_section"] = section_counts[section]
            source_evidence = slot.get("evidence", {}).get(
                "order_overall",
                slot.get("evidence", {}).get("card_section", {}),
            )
            if isinstance(source_evidence, Mapping) and source_evidence:
                derived_evidence = copy.deepcopy(dict(source_evidence))
                derived_evidence.update(
                    {
                        "source_kind": "derived",
                        "confidence": "high",
                        "source_ref": "CardDataNormalizerV1:section-order",
                        "reason": "Derived contiguously after final section grouping.",
                    }
                )
                slot["evidence"]["order_section"] = derived_evidence
        else:
            slot["order_section"] = None
            _quarantine(
                quarantines,
                "SECTION_UNKNOWN",
                (),
                "slot",
                slot["bout_id"],
                "card_section",
                "A current slot needs an explicit or trusted inferred section.",
                ("STRUCT", "ELIG"),
            )
        derived_role = (
            "main_event"
            if index == 1 and section == "main"
            else "co_main"
            if index == 2 and section == "main"
            else "regular"
        )
        slot["role"] = derived_role
        role_source = slot.get("evidence", {}).get(
            "order_overall",
            slot.get("evidence", {}).get("card_section", {}),
        )
        if isinstance(role_source, Mapping) and role_source:
            role_evidence = copy.deepcopy(dict(role_source))
            role_evidence.update(
                {
                    "source_kind": "derived",
                    "confidence": "high",
                    "source_ref": "CardDataNormalizerV1:slot-role",
                    "reason": "Derived from canonical section and top-down order.",
                }
            )
            slot["evidence"]["role"] = role_evidence
    for slot in slots.values():
        if not slot.get("is_current"):
            # Historical slots retain their last known placement.
            continue


def _apply_section_timing(
    event: Mapping[str, Any], slots: Mapping[int, dict[str, Any]]
) -> None:
    event_evidence = event.get("evidence", {})
    for map_field, slot_field in (
        ("section_start_times_utc", "scheduled_start_time_utc"),
        ("section_lock_times_utc", "automatic_lock_time_utc"),
    ):
        section_values = event.get(map_field, {})
        if not isinstance(section_values, Mapping):
            continue
        for slot in slots.values():
            section = slot.get("card_section")
            if section not in section_values:
                continue
            proposed_evidence = event_evidence.get(f"{map_field}.{section}", {})
            existing_evidence = slot.get("evidence", {}).get(slot_field, {})
            proposed_rank = _evidence_rank(proposed_evidence)
            existing_rank = _evidence_rank(existing_evidence)
            proposed_time = _evidence_time(proposed_evidence)
            existing_time = _evidence_time(existing_evidence)
            wins = proposed_rank > existing_rank or (
                proposed_rank == existing_rank
                and proposed_time is not None
                and (existing_time is None or proposed_time >= existing_time)
            )
            if wins:
                slot[slot_field] = copy.deepcopy(section_values[section])
                slot["evidence"][slot_field] = copy.deepcopy(proposed_evidence)


def _resolve_replacements(
    bouts: dict[int, dict[str, Any]],
    slots: dict[int, dict[str, Any]],
    previous_bouts: Mapping[int, Mapping[str, Any]],
    observations_by_bout: Mapping[int, Sequence[CardDataObservation]],
    quarantines: list[Quarantine],
) -> None:
    for bout_id, observations in sorted(observations_by_bout.items()):
        previous = previous_bouts.get(bout_id)
        fighter_observations = [
            item
            for item in observations
            if _fighter_ids(item.values.get("fighters")) is not None
        ]
        proposed_sets = {
            _fighter_ids(item.values.get("fighters")) for item in fighter_observations
        }
        if len(proposed_sets) > 1:
            bouts[bout_id]["fighters"] = (
                copy.deepcopy(previous.get("fighters", [])) if previous else []
            )
            _quarantine(
                quarantines,
                "MATCHUP_IDENTITY_CONFLICT",
                fighter_observations,
                "bout",
                bout_id,
                "fighters",
                "Multiple fighter sets cannot share one proposed canonical bout ID.",
                ("BOUT", "ELIG"),
            )
            continue
        if previous is not None:
            old_ids = _fighter_ids(previous.get("fighters"))
            for item in observations:
                if "fighters" not in item.values:
                    continue
                new_ids = _fighter_ids(item.values.get("fighters"))
                if old_ids is not None and new_ids is not None and old_ids != new_ids:
                    bouts[bout_id]["fighters"] = copy.deepcopy(previous["fighters"])
                    bouts[bout_id]["matchup_revision"] = previous.get(
                        "matchup_revision", 1
                    )
                    _quarantine(
                        quarantines,
                        "MATCHUP_ID_REUSE_FORBIDDEN",
                        [item],
                        "bout",
                        bout_id,
                        "fighters",
                        "A changed fighter set needs a separately allocated replacement bout ID.",
                        ("BOUT", "ELIG"),
                    )

    for bout_id, bout in sorted(bouts.items()):
        replaced_id = bout.get("replaces_bout_id")
        if not _positive_int(replaced_id):
            continue
        old = bouts.get(replaced_id)
        if old is None or replaced_id == bout_id:
            related = observations_by_bout.get(bout_id, ())
            _quarantine(
                quarantines,
                "REPLACEMENT_TARGET_INVALID",
                related,
                "bout",
                bout_id,
                "replaces_bout_id",
                "A replacement must reference a different retained canonical bout.",
                ("BOUT", "ELIG"),
            )
            continue
        old["status"] = "replaced"
        old["replaced_by_bout_id"] = bout_id
        bout["lineage_id"] = old.get("lineage_id", f"lineage_{replaced_id}")
        bout["matchup_revision"] = int(old.get("matchup_revision", 1)) + 1
        if replaced_id in slots:
            slots[replaced_id]["is_current"] = False


def _semantic_event(value: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "source_ids",
        "promotion",
        "name",
        "subtitle",
        "official_event_date",
        "official_date_timezone",
        "month_key",
        "status",
        "card_start_time_utc",
        "picks_lock_time_utc",
        "section_start_times_utc",
        "section_lock_times_utc",
        "first_final_result_at",
        "main_event_bout_id",
        "listed_bout_count",
        "mission_eligible_bout_count",
    )
    return {field: copy.deepcopy(value.get(field)) for field in fields}


def _semantic_bout(value: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "source_ids",
        "lineage_id",
        "matchup_revision",
        "replaces_bout_id",
        "replaced_by_bout_id",
        "status",
        "fighters",
        "weight_class",
        "gender",
        "scheduled_rounds",
        *TITLE_FIELDS,
        "result_revision",
        "result",
    )
    result = {field: copy.deepcopy(value.get(field)) for field in fields}
    result["result"] = _result_semantic(result["result"])
    return result


def _semantic_slot(value: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "is_current",
        "card_section",
        "order_overall",
        "order_section",
        "role",
        "scheduled_start_time_utc",
        "automatic_lock_time_utc",
    )
    return {field: copy.deepcopy(value.get(field)) for field in fields}


def _index(items: Any, key: str) -> dict[int, Mapping[str, Any]]:
    if not _is_sequence(items):
        return {}
    return {
        item[key]: item
        for item in items
        if isinstance(item, Mapping) and _positive_int(item.get(key))
    }


def _structure_projection(
    event: Mapping[str, Any],
    bouts: Mapping[int, Mapping[str, Any]],
    slots: Mapping[int, Mapping[str, Any]],
) -> Any:
    return {
        "main_event_bout_id": event.get("main_event_bout_id"),
        "bouts": {
            str(key): {
                "status": bout.get("status"),
                "title": [bout.get(field) for field in TITLE_FIELDS],
            }
            for key, bout in sorted(bouts.items())
        },
        "slots": {
            str(key): {
                field: slot.get(field)
                for field in (
                    "is_current",
                    "card_section",
                    "order_overall",
                    "order_section",
                    "role",
                )
            }
            for key, slot in sorted(slots.items())
        },
    }


def _timing_projection(
    event: Mapping[str, Any], slots: Mapping[int, Mapping[str, Any]]
) -> Any:
    return {
        "event": {field: copy.deepcopy(event.get(field)) for field in TIMING_EVENT_FIELDS},
        "slots": {
            str(key): {
                "scheduled_start_time_utc": slot.get("scheduled_start_time_utc"),
                "automatic_lock_time_utc": slot.get("automatic_lock_time_utc"),
            }
            for key, slot in sorted(slots.items())
        },
    }


def _eligibility_projection(
    event_id: int,
    snapshot_revision: int,
    generated_at: str,
    bouts: Mapping[int, Mapping[str, Any]],
    slots: Mapping[int, Mapping[str, Any]],
    quarantines: Sequence[Quarantine],
    previous: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    blocked_bouts = {
        item.entity_id
        for item in quarantines
        if item.entity_type in {"bout", "slot"}
        and ({"BOUT", "ELIG", "STRUCT"} & set(item.blocks_capabilities))
    }
    eligible = []
    excluded = []
    for bout_id, slot in sorted(slots.items()):
        if slot.get("is_current") is not True:
            continue
        bout = bouts.get(bout_id)
        reasons = []
        if bout is None:
            reasons.append("BOUT_MISSING")
            matchup_revision = 1
        else:
            matchup_revision = bout.get("matchup_revision", 1)
            if bout.get("status") in {"cancelled", "postponed", "replaced"}:
                reasons.append(f"BOUT_{str(bout.get('status')).upper()}")
            if _fighter_ids(bout.get("fighters")) is None:
                reasons.append("FIGHTERS_UNRESOLVED")
        if slot.get("card_section") not in {"early_prelim", "prelim", "main"}:
            reasons.append("SECTION_UNRESOLVED")
        if (
            slot.get("scheduled_start_time_utc") is None
            or slot.get("automatic_lock_time_utc") is None
        ):
            reasons.append("TIMING_UNRESOLVED")
        if bout_id in blocked_bouts:
            reasons.append("DATA_QUARANTINED")
        target = {"bout_id": bout_id, "matchup_revision": matchup_revision}
        if reasons:
            excluded.append({**target, "reason_codes": sorted(set(reasons))})
        else:
            eligible.append(target)
    fingerprint_input = {
        "contract_version": CONTRACT_VERSION,
        "rules_version": ELIGIBILITY_RULES_VERSION,
        "eligible_targets": eligible,
        "excluded_targets": excluded,
    }
    fingerprint = _hash(fingerprint_input)
    previous_eligibility = (
        previous.get("current_eligibility", {})
        if isinstance(previous, Mapping)
        else {}
    )
    created_at = (
        previous_eligibility.get("created_at")
        if previous_eligibility.get("fingerprint") == fingerprint
        else generated_at
    )
    return {
        "eligibility_snapshot_id": f"elig_{event_id}_{fingerprint.split(':', 1)[1][:16]}",
        "kind": "current",
        "event_id": event_id,
        "card_snapshot_revision": snapshot_revision,
        "created_at": created_at or generated_at,
        "eligible_targets": eligible,
        "excluded_targets": excluded,
        "denominator": len(eligible),
        "fingerprint": fingerprint,
    }


def _quality(
    event: Mapping[str, Any],
    bouts: Mapping[int, Mapping[str, Any]],
    slots: Mapping[int, Mapping[str, Any]],
    eligibility: Mapping[str, Any],
    quarantines: Sequence[Quarantine],
) -> dict[str, Any]:
    issues = [item.as_quality_issue() for item in quarantines]
    states = {capability: "ready" for capability in CAPABILITIES}
    for item in quarantines:
        for capability in item.blocks_capabilities:
            states[capability] = "quarantined"

    if not _positive_int(event.get("event_id")) or not _nonempty(event.get("name")):
        states["EVT"] = "blocked"
    if (
        not _nonempty(event.get("official_event_date"))
        or not _nonempty(event.get("official_date_timezone"))
    ):
        states["EVT_DATE"] = "blocked"
        issues.append(
            {
                "code": "EVENT_DATE_REQUIRED",
                "severity": "error",
                "scope_type": "event",
                "scope_id": str(event.get("event_id")),
                "field": "official_event_date",
                "message": "Official event date and timezone remain unresolved.",
                "blocks_capabilities": ["EVT_DATE"],
            }
        )
    current_bout_ids = {
        bout_id for bout_id, slot in slots.items() if slot.get("is_current") is True
    }
    for bout_id in sorted(current_bout_ids):
        bout = bouts.get(bout_id, {})
        if _fighter_ids(bout.get("fighters")) is None:
            states["BOUT"] = "blocked"
        if bout.get("is_title_fight") not in {True, False} or not isinstance(
            bout.get("evidence", {}).get("is_title_fight"), Mapping
        ) or bout.get("evidence", {}).get("is_title_fight", {}).get(
            "source_kind"
        ) != "admin_override":
            if states["TITLE"] != "quarantined":
                states["TITLE"] = "blocked"
            issues.append(
                {
                    "code": "TITLE_STATUS_UNKNOWN",
                    "severity": "warning",
                    "scope_type": "bout",
                    "scope_id": str(bout_id),
                    "field": "is_title_fight",
                    "message": "TITLE remains unresolved until Admin explicitly sets true or false.",
                    "blocks_capabilities": ["TITLE"],
                }
            )
        if bout.get("status") == "completed" and not isinstance(
            bout.get("result"), Mapping
        ):
            states["RES"] = "blocked"
    if states["RES"] == "ready" and any(
        bouts[bout_id].get("status") != "completed"
        for bout_id in current_bout_ids
        if bout_id in bouts
    ):
        states["RES"] = "pending"
    if any(
        slot.get("card_section") not in {"early_prelim", "prelim", "main"}
        for slot in slots.values()
        if slot.get("is_current") is True
    ):
        if states["STRUCT"] != "quarantined":
            states["STRUCT"] = "blocked"
    states["ELIG"] = (
        "quarantined"
        if any(
            state == "quarantined" for capability, state in states.items()
            if capability in {"BOUT", "STRUCT", "ELIG"}
        )
        else "ready"
    )
    overall = (
        "quarantined"
        if "quarantined" in states.values()
        else "ready"
        if all(state == "ready" for state in states.values())
        else "degraded"
    )
    unique_issues = {_canonical(item): item for item in issues}
    return {
        "overall": overall,
        "capabilities": states,
        "issues": [unique_issues[key] for key in sorted(unique_issues)],
    }


def _change(
    change_type: str,
    entity_type: str,
    entity_id: int,
    field: str,
    before: Any,
    after: Any,
    evidence: Any,
    impact: str,
) -> dict[str, Any]:
    return {
        "type": change_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "field": field,
        "before": copy.deepcopy(before),
        "after": copy.deepcopy(after),
        "evidence": copy.deepcopy(evidence) if evidence is not None else {},
        "impact": impact,
    }


def _build_changes(
    previous: Optional[Mapping[str, Any]],
    event: Mapping[str, Any],
    bouts: Mapping[int, Mapping[str, Any]],
    slots: Mapping[int, Mapping[str, Any]],
    eligibility: Mapping[str, Any],
    quality: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if previous is None:
        changes = [
            _change(
                "IDENTITY_RESOLVED",
                "event",
                event["event_id"],
                "event_id",
                None,
                event["event_id"],
                event.get("evidence", {}).get("source_ids.espn_event_id", {}),
                "none",
            )
        ]
        changes.extend(
            _change(
                "BOUT_ADDED",
                "bout",
                bout_id,
                "bout_id",
                None,
                bout_id,
                bout.get("evidence", {}).get("fighters", {}),
                "recompute_current_eligibility",
            )
            for bout_id, bout in sorted(bouts.items())
        )
        return changes

    previous_event = previous.get("event", {})
    previous_bouts = _index(previous.get("bouts"), "bout_id")
    previous_slots = _index(previous.get("card_slots"), "bout_id")
    changes = []
    event_evidence = event.get("evidence", {})
    for field in ("source_ids", "name", "subtitle"):
        if _canonical(previous_event.get(field)) != _canonical(event.get(field)):
            changes.append(
                _change(
                    "IDENTITY_RESOLVED",
                    "event",
                    event["event_id"],
                    field,
                    previous_event.get(field),
                    event.get(field),
                    event_evidence.get(field, {}),
                    "manual_review" if field == "source_ids" else "none",
                )
            )
    if previous_event.get("official_event_date") != event.get("official_event_date"):
        changes.append(
            _change(
                "EVENT_DATE_CHANGED",
                "event",
                event["event_id"],
                "official_event_date",
                previous_event.get("official_event_date"),
                event.get("official_event_date"),
                event_evidence.get("official_event_date", {}),
                "manual_review",
            )
        )
    if previous_event.get("status") != event.get("status"):
        changes.append(
            _change(
                "EVENT_STATUS_CHANGED",
                "event",
                event["event_id"],
                "status",
                previous_event.get("status"),
                event.get("status"),
                event_evidence.get("status", {}),
                "recompute_current_eligibility",
            )
        )
    if previous_event.get("first_final_result_at") != event.get(
        "first_final_result_at"
    ):
        changes.append(
            _change(
                "EVENT_STATUS_CHANGED",
                "event",
                event["event_id"],
                "first_final_result_at",
                previous_event.get("first_final_result_at"),
                event.get("first_final_result_at"),
                event_evidence.get("first_final_result_at", {}),
                "recompute_current_eligibility",
            )
        )
    for field in TIMING_EVENT_FIELDS[2:]:
        if _canonical(previous_event.get(field)) != _canonical(event.get(field)):
            changes.append(
                _change(
                    "TIMING_CHANGED",
                    "event",
                    event["event_id"],
                    field,
                    previous_event.get(field),
                    event.get(field),
                    event_evidence.get(field, {}),
                    "recompute_current_eligibility",
                )
            )

    for bout_id, bout in sorted(bouts.items()):
        old = previous_bouts.get(bout_id)
        if old is None:
            changes.append(
                _change(
                    "BOUT_ADDED",
                    "bout",
                    bout_id,
                    "bout_id",
                    None,
                    bout_id,
                    bout.get("evidence", {}).get("fighters", {}),
                    "recompute_current_eligibility",
                )
            )
            if bout.get("replaces_bout_id") is not None:
                changes.append(
                    _change(
                        "MATCHUP_REPLACED",
                        "bout",
                        bout_id,
                        "replaces_bout_id",
                        bout.get("replaces_bout_id"),
                        bout_id,
                        bout.get("evidence", {}).get("fighters", {}),
                        "void_direct_targets",
                    )
                )
            continue
        if old.get("status") != bout.get("status") and bout.get("status") == "cancelled":
            changes.append(
                _change(
                    "BOUT_CANCELLED",
                    "bout",
                    bout_id,
                    "status",
                    old.get("status"),
                    "cancelled",
                    bout.get("evidence", {}).get("status", {}),
                    "void_direct_targets",
                )
            )
        old_title = tuple(old.get(field) for field in TITLE_FIELDS)
        new_title = tuple(bout.get(field) for field in TITLE_FIELDS)
        if old_title != new_title:
            changes.append(
                _change(
                    "TITLE_CHANGED",
                    "bout",
                    bout_id,
                    "title",
                    list(old_title),
                    list(new_title),
                    bout.get("evidence", {}).get("is_title_fight", {}),
                    "recompute_current_eligibility",
                )
            )
        old_result = _result_semantic(old.get("result"))
        new_result = _result_semantic(bout.get("result"))
        if old_result != new_result:
            change_type = (
                "RESULT_RECORDED"
                if old_result is None and new_result is not None
                else "RESULT_CLEARED"
                if new_result is None
                else "RESULT_CORRECTED"
            )
            changes.append(
                _change(
                    change_type,
                    "bout",
                    bout_id,
                    "result",
                    old_result,
                    new_result,
                    bout.get("evidence", {}).get("result", {}),
                    "reconcile_results",
                )
            )

    for bout_id, slot in sorted(slots.items()):
        old = previous_slots.get(bout_id)
        if old is None:
            continue
        if old.get("card_section") != slot.get("card_section"):
            changes.append(
                _change(
                    "SECTION_CHANGED",
                    "slot",
                    bout_id,
                    "card_section",
                    old.get("card_section"),
                    slot.get("card_section"),
                    slot.get("evidence", {}).get("card_section", {}),
                    "recompute_current_eligibility",
                )
            )
        if old.get("order_overall") != slot.get("order_overall") or old.get(
            "order_section"
        ) != slot.get("order_section"):
            changes.append(
                _change(
                    "SLOT_REORDERED",
                    "slot",
                    bout_id,
                    "order",
                    [old.get("order_overall"), old.get("order_section")],
                    [slot.get("order_overall"), slot.get("order_section")],
                    slot.get("evidence", {}).get("order_overall", {}),
                    "recompute_current_eligibility",
                )
            )
        if old.get("role") != slot.get("role"):
            changes.append(
                _change(
                    "HEADLINER_CHANGED",
                    "slot",
                    bout_id,
                    "role",
                    old.get("role"),
                    slot.get("role"),
                    slot.get("evidence", {}).get("role", {}),
                    "recompute_current_eligibility",
                )
            )
    previous_eligibility = previous.get("current_eligibility", {})
    if previous_eligibility.get("fingerprint") != eligibility.get("fingerprint"):
        changes.append(
            _change(
                "ELIGIBILITY_CHANGED",
                "event",
                event["event_id"],
                "current_eligibility",
                {
                    "eligible_targets": previous_eligibility.get(
                        "eligible_targets", []
                    ),
                    "excluded_targets": previous_eligibility.get(
                        "excluded_targets", []
                    ),
                    "fingerprint": previous_eligibility.get("fingerprint"),
                },
                {
                    "eligible_targets": eligibility.get("eligible_targets", []),
                    "excluded_targets": eligibility.get("excluded_targets", []),
                    "fingerprint": eligibility.get("fingerprint"),
                },
                {},
                "recompute_current_eligibility",
            )
        )
    previous_quality = previous.get("quality", {})
    if _canonical(previous_quality) != _canonical(quality):
        changes.append(
            _change(
                "QUALITY_CHANGED",
                "event",
                event["event_id"],
                "quality",
                previous_quality,
                quality,
                {},
                "manual_review",
            )
        )
    return sorted(
        changes,
        key=lambda item: (
            item["type"],
            item["entity_type"],
            item["entity_id"],
            item["field"],
        ),
    )


def _normalize_observations(
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
) -> tuple[CardDataObservation, ...]:
    if not _is_sequence(observations) or not observations:
        raise NormalizationInputError("At least one observation is required.")
    normalized = []
    ids = set()
    for value in observations:
        item = (
            value
            if isinstance(value, CardDataObservation)
            else CardDataObservation.from_mapping(value)
        )
        item.validate()
        if item.observation_id in ids:
            raise NormalizationInputError(
                f"Duplicate observation_id {item.observation_id!r}."
            )
        ids.add(item.observation_id)
        normalized.append(item)
    event_ids = {item.event_id for item in normalized}
    if len(event_ids) != 1:
        raise NormalizationInputError(
            "One normalization call may contain observations for exactly one event."
        )
    return tuple(sorted(normalized, key=_observation_sort_key))


def normalize_card_data_v1(
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
    previous_snapshot: Optional[Mapping[str, Any]] = None,
) -> NormalizationResult:
    """Resolve one event's observations without side effects.

    The same semantic observations and previous snapshot produce the same
    revision allocation regardless of input order.  Timestamps in the output
    come only from observation timestamps (or the previous snapshot), never
    from the wall clock.
    """

    items = _normalize_observations(observations)
    event_id = items[0].event_id
    previous = copy.deepcopy(dict(previous_snapshot)) if previous_snapshot else None
    if previous is not None:
        previous_event_id = previous.get("event", {}).get("event_id")
        if previous_event_id != event_id:
            raise NormalizationInputError(
                "previous_snapshot belongs to a different canonical event."
            )
    latest_observed = max(items, key=lambda item: _parse_utc(item.observed_at)).observed_at
    generated_at = latest_observed
    previous_generated = _parse_utc(previous.get("generated_at")) if previous else None
    if previous_generated is not None and previous_generated > _parse_utc(generated_at):
        generated_at = previous["generated_at"]

    quarantines: list[Quarantine] = []
    accepted = _identity_filter(items, previous, quarantines)
    event_observations = [item for item in accepted if item.entity_type == "event"]
    bout_observations: defaultdict[int, list[CardDataObservation]] = defaultdict(list)
    slot_observations: defaultdict[int, list[CardDataObservation]] = defaultdict(list)
    result_observations: defaultdict[int, list[CardDataObservation]] = defaultdict(list)
    for item in accepted:
        if item.entity_type == "bout":
            bout_observations[item.entity_id].append(item)
        elif item.entity_type == "slot":
            slot_observations[item.entity_id].append(item)
        elif item.entity_type == "result":
            result_observations[item.entity_id].append(item)

    previous_event = previous.get("event", {}) if previous else {}
    event = (
        copy.deepcopy(dict(previous_event))
        if previous_event
        else _new_event(event_id, generated_at)
    )
    event["event_id"] = event_id
    event_evidence = copy.deepcopy(event.get("evidence", {}))
    event["evidence"] = event_evidence
    _resolve_nested_map(
        event,
        event_evidence,
        event_observations,
        "source_ids",
        quarantines,
        "event",
        event_id,
        ("EVT",),
        protect_source_aliases=True,
    )
    for field in EVENT_FIELDS:
        if field in DERIVED_EVENT_FIELDS:
            continue
        if field in NESTED_MAP_FIELDS["event"]:
            _resolve_nested_map(
                event,
                event_evidence,
                event_observations,
                field,
                quarantines,
                "event",
                event_id,
                ("EVT_DATE",) if "time" in field else ("EVT",),
            )
        else:
            _resolve_field(
                event,
                event_evidence,
                event_observations,
                field,
                quarantines,
                "event",
                event_id,
                ("EVT_DATE",) if field in TIMING_EVENT_FIELDS else ("EVT",),
            )
    if _nonempty(event.get("official_event_date")):
        event["month_key"] = event["official_event_date"][:7]

    previous_bouts = _index(previous.get("bouts") if previous else (), "bout_id")
    all_bout_ids = set(previous_bouts) | set(bout_observations) | set(result_observations)
    bouts: dict[int, dict[str, Any]] = {}
    for bout_id in sorted(all_bout_ids):
        base = previous_bouts.get(bout_id)
        bout = (
            copy.deepcopy(dict(base))
            if base is not None
            else _new_bout(event_id, bout_id, generated_at)
        )
        bout["bout_id"] = bout_id
        bout["event_id"] = event_id
        evidence = copy.deepcopy(bout.get("evidence", {}))
        bout["evidence"] = evidence
        related = bout_observations.get(bout_id, [])
        _resolve_nested_map(
            bout,
            evidence,
            related,
            "source_ids",
            quarantines,
            "bout",
            bout_id,
            ("BOUT",),
            protect_source_aliases=True,
        )
        for field in BOUT_FIELDS:
            _resolve_field(
                bout,
                evidence,
                related,
                field,
                quarantines,
                "bout",
                bout_id,
                ("BOUT", "ELIG") if field == "fighters" else ("BOUT",),
            )
        _resolve_title(bout, related, quarantines)
        _resolve_result(
            bout,
            [*related, *result_observations.get(bout_id, [])],
            quarantines,
        )
        source_times = [item.observed_at for item in related]
        if source_times:
            bout["source_updated_at"] = max(source_times)
        bouts[bout_id] = bout

    previous_slots = _index(previous.get("card_slots") if previous else (), "bout_id")
    all_slot_ids = set(previous_slots) | set(slot_observations)
    slots: dict[int, dict[str, Any]] = {}
    for bout_id in sorted(all_slot_ids):
        base = previous_slots.get(bout_id)
        slot = (
            copy.deepcopy(dict(base))
            if base is not None
            else _new_slot(event_id, bout_id)
        )
        slot["slot_id"] = f"{event_id}:{bout_id}"
        slot["event_id"] = event_id
        slot["bout_id"] = bout_id
        evidence = copy.deepcopy(slot.get("evidence", {}))
        slot["evidence"] = evidence
        related = slot_observations.get(bout_id, [])
        for field in SLOT_FIELDS:
            _resolve_field(
                slot,
                evidence,
                related,
                field,
                quarantines,
                "slot",
                bout_id,
                ("STRUCT", "ELIG")
                if field in {"is_current", "card_section", "order_overall", "role"}
                else ("EVT_DATE", "ELIG"),
            )
        slots[bout_id] = slot

    _resolve_replacements(
        bouts,
        slots,
        previous_bouts,
        bout_observations,
        quarantines,
    )
    _apply_section_timing(event, slots)
    _normalize_slots(slots, bouts, quarantines)

    current_slots = [slot for slot in slots.values() if slot.get("is_current") is True]
    event["main_event_bout_id"] = next(
        (slot["bout_id"] for slot in current_slots if slot.get("role") == "main_event"),
        None,
    )
    event["listed_bout_count"] = len(current_slots)
    starts = [
        slot["scheduled_start_time_utc"]
        for slot in current_slots
        if _nonempty(slot.get("scheduled_start_time_utc"))
    ]
    locks = [
        slot["automatic_lock_time_utc"]
        for slot in current_slots
        if _nonempty(slot.get("automatic_lock_time_utc"))
    ]
    if starts:
        event["card_start_time_utc"] = min(starts)
        event_evidence["card_start_time_utc"] = {
            "source_kind": "derived",
            "confidence": "high",
            "observed_at": generated_at,
            "source_ref": "CardDataNormalizerV1:current-slot-starts",
            "reason": "Minimum scheduled start among current canonical slots.",
        }
    if locks:
        event["picks_lock_time_utc"] = min(locks)
        event_evidence["picks_lock_time_utc"] = {
            "source_kind": "derived",
            "confidence": "high",
            "observed_at": generated_at,
            "source_ref": "CardDataNormalizerV1:current-slot-locks",
            "reason": "Minimum automatic lock among current canonical slots.",
        }
    final_times = [
        bout["result"]["recorded_at"]
        for bout in bouts.values()
        if isinstance(bout.get("result"), Mapping)
        and _nonempty(bout["result"].get("recorded_at"))
    ]
    if final_times:
        previous_first_final = previous_event.get("first_final_result_at")
        event["first_final_result_at"] = min(
            [*final_times]
            + ([previous_first_final] if _nonempty(previous_first_final) else [])
        )
    else:
        event["first_final_result_at"] = None

    previous_snapshot_revision = int(previous.get("snapshot_revision", 0)) if previous else 0
    eligibility_probe = _eligibility_projection(
        event_id,
        previous_snapshot_revision or 1,
        generated_at,
        bouts,
        slots,
        quarantines,
        previous,
    )
    event["mission_eligible_bout_count"] = eligibility_probe["denominator"]
    quality = _quality(event, bouts, slots, eligibility_probe, quarantines)
    previous_structure = (
        _structure_projection(previous_event, previous_bouts, previous_slots)
        if previous
        else None
    )
    new_structure = _structure_projection(event, bouts, slots)
    structure_changed = previous is None or _canonical(previous_structure) != _canonical(
        new_structure
    )
    previous_timing = (
        _timing_projection(previous_event, previous_slots) if previous else None
    )
    timing_changed = previous is None or _canonical(previous_timing) != _canonical(
        _timing_projection(event, slots)
    )
    lifecycle_changed = previous is None or any(
        previous_event.get(field) != event.get(field)
        for field in ("status", "first_final_result_at")
    )

    previous_semantic = (
        {
            "event": _semantic_event(previous_event),
            "bouts": {
                str(key): _semantic_bout(value)
                for key, value in sorted(previous_bouts.items())
            },
            "slots": {
                str(key): _semantic_slot(value)
                for key, value in sorted(previous_slots.items())
            },
            "eligibility": {
                field: copy.deepcopy(previous.get("current_eligibility", {}).get(field))
                for field in (
                    "eligible_targets",
                    "excluded_targets",
                    "denominator",
                    "fingerprint",
                )
            },
            "quality": copy.deepcopy(previous.get("quality", {})),
        }
        if previous
        else None
    )
    new_semantic = {
        "event": _semantic_event(event),
        "bouts": {
            str(key): _semantic_bout(value) for key, value in sorted(bouts.items())
        },
        "slots": {
            str(key): _semantic_slot(value) for key, value in sorted(slots.items())
        },
        "eligibility": {
            field: copy.deepcopy(eligibility_probe.get(field))
            for field in (
                "eligible_targets",
                "excluded_targets",
                "denominator",
                "fingerprint",
            )
        },
        "quality": copy.deepcopy(quality),
    }
    semantic_changed = previous is None or _canonical(previous_semantic) != _canonical(
        new_semantic
    )
    snapshot_revision = (
        1
        if previous is None
        else previous_snapshot_revision + 1
        if semantic_changed
        else previous_snapshot_revision
    )
    event["lifecycle_revision"] = (
        1
        if previous is None
        else int(previous_event.get("lifecycle_revision", 1)) + int(lifecycle_changed)
    )
    event["structure_revision"] = (
        1
        if previous is None
        else int(previous_event.get("structure_revision", 1)) + int(structure_changed)
    )
    event["timing_revision"] = (
        1
        if previous is None
        else int(previous_event.get("timing_revision", 1)) + int(timing_changed)
    )
    event["created_at"] = previous_event.get("created_at", generated_at)
    event["canonical_updated_at"] = (
        generated_at
        if semantic_changed
        else previous_event.get("canonical_updated_at", generated_at)
    )
    for bout_id, bout in bouts.items():
        old = previous_bouts.get(bout_id)
        bout_changed = old is None or _canonical(_semantic_bout(old)) != _canonical(
            _semantic_bout(bout)
        )
        bout["canonical_updated_at"] = (
            generated_at
            if bout_changed
            else old.get("canonical_updated_at", generated_at)
        )
    for bout_id, slot in slots.items():
        old = previous_slots.get(bout_id)
        placement_changed = old is None or _canonical(_semantic_slot(old)) != _canonical(
            _semantic_slot(slot)
        )
        slot["structure_revision"] = (
            event["structure_revision"]
            if placement_changed
            else old.get("structure_revision", event["structure_revision"])
        )

    eligibility = _eligibility_projection(
        event_id,
        snapshot_revision,
        generated_at,
        bouts,
        slots,
        quarantines,
        previous,
    )
    event["mission_eligible_bout_count"] = eligibility["denominator"]
    source_records = []
    seen_sources = set()
    for item in items:
        record = {
            "source": item.source_kind,
            "source_event_id": item.source_event_id,
            "observed_at": item.observed_at,
            "payload_hash": item.payload_hash
            or _hash({"source_ref": item.source_ref, "values": item.values}),
        }
        key = _canonical(record)
        if key not in seen_sources:
            seen_sources.add(key)
            source_records.append(record)
    source_records.sort(key=_canonical)
    run_id = f"normalize_{_hash(source_records).split(':', 1)[1][:16]}"

    snapshot = {
        "contract_version": CONTRACT_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "snapshot_id": f"{event_id}:{snapshot_revision}",
        "snapshot_revision": snapshot_revision,
        "generated_at": generated_at,
        "source_run": {
            "run_id": run_id,
            "observed_at": latest_observed,
            "sources": source_records,
            "previous_snapshot_revision": previous_snapshot_revision or None,
        },
        "event": event,
        "bouts": [bouts[key] for key in sorted(bouts)],
        "card_slots": [slots[key] for key in sorted(slots)],
        "current_eligibility": eligibility,
        "quality": quality,
    }
    changes = _build_changes(previous, event, bouts, slots, eligibility, quality)
    change_payload = {
        "event_id": event_id,
        "previous_snapshot_revision": previous_snapshot_revision,
        "new_snapshot_revision": snapshot_revision,
        "changes": changes,
    }
    change_set = {
        "change_set_id": (
            f"change_{event_id}_{previous_snapshot_revision}_{snapshot_revision}_"
            f"{_hash(change_payload).split(':', 1)[1][:12]}"
        ),
        "event_id": event_id,
        "previous_snapshot_revision": previous_snapshot_revision,
        "new_snapshot_revision": snapshot_revision,
        "detected_at": generated_at,
        "changes": changes,
    }
    ordered_quarantines = tuple(
        sorted(
            set(quarantines),
            key=lambda item: (
                item.code,
                item.entity_type,
                -1 if item.entity_id is None else item.entity_id,
                item.field,
                item.observation_ids,
            ),
        )
    )
    return NormalizationResult(snapshot, change_set, ordered_quarantines)


__all__ = [
    "ELIGIBILITY_RULES_VERSION",
    "NORMALIZER_VERSION",
    "SOURCE_RANKS",
    "CardDataObservation",
    "NormalizationInputError",
    "NormalizationResult",
    "Quarantine",
    "normalize_card_data_v1",
]
