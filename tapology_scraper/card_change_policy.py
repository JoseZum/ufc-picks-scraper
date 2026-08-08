"""Pure late-card change policy over CardData V1 observations.

SCR-010 owns the distinction between a transiently incomplete source payload
and a canonical card change.  It wraps ``CardDataNormalizerV1`` without
opening a database connection:

* explicit cancellation/postponement and linked replacement facts apply now;
* one missing or partial payload only emits operational findings;
* inferred removal requires three distinct complete ESPN-detail observations
  spanning at least 30 minutes inside the seven days before card lock;
* no absence inference runs after lock, after the first final result, or while
  the event is not scheduled;
* cancelled/replaced bout IDs are terminal and cannot be silently revived;
* current eligibility may change, while frozen mission/streak/monthly
  snapshots are explicitly declared immutable.

The returned presence state is a persistence-neutral value object for a future
adapter.  This module never writes it, never calls current writers, and never
physically deletes a bout or slot.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from tapology_scraper.card_data_contract import validate_card_data_v1
from tapology_scraper.card_data_normalizer import (
    SOURCE_RANKS,
    CardDataObservation,
    NormalizationResult,
    normalize_card_data_v1,
)


POLICY_VERSION = "card-change-policy/v1"
AUTHORITATIVE_COMPLETE_SOURCE = "espn_detail"
AUTO_REMOVAL_REQUIRED_MISSES = 3
AUTO_REMOVAL_MIN_SPAN_SECONDS = 30 * 60
AUTO_REMOVAL_WINDOW_DAYS = 7
TERMINAL_BOUT_STATUSES = {"cancelled", "replaced"}
VALID_COVERAGE_KINDS = {"complete", "partial"}
VALID_PRESENCE_DISPOSITIONS = {
    "missing_pending",
    "missing_review_required",
    "removal_confirmed",
}


class CardChangePolicyInputError(ValueError):
    """Raised when the policy boundary receives invalid state or evidence."""


@dataclass(frozen=True)
class CardCoverageObservation:
    """One source run's declaration of which bouts its card payload contains."""

    coverage_id: str
    source_kind: str
    observed_at: str
    event_id: int
    source_event_id: str
    source_ref: str
    payload_hash: str
    coverage_kind: str
    present_bout_ids: tuple[int, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CardCoverageObservation":
        if not isinstance(value, Mapping):
            raise CardChangePolicyInputError("coverage must be an object.")
        present = value.get("present_bout_ids", ())
        if not _is_sequence(present):
            raise CardChangePolicyInputError(
                "coverage.present_bout_ids must be an array."
            )
        return cls(
            coverage_id=value.get("coverage_id"),
            source_kind=value.get("source_kind"),
            observed_at=value.get("observed_at"),
            event_id=value.get("event_id"),
            source_event_id=value.get("source_event_id"),
            source_ref=value.get("source_ref"),
            payload_hash=value.get("payload_hash"),
            coverage_kind=value.get("coverage_kind"),
            present_bout_ids=tuple(present),
        )

    def validate(self) -> None:
        for field in (
            "coverage_id",
            "source_event_id",
            "source_ref",
            "payload_hash",
        ):
            if not _nonempty(getattr(self, field)):
                raise CardChangePolicyInputError(
                    f"coverage.{field} must be a non-empty string."
                )
        if self.source_kind not in SOURCE_RANKS:
            raise CardChangePolicyInputError(
                f"Unsupported coverage source_kind {self.source_kind!r}."
            )
        if _parse_utc(self.observed_at) is None:
            raise CardChangePolicyInputError(
                "coverage.observed_at must be an aware UTC datetime."
            )
        if not _positive_int(self.event_id):
            raise CardChangePolicyInputError(
                "coverage.event_id must be a positive integer."
            )
        if self.coverage_kind not in VALID_COVERAGE_KINDS:
            raise CardChangePolicyInputError(
                "coverage.coverage_kind must be complete or partial."
            )
        if any(not _positive_int(item) for item in self.present_bout_ids):
            raise CardChangePolicyInputError(
                "coverage.present_bout_ids must contain positive integers."
            )
        if len(set(self.present_bout_ids)) != len(self.present_bout_ids):
            raise CardChangePolicyInputError(
                "coverage.present_bout_ids cannot contain duplicates."
            )

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["present_bout_ids"] = list(self.present_bout_ids)
        return value


@dataclass(frozen=True)
class BoutPresenceState:
    """Minimal replay-safe evidence state for one currently missing bout."""

    event_id: int
    bout_id: int
    disposition: str
    consecutive_complete_misses: int
    first_qualifying_missing_at: Optional[str]
    last_missing_at: str
    last_coverage_id: str
    last_payload_hash: str
    source_kind: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BoutPresenceState":
        if not isinstance(value, Mapping):
            raise CardChangePolicyInputError(
                "Every previous presence state must be an object."
            )
        return cls(
            event_id=value.get("event_id"),
            bout_id=value.get("bout_id"),
            disposition=value.get("disposition"),
            consecutive_complete_misses=value.get(
                "consecutive_complete_misses"
            ),
            first_qualifying_missing_at=value.get(
                "first_qualifying_missing_at"
            ),
            last_missing_at=value.get("last_missing_at"),
            last_coverage_id=value.get("last_coverage_id"),
            last_payload_hash=value.get("last_payload_hash"),
            source_kind=value.get("source_kind"),
        )

    def validate(self) -> None:
        if not _positive_int(self.event_id) or not _positive_int(self.bout_id):
            raise CardChangePolicyInputError(
                "Presence state event_id/bout_id must be positive integers."
            )
        if self.disposition not in VALID_PRESENCE_DISPOSITIONS:
            raise CardChangePolicyInputError(
                f"Unsupported presence disposition {self.disposition!r}."
            )
        if (
            not isinstance(self.consecutive_complete_misses, int)
            or isinstance(self.consecutive_complete_misses, bool)
            or self.consecutive_complete_misses < 0
        ):
            raise CardChangePolicyInputError(
                "consecutive_complete_misses must be a non-negative integer."
            )
        if _parse_utc(self.last_missing_at) is None:
            raise CardChangePolicyInputError(
                "Presence state last_missing_at must be an aware UTC datetime."
            )
        if self.first_qualifying_missing_at is not None and _parse_utc(
            self.first_qualifying_missing_at
        ) is None:
            raise CardChangePolicyInputError(
                "first_qualifying_missing_at must be null or an aware UTC datetime."
            )
        if not _nonempty(self.last_coverage_id) or not _nonempty(
            self.last_payload_hash
        ):
            raise CardChangePolicyInputError(
                "Presence state coverage identity must be non-empty."
            )
        if self.source_kind not in SOURCE_RANKS:
            raise CardChangePolicyInputError(
                f"Unsupported presence source_kind {self.source_kind!r}."
            )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PolicyFinding:
    code: str
    severity: str
    event_id: int
    bout_id: Optional[int]
    message: str
    action: str
    evidence: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["evidence"] = copy.deepcopy(dict(self.evidence))
        return value


@dataclass(frozen=True)
class CardChangePolicyResult:
    normalization: NormalizationResult
    presence_states: tuple[BoutPresenceState, ...]
    policy_change_set: Mapping[str, Any]
    findings: tuple[PolicyFinding, ...]
    #: Removal observations the policy synthesised for this run. The policy is
    #: pure, so it only normalises them in memory; a caller that persists has to
    #: feed these to the canonical writer or the confirmed removal never lands.
    #: Kept out of `as_dict` on purpose: it is a caller handle, not policy state
    #: (the decision itself is already recorded in `policy_change_set`).
    synthetic_observations: tuple[CardDataObservation, ...] = ()

    @property
    def snapshot(self) -> Mapping[str, Any]:
        return self.normalization.snapshot

    @property
    def quarantines(self) -> tuple[Any, ...]:
        return self.normalization.quarantines

    def as_dict(self) -> dict[str, Any]:
        return {
            "normalization": self.normalization.as_dict(),
            "presence_states": [item.as_dict() for item in self.presence_states],
            "policy_change_set": copy.deepcopy(dict(self.policy_change_set)),
            "findings": [item.as_dict() for item in self.findings],
        }


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _parse_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return parsed.astimezone(timezone.utc)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(
        _canonical(value).encode("utf-8")
    ).hexdigest()


def _index(
    values: Any, key: str
) -> dict[int, Mapping[str, Any]]:
    if not _is_sequence(values):
        return {}
    return {
        item[key]: item
        for item in values
        if isinstance(item, Mapping) and _positive_int(item.get(key))
    }


def _normalize_observations(
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
) -> tuple[CardDataObservation, ...]:
    if not _is_sequence(observations) or not observations:
        raise CardChangePolicyInputError("At least one observation is required.")
    normalized = []
    seen = set()
    for value in observations:
        try:
            item = (
                value
                if isinstance(value, CardDataObservation)
                else CardDataObservation.from_mapping(value)
            )
            item.validate()
        except (TypeError, ValueError) as exc:
            raise CardChangePolicyInputError(str(exc)) from exc
        if item.observation_id in seen:
            raise CardChangePolicyInputError(
                f"Duplicate observation_id {item.observation_id!r}."
            )
        seen.add(item.observation_id)
        normalized.append(item)
    event_ids = {item.event_id for item in normalized}
    if len(event_ids) != 1:
        raise CardChangePolicyInputError(
            "One policy call may contain observations for exactly one event."
        )
    return tuple(
        sorted(
            normalized,
            key=lambda item: (
                item.observed_at,
                item.source_kind,
                item.entity_type,
                item.entity_id,
                item.observation_id,
            ),
        )
    )


def _normalize_states(
    states: Sequence[BoutPresenceState | Mapping[str, Any]], event_id: int
) -> dict[int, BoutPresenceState]:
    if not _is_sequence(states):
        raise CardChangePolicyInputError(
            "previous_presence_states must be an array."
        )
    result = {}
    for value in states:
        item = (
            value
            if isinstance(value, BoutPresenceState)
            else BoutPresenceState.from_mapping(value)
        )
        item.validate()
        if item.event_id != event_id:
            raise CardChangePolicyInputError(
                "Presence state belongs to a different event."
            )
        if item.bout_id in result:
            raise CardChangePolicyInputError(
                f"Duplicate presence state for bout {item.bout_id}."
            )
        result[item.bout_id] = item
    return result


def _finding(
    code: str,
    event_id: int,
    bout_id: Optional[int],
    message: str,
    action: str,
    evidence: Optional[Mapping[str, Any]] = None,
    severity: str = "warning",
) -> PolicyFinding:
    return PolicyFinding(
        code=code,
        severity=severity,
        event_id=event_id,
        bout_id=bout_id,
        message=message,
        action=action,
        evidence=copy.deepcopy(dict(evidence or {})),
    )


def _finding_sort_key(item: PolicyFinding) -> tuple[Any, ...]:
    return (
        item.code,
        -1 if item.bout_id is None else item.bout_id,
        item.message,
        _canonical(item.evidence),
    )


def _valid_bout_transition(
    previous_status: Any,
    proposed_status: Any,
    observation: CardDataObservation,
    replacement_targets: set[int],
) -> bool:
    if not isinstance(proposed_status, str) or proposed_status == previous_status:
        return True
    if previous_status in TERMINAL_BOUT_STATUSES:
        return False
    if proposed_status == "replaced":
        return observation.entity_id in replacement_targets
    allowed = {
        "scheduled": {"live", "completed", "postponed", "cancelled"},
        "postponed": {"scheduled", "cancelled"},
        "live": {"completed", "cancelled"},
        "completed": set(),
    }
    if previous_status == "completed" and proposed_status == "scheduled":
        return (
            observation.source_kind == "admin_override"
            and "result" in observation.clear_fields
        )
    return proposed_status in allowed.get(previous_status, set())


def _guard_lifecycle(
    observations: Sequence[CardDataObservation],
    previous_snapshot: Optional[Mapping[str, Any]],
    event_id: int,
) -> tuple[tuple[CardDataObservation, ...], list[PolicyFinding]]:
    if previous_snapshot is None:
        return tuple(observations), []
    previous_bouts = _index(previous_snapshot.get("bouts"), "bout_id")
    replacement_targets = {
        item.values.get("replaces_bout_id")
        for item in observations
        if item.entity_type == "bout"
        and _positive_int(item.values.get("replaces_bout_id"))
    }
    guarded = []
    findings = []
    for item in observations:
        previous = previous_bouts.get(item.entity_id, {})
        previous_status = previous.get("status")
        values = copy.deepcopy(dict(item.values))
        clear_fields = list(item.clear_fields)
        if item.entity_type == "bout" and previous:
            proposed_status = values.get("status")
            if "status" in values and not _valid_bout_transition(
                previous_status,
                proposed_status,
                item,
                replacement_targets,
            ):
                values.pop("status", None)
                clear_fields = [field for field in clear_fields if field != "status"]
                findings.append(
                    _finding(
                        "ILLEGAL_BOUT_LIFECYCLE_TRANSITION",
                        event_id,
                        item.entity_id,
                        (
                            f"Blocked bout status transition {previous_status!r} -> "
                            f"{proposed_status!r}."
                        ),
                        "retain_previous_status",
                        {
                            "observation_id": item.observation_id,
                            "source_kind": item.source_kind,
                            "observed_at": item.observed_at,
                        },
                        severity="error",
                    )
                )
        if (
            item.entity_type == "slot"
            and previous_status in TERMINAL_BOUT_STATUSES
            and values.get("is_current") is True
        ):
            values.pop("is_current", None)
            clear_fields = [
                field for field in clear_fields if field != "is_current"
            ]
            findings.append(
                _finding(
                    "TERMINAL_SLOT_REVIVAL_BLOCKED",
                    event_id,
                    item.entity_id,
                    "A cancelled/replaced bout cannot silently become current again.",
                    "retain_historical_slot",
                    {
                        "observation_id": item.observation_id,
                        "source_kind": item.source_kind,
                        "observed_at": item.observed_at,
                    },
                    severity="error",
                )
            )
        guarded.append(
            replace(item, values=values, clear_fields=tuple(clear_fields))
        )
    return tuple(guarded), findings


def _coverage_window_reason(
    previous_snapshot: Mapping[str, Any], coverage: CardCoverageObservation
) -> Optional[str]:
    event = previous_snapshot.get("event", {})
    if event.get("status") != "scheduled":
        return "EVENT_NOT_SCHEDULED"
    if event.get("first_final_result_at") is not None:
        return "FIRST_RESULT_ALREADY_RECORDED"
    lock_at = _parse_utc(
        event.get("picks_lock_time_utc") or event.get("card_start_time_utc")
    )
    if lock_at is None:
        return "CARD_LOCK_UNRESOLVED"
    observed_at = _parse_utc(coverage.observed_at)
    if observed_at is None:
        return "COVERAGE_TIME_INVALID"
    generated_at = _parse_utc(previous_snapshot.get("generated_at"))
    if generated_at is not None and observed_at < generated_at:
        return "COVERAGE_BEFORE_CANONICAL_SNAPSHOT"
    if observed_at >= lock_at:
        return "CARD_ALREADY_LOCKED"
    if observed_at < lock_at - timedelta(days=AUTO_REMOVAL_WINDOW_DAYS):
        return "OUTSIDE_PRE_LOCK_WINDOW"
    return None


def _coverage_evidence(coverage: CardCoverageObservation) -> dict[str, Any]:
    return {
        "coverage_id": coverage.coverage_id,
        "source_kind": coverage.source_kind,
        "observed_at": coverage.observed_at,
        "source_event_id": coverage.source_event_id,
        "source_ref": coverage.source_ref,
        "payload_hash": coverage.payload_hash,
        "coverage_kind": coverage.coverage_kind,
    }


def _policy_change(
    change_type: str,
    entity_type: str,
    entity_id: int,
    field: str,
    before: Any,
    after: Any,
    evidence: Mapping[str, Any],
    impact: str,
    effects: Sequence[str],
) -> dict[str, Any]:
    return {
        "type": change_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "field": field,
        "before": copy.deepcopy(before),
        "after": copy.deepcopy(after),
        "evidence": copy.deepcopy(dict(evidence)),
        "impact": impact,
        "policy_effects": sorted(set(effects)),
    }


def _process_coverage(
    previous_snapshot: Optional[Mapping[str, Any]],
    coverage: Optional[CardCoverageObservation],
    previous_states: dict[int, BoutPresenceState],
    observations: Sequence[CardDataObservation],
    event_id: int,
) -> tuple[
    dict[int, BoutPresenceState],
    list[CardDataObservation],
    list[dict[str, Any]],
    list[PolicyFinding],
]:
    states = dict(previous_states)
    synthetic = []
    changes = []
    findings = []
    if previous_snapshot is None:
        return states, synthetic, changes, findings

    previous_bouts = _index(previous_snapshot.get("bouts"), "bout_id")
    previous_slots = _index(previous_snapshot.get("card_slots"), "bout_id")
    current_ids = {
        bout_id
        for bout_id, slot in previous_slots.items()
        if slot.get("is_current") is True
        and previous_bouts.get(bout_id, {}).get("status")
        not in TERMINAL_BOUT_STATUSES
    }
    replacement_targets = {
        item.values.get("replaces_bout_id")
        for item in observations
        if item.entity_type == "bout"
        and _positive_int(item.values.get("replaces_bout_id"))
    }
    explicit_retirements = {
        item.entity_id
        for item in observations
        if item.entity_type == "bout"
        and item.values.get("status") in {"cancelled", "postponed"}
    }
    resolved_ids = replacement_targets | explicit_retirements
    for bout_id in sorted(resolved_ids & set(states)):
        old = states.pop(bout_id)
        changes.append(
            _policy_change(
                "MISSING_EVIDENCE_RESOLVED_EXPLICITLY",
                "bout",
                bout_id,
                "presence",
                old.as_dict(),
                "explicit_card_change",
                {},
                "recompute_current_eligibility",
                (
                    "DISCARD_PENDING_MISSING_COUNTER",
                    "USE_EXPLICIT_LIFECYCLE_CHANGE",
                ),
            )
        )
    if coverage is None:
        return states, synthetic, changes, findings
    present_ids = set(coverage.present_bout_ids)
    evidence = _coverage_evidence(coverage)

    restored_ids = sorted(present_ids & set(states))
    for bout_id in restored_ids:
        old = states.pop(bout_id)
        changes.append(
            _policy_change(
                "BOUT_PRESENCE_RESTORED",
                "bout",
                bout_id,
                "presence",
                old.disposition,
                "present",
                evidence,
                "none",
                ("RETAIN_CANONICAL_BOUT", "RESET_MISSING_EVIDENCE"),
            )
        )

    missing_ids = sorted(current_ids - present_ids - resolved_ids)
    if not missing_ids:
        return states, synthetic, changes, findings
    if coverage.coverage_kind != "complete":
        findings.append(
            _finding(
                "AUTHORITATIVE_CARD_INCOMPLETE",
                event_id,
                None,
                "A partial card payload cannot advance missing-bout evidence.",
                "retain_current_card",
                evidence,
            )
        )
        return states, synthetic, changes, findings
    if coverage.source_kind != AUTHORITATIVE_COMPLETE_SOURCE:
        findings.extend(
            _finding(
                "UNEXPLAINED_REMOVAL",
                event_id,
                bout_id,
                "Only complete ESPN-detail coverage can confirm an absent bout.",
                "retain_current_bout",
                evidence,
            )
            for bout_id in missing_ids
        )
        return states, synthetic, changes, findings

    window_reason = _coverage_window_reason(previous_snapshot, coverage)
    for bout_id in missing_ids:
        old = states.get(bout_id)
        if old is not None and old.last_coverage_id == coverage.coverage_id:
            if old.last_payload_hash != coverage.payload_hash:
                raise CardChangePolicyInputError(
                    "A coverage_id cannot be replayed with a different payload_hash."
                )
            continue
        if old is not None and _parse_utc(coverage.observed_at) <= _parse_utc(
            old.last_missing_at
        ):
            findings.append(
                _finding(
                    "STALE_COVERAGE_IGNORED",
                    event_id,
                    bout_id,
                    "Older/equal missing-card evidence cannot advance the counter.",
                    "retain_previous_presence_state",
                    evidence,
                )
            )
            continue

        if window_reason is None:
            count = (
                old.consecutive_complete_misses + 1
                if old is not None
                and old.first_qualifying_missing_at is not None
                else 1
            )
            first_at = (
                old.first_qualifying_missing_at
                if old is not None
                and old.first_qualifying_missing_at is not None
                else coverage.observed_at
            )
        else:
            count = old.consecutive_complete_misses if old is not None else 0
            first_at = (
                old.first_qualifying_missing_at if old is not None else None
            )
        span_seconds = (
            (_parse_utc(coverage.observed_at) - _parse_utc(first_at)).total_seconds()
            if first_at is not None
            else 0
        )
        qualifies = (
            window_reason is None
            and count >= AUTO_REMOVAL_REQUIRED_MISSES
            and span_seconds >= AUTO_REMOVAL_MIN_SPAN_SECONDS
        )
        requires_review = window_reason in {
            "CARD_ALREADY_LOCKED",
            "EVENT_NOT_SCHEDULED",
            "FIRST_RESULT_ALREADY_RECORDED",
        }
        disposition = (
            "removal_confirmed"
            if qualifies
            else "missing_review_required"
            if requires_review
            else "missing_pending"
        )
        state = BoutPresenceState(
            event_id=event_id,
            bout_id=bout_id,
            disposition=disposition,
            consecutive_complete_misses=count,
            first_qualifying_missing_at=first_at,
            last_missing_at=coverage.observed_at,
            last_coverage_id=coverage.coverage_id,
            last_payload_hash=coverage.payload_hash,
            source_kind=coverage.source_kind,
        )
        states[bout_id] = state
        findings.append(
            _finding(
                "UNEXPLAINED_REMOVAL",
                event_id,
                bout_id,
                (
                    "Bout is absent from complete authoritative coverage; "
                    + (
                        "the repeat threshold confirms removal."
                        if qualifies
                        else "canonical state is retained pending stronger evidence."
                    )
                ),
                (
                    "propose_inferred_cancellation"
                    if qualifies
                    else "retain_current_bout"
                ),
                {
                    **evidence,
                    "qualifying_miss_count": count,
                    "required_miss_count": AUTO_REMOVAL_REQUIRED_MISSES,
                    "qualifying_span_seconds": int(span_seconds),
                    "required_span_seconds": AUTO_REMOVAL_MIN_SPAN_SECONDS,
                    "window_reason": window_reason,
                },
            )
        )
        changes.append(
            _policy_change(
                (
                    "REMOVAL_CONFIRMED_BY_ABSENCE_POLICY"
                    if qualifies
                    else "BOUT_MISSING_PENDING"
                ),
                "bout",
                bout_id,
                "presence",
                old.as_dict() if old is not None else "present",
                state.as_dict(),
                evidence,
                "void_direct_targets" if qualifies else "manual_review",
                (
                    (
                        "RECOMPUTE_CURRENT_ELIGIBILITY",
                        "VOID_DIRECT_TARGETS",
                        "KEEP_FROZEN_DENOMINATORS",
                        "EVALUATE_FROZEN_AGGREGATES",
                    )
                    if qualifies
                    else (
                        "RETAIN_CANONICAL_BOUT",
                        "NO_ELIGIBILITY_CHANGE",
                    )
                ),
            )
        )
        if qualifies:
            synthetic.append(
                CardDataObservation(
                    observation_id=(
                        f"policy-removal:{coverage.coverage_id}:{bout_id}"
                    ),
                    source_kind=coverage.source_kind,
                    observed_at=coverage.observed_at,
                    event_id=event_id,
                    entity_type="bout",
                    entity_id=bout_id,
                    source_ref=f"CardChangePolicyV1:{coverage.source_ref}",
                    source_event_id=coverage.source_event_id,
                    values={"status": "cancelled"},
                    identity_basis="canonical_id",
                    reason=(
                        "Repeated complete ESPN-detail absence satisfied the "
                        "pre-lock removal policy."
                    ),
                    payload_hash=_hash(
                        {
                            "coverage_id": coverage.coverage_id,
                            "payload_hash": coverage.payload_hash,
                            "bout_id": bout_id,
                            "policy_version": POLICY_VERSION,
                        }
                    ),
                )
            )
    return states, synthetic, changes, findings


def _effects_for_change(change: Mapping[str, Any]) -> tuple[str, ...]:
    change_type = change.get("type")
    if change_type in {"BOUT_CANCELLED", "BOUT_POSTPONED"}:
        return (
            "EVALUATE_FROZEN_AGGREGATES",
            "KEEP_FROZEN_DENOMINATORS",
            "RECOMPUTE_CURRENT_ELIGIBILITY",
            "VOID_DIRECT_TARGETS",
        )
    if change_type == "MATCHUP_REPLACED":
        return (
            "DO_NOT_COPY_PICKS_OR_MISSIONS",
            "KEEP_FROZEN_DENOMINATORS",
            "RECOMPUTE_CURRENT_ELIGIBILITY",
            "VOID_DIRECT_TARGETS",
        )
    if change_type == "EVENT_DATE_CHANGED":
        return (
            "KEEP_FROZEN_MONTHLY_EVENT_SET",
            "REVIEW_UNFROZEN_MONTHLY_MEMBERSHIP",
        )
    if change_type == "TIMING_CHANGED":
        return ("DO_NOT_REOPEN_LOCKED_MISSIONS", "RECHECK_FUTURE_LOCKS")
    if change_type in {
        "SLOT_REORDERED",
        "SECTION_CHANGED",
        "HEADLINER_CHANGED",
        "TITLE_CHANGED",
        "SLOT_CURRENT_CHANGED",
    }:
        return (
            "KEEP_SELECTED_ASSIGNMENT_SNAPSHOTS",
            "REFRESH_CURRENT_CARD_READ_MODEL",
        )
    if change_type in {"RESULT_RECORDED", "RESULT_CORRECTED", "RESULT_CLEARED"}:
        return ("RECONCILE_RESULT_CONSUMERS",)
    if change_type == "ELIGIBILITY_CHANGED":
        return (
            "KEEP_FROZEN_DENOMINATORS",
            "UPDATE_CURRENT_ELIGIBILITY_ONLY",
        )
    return ("NONE",)


def _canonical_policy_changes(
    normalization: NormalizationResult,
    previous_snapshot: Optional[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    changes = []
    for raw in normalization.change_set.get("changes", []):
        change = copy.deepcopy(dict(raw))
        change["policy_effects"] = list(_effects_for_change(change))
        changes.append(change)
    if previous_snapshot is None:
        return changes

    previous_bouts = _index(previous_snapshot.get("bouts"), "bout_id")
    current_bouts = _index(normalization.snapshot.get("bouts"), "bout_id")
    for bout_id, current in sorted(current_bouts.items()):
        previous = previous_bouts.get(bout_id)
        if previous is None:
            continue
        if (
            previous.get("status") != current.get("status")
            and current.get("status") == "postponed"
        ):
            changes.append(
                _policy_change(
                    "BOUT_POSTPONED",
                    "bout",
                    bout_id,
                    "status",
                    previous.get("status"),
                    current.get("status"),
                    current.get("evidence", {}).get("status", {}),
                    "void_direct_targets",
                    _effects_for_change({"type": "BOUT_POSTPONED"}),
                )
            )

    previous_slots = _index(previous_snapshot.get("card_slots"), "bout_id")
    current_slots = _index(normalization.snapshot.get("card_slots"), "bout_id")
    for bout_id, current in sorted(current_slots.items()):
        previous = previous_slots.get(bout_id)
        if previous is None or previous.get("is_current") == current.get(
            "is_current"
        ):
            continue
        changes.append(
            _policy_change(
                "SLOT_CURRENT_CHANGED",
                "slot",
                bout_id,
                "is_current",
                previous.get("is_current"),
                current.get("is_current"),
                current.get("evidence", {}).get("is_current", {}),
                "recompute_current_eligibility",
                _effects_for_change({"type": "SLOT_CURRENT_CHANGED"}),
            )
        )
    return changes


def _eligibility_delta(
    previous_snapshot: Optional[Mapping[str, Any]],
    current_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    before = (
        previous_snapshot.get("current_eligibility", {})
        if isinstance(previous_snapshot, Mapping)
        else {}
    )
    after = current_snapshot.get("current_eligibility", {})
    before_targets = {
        (item.get("bout_id"), item.get("matchup_revision")): item
        for item in before.get("eligible_targets", [])
        if isinstance(item, Mapping)
    }
    after_targets = {
        (item.get("bout_id"), item.get("matchup_revision")): item
        for item in after.get("eligible_targets", [])
        if isinstance(item, Mapping)
    }
    current_bouts = _index(current_snapshot.get("bouts"), "bout_id")
    current_slots = _index(current_snapshot.get("card_slots"), "bout_id")
    removed = []
    for key in sorted(set(before_targets) - set(after_targets)):
        bout_id, matchup_revision = key
        bout_status = current_bouts.get(bout_id, {}).get("status")
        slot = current_slots.get(bout_id, {})
        cause = (
            str(bout_status).upper()
            if bout_status in {"cancelled", "postponed", "replaced"}
            else "SLOT_NOT_CURRENT"
            if slot.get("is_current") is not True
            else "ELIGIBILITY_RULE_CHANGED"
        )
        removed.append(
            {
                "bout_id": bout_id,
                "matchup_revision": matchup_revision,
                "cause": cause,
            }
        )
    return {
        "before_fingerprint": before.get("fingerprint"),
        "after_fingerprint": after.get("fingerprint"),
        "before_denominator": before.get("denominator", 0),
        "after_denominator": after.get("denominator", 0),
        "added_targets": [
            copy.deepcopy(after_targets[key])
            for key in sorted(set(after_targets) - set(before_targets))
        ],
        "removed_targets": removed,
        "frozen_snapshots_mutated": False,
        "frozen_snapshot_instruction": (
            "KEEP_RECORDED_TARGETS_LINES_AND_DENOMINATORS"
        ),
    }


def _finalize_confirmed_states(
    states: dict[int, BoutPresenceState],
    snapshot: Mapping[str, Any],
    findings: list[PolicyFinding],
    event_id: int,
) -> set[int]:
    bouts = _index(snapshot.get("bouts"), "bout_id")
    suppressed = set()
    for bout_id, state in list(states.items()):
        if state.disposition != "removal_confirmed":
            continue
        if bouts.get(bout_id, {}).get("status") == "cancelled":
            continue
        states[bout_id] = replace(
            state, disposition="missing_review_required"
        )
        suppressed.add(bout_id)
        findings.append(
            _finding(
                "AUTO_REMOVAL_SUPPRESSED_BY_HIGHER_AUTHORITY",
                event_id,
                bout_id,
                "Repeat absence met the threshold but a higher-authority value retained the bout.",
                "manual_review",
                {
                    "last_coverage_id": state.last_coverage_id,
                    "qualifying_miss_count": state.consecutive_complete_misses,
                },
            )
        )
    return suppressed


def apply_card_change_policy(
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
    previous_snapshot: Optional[Mapping[str, Any]] = None,
    *,
    coverage: Optional[CardCoverageObservation | Mapping[str, Any]] = None,
    previous_presence_states: Sequence[
        BoutPresenceState | Mapping[str, Any]
    ] = (),
) -> CardChangePolicyResult:
    """Normalize one event while enforcing deterministic late-change policy."""

    items = _normalize_observations(observations)
    event_id = items[0].event_id
    previous = copy.deepcopy(dict(previous_snapshot)) if previous_snapshot else None
    if previous is not None:
        validation = validate_card_data_v1(previous)
        if not validation.is_valid:
            raise CardChangePolicyInputError(
                "previous_snapshot violates CardDataContractV1: "
                + ", ".join(validation.codes)
            )
        if previous.get("event", {}).get("event_id") != event_id:
            raise CardChangePolicyInputError(
                "previous_snapshot belongs to a different event."
            )
    elif previous_presence_states:
        raise CardChangePolicyInputError(
            "Presence state requires a previous canonical snapshot."
        )

    normalized_coverage = None
    if coverage is not None:
        normalized_coverage = (
            coverage
            if isinstance(coverage, CardCoverageObservation)
            else CardCoverageObservation.from_mapping(coverage)
        )
        normalized_coverage.validate()
        if normalized_coverage.event_id != event_id:
            raise CardChangePolicyInputError(
                "coverage belongs to a different event."
            )
        if previous is not None and normalized_coverage.source_kind == (
            AUTHORITATIVE_COMPLETE_SOURCE
        ):
            expected_source_event_id = previous.get("event", {}).get(
                "source_ids", {}
            ).get("espn_event_id")
            if (
                _nonempty(expected_source_event_id)
                and normalized_coverage.source_event_id
                != expected_source_event_id
            ):
                raise CardChangePolicyInputError(
                    "coverage.source_event_id does not match the canonical ESPN event alias."
                )

    states = _normalize_states(previous_presence_states, event_id)
    guarded, lifecycle_findings = _guard_lifecycle(items, previous, event_id)
    (
        states,
        synthetic,
        presence_changes,
        coverage_findings,
    ) = _process_coverage(
        previous,
        normalized_coverage,
        states,
        guarded,
        event_id,
    )
    normalization = normalize_card_data_v1([*guarded, *synthetic], previous)
    findings = [*lifecycle_findings, *coverage_findings]
    suppressed_removals = _finalize_confirmed_states(
        states, normalization.snapshot, findings, event_id
    )
    for change in presence_changes:
        if (
            change.get("type") == "REMOVAL_CONFIRMED_BY_ABSENCE_POLICY"
            and change.get("entity_id") in suppressed_removals
        ):
            bout_id = change["entity_id"]
            change["type"] = "REMOVAL_CONFIRMATION_BLOCKED_BY_AUTHORITY"
            change["after"] = states[bout_id].as_dict()
            change["impact"] = "manual_review"
            change["policy_effects"] = [
                "NO_ELIGIBILITY_CHANGE",
                "RETAIN_CANONICAL_BOUT",
            ]

    canonical_changes = _canonical_policy_changes(normalization, previous)
    changes = [*canonical_changes, *presence_changes]
    unique_changes = {_canonical(item): item for item in changes}
    changes = sorted(
        unique_changes.values(),
        key=lambda item: (
            item.get("type", ""),
            item.get("entity_type", ""),
            item.get("entity_id", -1),
            item.get("field", ""),
            _canonical(item.get("after")),
        ),
    )
    eligibility = _eligibility_delta(previous, normalization.snapshot)
    detected_candidates = [item.observed_at for item in guarded]
    if normalized_coverage is not None:
        detected_candidates.append(normalized_coverage.observed_at)
    detected_at = max(
        detected_candidates,
        key=lambda value: _parse_utc(value),
    )
    state_values = tuple(sorted(states.values(), key=lambda item: item.bout_id))
    change_payload = {
        "policy_version": POLICY_VERSION,
        "event_id": event_id,
        "previous_snapshot_revision": (
            previous.get("snapshot_revision") if previous else None
        ),
        "new_snapshot_revision": normalization.snapshot["snapshot_revision"],
        "detected_at": detected_at,
        "changes": changes,
        "eligibility": eligibility,
        "presence_states": [item.as_dict() for item in state_values],
    }
    policy_change_set = {
        "change_set_id": (
            f"cardpolicy_{event_id}_"
            f"{_hash(change_payload).split(':', 1)[1][:16]}"
        ),
        **change_payload,
    }
    unique_findings = {
        _canonical(item.as_dict()): item for item in findings
    }
    return CardChangePolicyResult(
        normalization=normalization,
        presence_states=state_values,
        policy_change_set=policy_change_set,
        findings=tuple(
            sorted(unique_findings.values(), key=_finding_sort_key)
        ),
        synthetic_observations=tuple(synthetic),
    )


__all__ = [
    "AUTHORITATIVE_COMPLETE_SOURCE",
    "AUTO_REMOVAL_MIN_SPAN_SECONDS",
    "AUTO_REMOVAL_REQUIRED_MISSES",
    "AUTO_REMOVAL_WINDOW_DAYS",
    "BoutPresenceState",
    "CardChangePolicyInputError",
    "CardChangePolicyResult",
    "CardCoverageObservation",
    "POLICY_VERSION",
    "PolicyFinding",
    "apply_card_change_policy",
]
