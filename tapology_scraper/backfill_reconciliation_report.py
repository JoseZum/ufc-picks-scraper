"""Sanitized, strict read-only CardData backfill dry-run (SCR-011).

The report projects the approved Q-014 production cards into CardData V1 in
memory, computes collection/field diffs, and invokes the SCR-009 slot planner.
It never applies a plan and contains no database mutation method.  Raw
documents, fighter names, credentials and before/after values are excluded from
the rendered artifact.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, TextIO

from tapology_scraper.admin_title_attestation import (
    AdminTitleAttestation,
    AdminTitleAttestationError,
    load_admin_title_attestation,
)
from tapology_scraper.card_data_normalizer import SOURCE_RANKS, normalize_card_data_v1
from tapology_scraper.production_card_audit import (
    BOUT_PROJECTION,
    EVENT_PROJECTION,
    GOLDEN_CARD_SPECS,
    GOLDEN_EVENT_IDS,
    SLOT_PROJECTION,
    GoldenCardSpec,
    LegacyCardDocuments,
    ProductionAuditConfigurationError,
    audit_legacy_card,
    load_mongo_settings,
)
from tapology_scraper.slot_reconciliation import (
    SlotReconciliationPlan,
    plan_slot_reconciliation,
)


REPORT_VERSION = "card-data-backfill-dry-run/v1"
PROJECTION_VERSION = "legacy-card-projection/v1"
BACKFILL_OBSERVED_AT = "2026-08-01T00:00:00Z"
READ_COLLECTIONS = ("events", "bouts", "event_card_slots")
WRITE_COLLECTIONS = ("events", "bouts", "event_card_slots")
REQUIRED_READY_CAPABILITIES = ("EVT", "EVT_DATE", "BOUT", "ELIG", "STRUCT", "TITLE")
BLOCKING_SEVERITIES = {"blocking", "error"}
SECTIONS = {"early_prelim", "prelim", "main"}
ACTIVE_BOUT_STATUSES = {"scheduled", "live", "completed"}
KNOWN_BOUT_STATUSES = ACTIVE_BOUT_STATUSES | {"cancelled", "postponed", "replaced"}
EXIT_REVIEWABLE = 0
EXIT_BLOCKED = 1
EXIT_CONFIGURATION_ERROR = 2

BACKFILL_EVENT_PROJECTION = {
    **EVENT_PROJECTION,
    "card_data_v1": 1,
    "source_ids": 1,
    "subtitle": 1,
    "updated_at": 1,
    "created_at": 1,
}
BACKFILL_BOUT_PROJECTION = {
    **BOUT_PROJECTION,
    "card_data_v1": 1,
    "source_ids": 1,
    "weight_class": 1,
    "gender": 1,
    "scheduled_start_time_utc": 1,
    "fighters.red.fighter_name": 1,
    "fighters.red.name": 1,
    "fighters.blue.fighter_name": 1,
    "fighters.blue.name": 1,
}
BACKFILL_SLOT_PROJECTION = {
    **SLOT_PROJECTION,
    # Required only in memory so SCR-009 can detect an immutable storage-ID
    # mismatch. The value is never included in the rendered artifact.
    "_id": 1,
    "is_co_main_event": 1,
    "card_snapshot_revision": 1,
    "canonical_slot_version": 1,
    "reconciliation_fingerprint": 1,
}

EVENT_OWNED_FIELDS = (
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
    "lifecycle_revision",
    "structure_revision",
    "timing_revision",
    "current_eligibility",
    "evidence",
)
BOUT_OWNED_FIELDS = (
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
    "is_title_fight",
    "is_bmf_title_fight",
    "title_type",
    "result_revision",
    "result",
    "evidence",
)


class BackfillDryRunConfigurationError(ValueError):
    """Safe operator-facing failure without raw document or secret contents."""


@dataclass(frozen=True)
class BackfillFinding:
    code: str
    severity: str
    scope: str
    affected_count: int
    example_ids: tuple[str, ...]
    message: str

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["example_ids"] = list(self.example_ids)
        return value


@dataclass(frozen=True)
class BackfillCardDryRun:
    event_id: int
    expected_name: str
    scenario_tags: tuple[str, ...]
    review_status: str
    snapshot_id: Optional[str]
    slot_plan_id: Optional[str]
    counts: tuple[tuple[str, int], ...]
    capability_states: tuple[tuple[str, str], ...]
    changed_fields: tuple[tuple[str, int], ...]
    findings: tuple[BackfillFinding, ...]

    @property
    def blocked(self) -> bool:
        return self.review_status == "BLOCKED"

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "expected_name": self.expected_name,
            "scenario_tags": list(self.scenario_tags),
            "review_status": self.review_status,
            "snapshot_id": self.snapshot_id,
            "slot_plan_id": self.slot_plan_id,
            "counts": dict(self.counts),
            "capability_states": dict(self.capability_states),
            "changed_fields": dict(self.changed_fields),
            "findings": [item.as_dict() for item in self.findings],
        }


@dataclass(frozen=True)
class CardBackfillProjection:
    """Internal desired state plus its sanitized operator-facing result."""

    result: BackfillCardDryRun
    snapshot: Optional[Mapping[str, Any]]
    slot_plan: Optional[SlotReconciliationPlan]


@dataclass(frozen=True)
class BackfillDryRunReport:
    cards: tuple[BackfillCardDryRun, ...]
    title_attestation_id: Optional[str] = None
    title_attestation_decision_ref: Optional[str] = None
    title_attestation_scope: tuple[int, ...] = ()
    title_attestation_explicit_bout_count: int = 0
    title_attestation_applied_bout_count: int = 0

    @property
    def blocked(self) -> bool:
        return any(card.blocked for card in self.cards)

    @property
    def exit_code(self) -> int:
        return EXIT_BLOCKED if self.blocked else EXIT_REVIEWABLE

    def summary(self) -> dict[str, Any]:
        counts = Counter()
        fields = Counter()
        findings = Counter()
        for card in self.cards:
            counts.update(dict(card.counts))
            fields.update(dict(card.changed_fields))
            findings.update(item.code for item in card.findings)
        return {
            "card_count": len(self.cards),
            "reviewable_card_count": sum(not card.blocked for card in self.cards),
            "blocked_card_count": sum(card.blocked for card in self.cards),
            "counts": dict(sorted(counts.items())),
            "changed_fields": dict(sorted(fields.items())),
            "finding_codes": dict(sorted(findings.items())),
        }

    def as_dict(self) -> dict[str, Any]:
        attestation = None
        if self.title_attestation_id is not None:
            attestation = {
                "schema_version": "admin-title-attestation/v1",
                "attestation_id": self.title_attestation_id,
                "decision_ref": self.title_attestation_decision_ref,
                "scope_event_ids": list(self.title_attestation_scope),
                "explicit_title_bout_count": (
                    self.title_attestation_explicit_bout_count
                ),
                "applied_bout_count": self.title_attestation_applied_bout_count,
                "all_other_bouts_non_title": True,
                "authorized_use": "CARD_DATA_BACKFILL_DRY_RUN_ONLY",
                "production_write_authorized": False,
            }
        return {
            "report_version": REPORT_VERSION,
            "projection_version": PROJECTION_VERSION,
            "source": "production MongoDB / strict projected reads",
            "allowlisted_event_ids": list(GOLDEN_EVENT_IDS),
            "selected_event_ids": [card.event_id for card in self.cards],
            "read_collections": list(READ_COLLECTIONS),
            "proposed_write_collections": list(WRITE_COLLECTIONS),
            "dry_run": True,
            "write_executed": False,
            "production_write_authorized": False,
            "raw_documents_retained": False,
            "contains_credentials": False,
            "contains_fighter_names": False,
            "admin_title_attestation": attestation,
            "summary": self.summary(),
            "cards": [card.as_dict() for card in self.cards],
            "recovery_plan": {
                "current_run": "No recovery required because this run performs zero writes.",
                "future_preconditions": [
                    "Capture event-scoped preimage hashes and encrypted recoverable preimages.",
                    "Require exact reviewed plan IDs and optimistic revisions.",
                    "Apply all event-scoped changes atomically or none.",
                    "Verify post-write CardData invariants and zero reconciliation drift.",
                    "Restore only the captured event-scoped preimages under separate human approval.",
                ],
            },
        }


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _document_id(document: Mapping[str, Any]) -> Optional[int]:
    value = document.get("id") or document.get("bout_id")
    return value if _positive_int(value) else None


def _utc_string(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return None


def _date_string(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        candidate = value.strip()
        if len(candidate) >= 10:
            try:
                return date.fromisoformat(candidate[:10]).isoformat()
            except ValueError:
                return None
    return None


def _canonical_slot_evidence(document: Mapping[str, Any]) -> Mapping[str, Any]:
    if str(document.get("source") or "").lower() != "card_data_v1":
        return {}
    evidence = _mapping(document.get("evidence"))
    for field in ("card_section", "order_overall", "is_current"):
        candidate = _mapping(evidence.get(field))
        if candidate.get("source_kind") in SOURCE_RANKS:
            return candidate
    return {}


def _source_kind(document: Mapping[str, Any]) -> str:
    source = str(document.get("source") or "").lower()
    if source == "card_data_v1":
        # `source` now identifies the canonical storage projection, not the
        # upstream observation that won the field. Reuse that persisted winner
        # so an inserted Tapology/fallback slot does not become ESPN evidence
        # merely because it passed through CardData V1 storage.
        persisted = _canonical_slot_evidence(document)
        return str(persisted.get("source_kind") or "deterministic_fallback")
    if "espn" in source:
        return "espn_detail"
    if "tapology" in source:
        return "tapology_explicit"
    return "deterministic_fallback"


def _evidence_time(value: Any) -> str:
    observed = _mapping(value).get("observed_at")
    return _utc_string(observed) or BACKFILL_OBSERVED_AT


def _observation(
    event_id: int,
    entity_type: str,
    entity_id: int,
    values: Mapping[str, Any],
    *,
    source_kind: str,
    source_event_id: str,
    suffix: str,
    observed_at: str = BACKFILL_OBSERVED_AT,
    reason: Optional[str] = None,
    identity_basis: str = "canonical_id",
) -> dict[str, Any]:
    resolved_reason = reason
    if source_kind == "deterministic_fallback" and not resolved_reason:
        resolved_reason = "Legacy migration projection requires manual review."
    payload = {
        "event_id": event_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "values": copy.deepcopy(dict(values)),
        "source_kind": source_kind,
        "source_event_id": source_event_id,
        "suffix": suffix,
    }
    return {
        "observation_id": f"backfill:{event_id}:{entity_type}:{entity_id}:{suffix}",
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": event_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"legacy-backfill:{entity_type}:{entity_id}:{suffix}",
        "source_event_id": source_event_id,
        "values": copy.deepcopy(dict(values)),
        "clear_fields": [],
        "identity_basis": identity_basis,
        "reason": resolved_reason,
        "payload_hash": _hash(payload),
    }


def _finding(
    code: str,
    severity: str,
    scope: str,
    ids: Iterable[Any],
    message: str,
    *,
    affected_count: Optional[int] = None,
) -> BackfillFinding:
    normalized_ids = tuple(sorted({str(item) for item in ids if item is not None})[:3])
    return BackfillFinding(
        code=code,
        severity=severity,
        scope=scope,
        affected_count=(
            affected_count if affected_count is not None else len(normalized_ids)
        ),
        example_ids=normalized_ids,
        message=message,
    )


def _finding_sort_key(item: BackfillFinding) -> tuple[Any, ...]:
    return (
        0 if item.severity in BLOCKING_SEVERITIES else 1,
        item.code,
        item.scope,
        item.example_ids,
    )


def _fighter_ref(
    raw: Mapping[str, Any],
    corner: str,
    bout_id: int,
    findings: list[BackfillFinding],
) -> Optional[dict[str, Any]]:
    raw_id = raw.get("fighter_id")
    espn_id = raw.get("espn_id")
    tapology_id = raw.get("tapology_id")
    fighter_id = (
        str(raw_id)
        if raw_id is not None
        else f"espn:{espn_id}"
        if espn_id is not None
        else f"tapology:{tapology_id}"
        if tapology_id is not None
        else None
    )
    display_name = raw.get("fighter_name") or raw.get("name")
    if not _nonempty(fighter_id) or not _nonempty(display_name):
        findings.append(
            _finding(
                "FIGHTER_BACKFILL_IDENTITY_INCOMPLETE",
                "blocking",
                "bouts",
                (bout_id,),
                "A fighter needs a stable identity and existing display snapshot.",
            )
        )
        return None
    source_ids = {}
    if espn_id is not None:
        source_ids["espn_athlete_id"] = str(espn_id)
    if tapology_id is not None:
        source_ids["tapology_fighter_id"] = str(tapology_id)
    return {
        "fighter_id": fighter_id,
        "display_name": display_name,
        "corner": corner,
        "source_ids": source_ids,
        "identity_confidence": ("exact_source" if source_ids else "resolved_alias"),
    }


def _method_family(value: Any) -> Optional[str]:
    if not _nonempty(value):
        return None
    normalized = value.lower().replace("/", "_").replace("-", "_")
    if "ko" in normalized or "tko" in normalized:
        return "ko_tko"
    if "sub" in normalized:
        return "submission"
    if "dec" in normalized:
        return "decision"
    if "dq" in normalized or "disqual" in normalized:
        return "dq"
    return "other"


def _ending_seconds(value: Any) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 300:
        return value
    if not _nonempty(value) or ":" not in value:
        return None
    minutes, seconds = value.split(":", 1)
    try:
        total = int(minutes) * 60 + int(seconds)
    except ValueError:
        return None
    return total if 0 <= total <= 300 else None


def _result_values(
    bout: Mapping[str, Any],
    fighter_refs: Sequence[Mapping[str, Any]],
    findings: list[BackfillFinding],
) -> Optional[dict[str, Any]]:
    raw = _mapping(bout.get("result"))
    if not raw:
        return None
    bout_id = _document_id(bout)
    raw_outcome = str(raw.get("outcome") or "").lower()
    winner_corner = str(raw.get("winner") or "").lower()
    outcome = (
        raw_outcome
        if raw_outcome in {"red_win", "blue_win", "draw", "no_contest"}
        else "red_win"
        if raw_outcome == "red" or winner_corner == "red"
        else "blue_win"
        if raw_outcome == "blue" or winner_corner == "blue"
        else "no_contest"
        if raw_outcome in {"nc", "no contest"}
        else "draw"
        if raw_outcome == "draw"
        else None
    )
    method_family = _method_family(raw.get("method_family") or raw.get("method"))
    if outcome is None or method_family is None:
        findings.append(
            _finding(
                "RESULT_BACKFILL_UNRESOLVED",
                "blocking",
                "bouts",
                (bout_id,),
                "Legacy result outcome/method cannot be projected canonically.",
            )
        )
        return None
    winner_id = raw.get("winner_fighter_id")
    if outcome in {"red_win", "blue_win"} and not _nonempty(winner_id):
        corner = "red" if outcome == "red_win" else "blue"
        winner_id = next(
            (
                item.get("fighter_id")
                for item in fighter_refs
                if item.get("corner") == corner
            ),
            None,
        )
    if outcome in {"draw", "no_contest"}:
        winner_id = None
    if outcome in {"red_win", "blue_win"} and not _nonempty(winner_id):
        findings.append(
            _finding(
                "RESULT_WINNER_BACKFILL_UNRESOLVED",
                "blocking",
                "bouts",
                (bout_id,),
                "Legacy result winner cannot be linked to a canonical fighter ID.",
            )
        )
        return None
    round_number = raw.get("ending_round") or raw.get("round")
    if not _positive_int(round_number):
        round_number = None
    return {
        "outcome": outcome,
        "winner_fighter_id": winner_id,
        "method_family": method_family,
        "method_detail": raw.get("method_detail") or raw.get("method"),
        "ending_round": round_number,
        "ending_time_seconds": _ending_seconds(
            raw.get("ending_time_seconds")
            if "ending_time_seconds" in raw
            else raw.get("time")
        ),
    }


def _identity_safe(
    card: LegacyCardDocuments,
    findings: list[BackfillFinding],
) -> bool:
    event = card.event
    if not isinstance(event, Mapping):
        findings.append(
            _finding(
                "EVENT_NOT_FOUND",
                "error",
                "events",
                (card.spec.event_id,),
                "The allowlisted event document does not exist.",
            )
        )
        return False
    observed_id = event.get("id")
    if observed_id != card.spec.event_id:
        findings.append(
            _finding(
                "EVENT_CANONICAL_ID_MISMATCH",
                "error",
                "events",
                (card.spec.event_id,),
                "The projected event does not match the allowlisted canonical ID.",
            )
        )
        return False
    audited = audit_legacy_card(card)
    safe = audited.observed_name_matches
    if not audited.observed_name_matches:
        findings.append(
            _finding(
                "EVENT_NAME_IDENTITY_MISMATCH",
                "error",
                "events",
                (card.spec.event_id,),
                "Observed event identity contradicts the approved golden scenario.",
            )
        )
    expected_alias = card.spec.expected_espn_event_id
    observed_alias = event.get("espn_event_id")
    if expected_alias is not None and str(observed_alias or "") != expected_alias:
        safe = False
        findings.append(
            _finding(
                "ESPN_EVENT_ALIAS_MISMATCH",
                "error",
                "events",
                (card.spec.event_id,),
                "Observed ESPN alias contradicts the approved event mapping.",
            )
        )
    return safe


def _legacy_slot_index(
    card: LegacyCardDocuments,
    findings: list[BackfillFinding],
) -> dict[int, Mapping[str, Any]]:
    by_bout: defaultdict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for slot in card.slots:
        bout_id = slot.get("bout_id")
        if _positive_int(bout_id):
            by_bout[bout_id].append(slot)
        else:
            findings.append(
                _finding(
                    "SLOT_IDENTITY_UNRESOLVED",
                    "error",
                    "event_card_slots",
                    (card.spec.event_id,),
                    "A persisted slot lacks a positive canonical bout ID.",
                )
            )
    duplicates = [bout_id for bout_id, values in by_bout.items() if len(values) > 1]
    if duplicates:
        findings.append(
            _finding(
                "SLOT_BOUT_DUPLICATE",
                "error",
                "event_card_slots",
                duplicates,
                "A bout has multiple persisted slot documents.",
                affected_count=len(duplicates),
            )
        )
    return {bout_id: values[0] for bout_id, values in by_bout.items()}


def _slot_seed(
    card: LegacyCardDocuments,
    bout: Mapping[str, Any],
    slot: Optional[Mapping[str, Any]],
    findings: list[BackfillFinding],
) -> Optional[dict[str, Any]]:
    bout_id = _document_id(bout)
    if bout_id is None:
        return None
    source = slot or bout
    section = source.get("card_section")
    order = source.get("order_overall") or source.get("card_order")
    status = bout.get("status")
    current = (
        source.get("is_current")
        if isinstance(source.get("is_current"), bool)
        else status in ACTIVE_BOUT_STATUSES
    )
    if status in {"cancelled", "replaced"}:
        current = False
    if current and section not in SECTIONS:
        findings.append(
            _finding(
                "SECTION_BACKFILL_UNRESOLVED",
                "blocking",
                "event_card_slots",
                (bout_id,),
                "A current bout has no trusted canonical card section.",
            )
        )
        return None
    if current and not _positive_int(order):
        findings.append(
            _finding(
                "ORDER_BACKFILL_UNRESOLVED",
                "blocking",
                "event_card_slots",
                (bout_id,),
                "A current bout has no trusted global card order.",
            )
        )
        return None
    if slot is None and current:
        findings.append(
            _finding(
                "SLOT_DERIVED_FROM_FLATTENED_BOUT",
                "warning",
                "bouts,event_card_slots",
                (bout_id,),
                "Missing current slot can be proposed from legacy flattened placement.",
            )
        )
    source_evidence = _canonical_slot_evidence(source)
    return {
        "bout_id": bout_id,
        "is_current": bool(current),
        "card_section": section,
        "order_overall": order,
        "scheduled_start_time_utc": _utc_string(
            source.get("scheduled_start_time_utc")
            or bout.get("scheduled_start_time_utc")
        ),
        "automatic_lock_time_utc": _utc_string(
            source.get("automatic_lock_time_utc") or bout.get("automatic_lock_time_utc")
        ),
        "source": source,
        "observed_at": _evidence_time(source_evidence),
    }


def _title_observation(
    event_id: int,
    source_event_id: str,
    bout: Mapping[str, Any],
    *,
    attested_values: Optional[Mapping[str, Any]] = None,
    attestation: Optional[AdminTitleAttestation] = None,
) -> Optional[dict[str, Any]]:
    if attested_values is not None:
        if attestation is None:
            raise AdminTitleAttestationError(
                "Attested TITLE values require their validated attestation."
            )
        return _observation(
            event_id,
            "bout",
            _document_id(bout),
            attested_values,
            source_kind="admin_override",
            source_event_id=source_event_id,
            suffix=f"admin-title-{attestation.attestation_id[-12:]}",
            observed_at=attestation.attested_at,
            reason=(
                "Apply the complete Admin TITLE baseline approved for this dry-run "
                f"under {attestation.decision_ref}."
            ),
            identity_basis="admin",
        )
    evidence = _mapping(_mapping(bout.get("evidence")).get("is_title_fight"))
    if evidence.get("source_kind") != "admin_override" or bout.get(
        "is_title_fight"
    ) not in {True, False}:
        return None
    is_title = bout.get("is_title_fight")
    title_type = bout.get("title_type")
    if is_title is False:
        title_type = "none"
    elif title_type in {None, "none"}:
        title_type = "unknown"
    return _observation(
        event_id,
        "bout",
        _document_id(bout),
        {
            "is_title_fight": is_title,
            "is_bmf_title_fight": bool(bout.get("is_bmf_title_fight")),
            "title_type": title_type,
        },
        source_kind="admin_override",
        source_event_id=source_event_id,
        suffix="admin-title",
        observed_at=_evidence_time(evidence),
        reason="Preserve verified legacy Admin title authority.",
        identity_basis="admin",
    )


def _build_observations(
    card: LegacyCardDocuments,
    findings: list[BackfillFinding],
    attestation: Optional[AdminTitleAttestation] = None,
) -> Optional[list[dict[str, Any]]]:
    if not _identity_safe(card, findings):
        return None
    event = _mapping(card.event)
    event_id = card.spec.event_id
    espn_event_id = event.get("espn_event_id")
    source_event_id = str(espn_event_id or f"legacy-event-{event_id}")
    official_date = _date_string(event.get("official_event_date") or event.get("date"))
    if official_date is None:
        findings.append(
            _finding(
                "OFFICIAL_DATE_BACKFILL_UNRESOLVED",
                "blocking",
                "events",
                (event_id,),
                "Legacy event date cannot establish official_event_date.",
            )
        )
    event_status = event.get("status")
    if event_status not in {"scheduled", "live", "completed", "cancelled", "postponed"}:
        findings.append(
            _finding(
                "EVENT_STATUS_BACKFILL_UNRESOLVED",
                "blocking",
                "events",
                (event_id,),
                "Legacy event status is outside CardData V1 lifecycle.",
            )
        )
    if official_date is None or event_status not in {
        "scheduled",
        "live",
        "completed",
        "cancelled",
        "postponed",
    }:
        return None

    observations = []
    event_source_ids = copy.deepcopy(_mapping(event.get("source_ids")))
    if espn_event_id is not None:
        event_source_ids["espn_event_id"] = str(espn_event_id)
    event_values = {
        "source_ids": event_source_ids,
        "promotion": event.get("promotion") or "UFC",
        "name": event.get("name"),
        "subtitle": event.get("subtitle"),
        "official_event_date": official_date,
        "official_date_timezone": (
            event.get("official_date_timezone")
            if event.get("official_date_timezone") == "America/New_York"
            else "America/New_York"
        ),
        "status": event_status,
    }
    observations.append(
        _observation(
            event_id,
            "event",
            event_id,
            event_values,
            source_kind=(
                "espn_summary" if espn_event_id is not None else _source_kind(event)
            ),
            source_event_id=source_event_id,
            suffix="event",
            reason=(
                "Project legacy event fields for a no-write migration review."
                if espn_event_id is None
                else None
            ),
            identity_basis="source_alias"
            if espn_event_id is not None
            else "canonical_id",
        )
    )

    slot_by_bout = _legacy_slot_index(card, findings)
    bout_by_id = {
        _document_id(bout): bout
        for bout in card.bouts
        if _document_id(bout) is not None
    }
    attested_title_values: dict[int, dict[str, Any]] = {}
    if attestation is not None and event_id in attestation.scope_event_ids:
        try:
            attested_title_values = attestation.values_for_card(
                event_id,
                tuple(sorted(bout_by_id)),
            )
        except AdminTitleAttestationError:
            findings.append(
                _finding(
                    "ADMIN_TITLE_ATTESTATION_INVALID",
                    "error",
                    "bouts",
                    (event_id,),
                    "Admin TITLE baseline does not match the observed card scope.",
                )
            )
            return None
        findings.append(
            _finding(
                "ADMIN_TITLE_ATTESTATION_APPLIED",
                "info",
                "bouts",
                (event_id,),
                "Complete Admin TITLE baseline applied to the no-write projection.",
                affected_count=len(attested_title_values),
            )
        )
    orphan_slots = sorted(set(slot_by_bout) - set(bout_by_id))
    if orphan_slots:
        findings.append(
            _finding(
                "ORPHAN_SLOT_BLOCKS_BACKFILL",
                "error",
                "event_card_slots",
                orphan_slots,
                "Persisted slots reference bouts absent from the projected event.",
                affected_count=len(orphan_slots),
            )
        )

    slot_seeds = []
    fighter_refs_by_bout = {}
    for bout_id, bout in sorted(bout_by_id.items()):
        if bout.get("event_id") != event_id:
            findings.append(
                _finding(
                    "CROSS_EVENT_BOUT_BLOCKS_BACKFILL",
                    "error",
                    "bouts",
                    (bout_id,),
                    "Bout event_id differs from its allowlisted event.",
                )
            )
            continue
        status = bout.get("status")
        if status not in KNOWN_BOUT_STATUSES:
            findings.append(
                _finding(
                    "BOUT_STATUS_BACKFILL_UNRESOLVED",
                    "blocking",
                    "bouts",
                    (bout_id,),
                    "Bout status is outside CardData V1 lifecycle.",
                )
            )
            continue
        raw_fighters = _mapping(bout.get("fighters"))
        fighter_refs = [
            _fighter_ref(_mapping(raw_fighters.get(corner)), corner, bout_id, findings)
            for corner in ("red", "blue")
        ]
        if any(item is None for item in fighter_refs):
            continue
        fighter_refs = [item for item in fighter_refs if item is not None]
        fighter_refs_by_bout[bout_id] = fighter_refs
        rounds = bout.get("scheduled_rounds") or bout.get("rounds_scheduled")
        if rounds not in {3, 5}:
            findings.append(
                _finding(
                    "SCHEDULED_ROUNDS_BACKFILL_UNRESOLVED",
                    "blocking",
                    "bouts",
                    (bout_id,),
                    "Bout needs a trusted 3/5 scheduled-round value.",
                )
            )
            continue
        source_kind = _source_kind(bout)
        bout_source_ids = copy.deepcopy(_mapping(bout.get("source_ids")))
        if bout.get("espn_competition_id") is not None:
            bout_source_ids["espn_competition_id"] = str(
                bout.get("espn_competition_id")
            )
        bout_values = {
            "source_ids": bout_source_ids,
            "lineage_id": bout.get("lineage_id") or f"lineage_{bout_id}",
            "replaces_bout_id": bout.get("replaces_bout_id"),
            "replaced_by_bout_id": bout.get("replaced_by_bout_id"),
            "status": status,
            "fighters": fighter_refs,
            "weight_class": bout.get("weight_class"),
            "gender": bout.get("gender") or "unknown",
            "scheduled_rounds": rounds,
        }
        observations.append(
            _observation(
                event_id,
                "bout",
                bout_id,
                bout_values,
                source_kind=source_kind,
                source_event_id=source_event_id,
                suffix="bout",
                reason="Project retained legacy bout for migration review.",
                identity_basis=(
                    "source_competition_id"
                    if bout.get("espn_competition_id") is not None
                    else "canonical_id"
                ),
            )
        )
        title = _title_observation(
            event_id,
            source_event_id,
            bout,
            attested_values=attested_title_values.get(bout_id),
            attestation=attestation,
        )
        if title is not None:
            observations.append(title)
        result = _result_values(bout, fighter_refs, findings)
        if result is not None:
            observations.append(
                _observation(
                    event_id,
                    "result",
                    bout_id,
                    result,
                    source_kind=(
                        "espn_detail" if source_kind == "espn_detail" else source_kind
                    ),
                    source_event_id=source_event_id,
                    suffix="result",
                    reason="Project legacy final result for migration review.",
                    identity_basis=(
                        "source_competition_id"
                        if bout.get("espn_competition_id") is not None
                        else "canonical_id"
                    ),
                )
            )
        seed = _slot_seed(card, bout, slot_by_bout.get(bout_id), findings)
        if seed is not None:
            slot_seeds.append(seed)

    if any(
        item.severity in BLOCKING_SEVERITIES
        and item.code
        in {
            "FIGHTER_BACKFILL_IDENTITY_INCOMPLETE",
            "SECTION_BACKFILL_UNRESOLVED",
            "ORDER_BACKFILL_UNRESOLVED",
            "BOUT_STATUS_BACKFILL_UNRESOLVED",
            "SCHEDULED_ROUNDS_BACKFILL_UNRESOLVED",
            "ORPHAN_SLOT_BLOCKS_BACKFILL",
            "SLOT_BOUT_DUPLICATE",
        }
        for item in findings
    ):
        return None

    current_seeds = [item for item in slot_seeds if item["is_current"]]
    duplicate_orders = [
        order
        for order, count in Counter(
            item["order_overall"] for item in current_seeds
        ).items()
        if count > 1
    ]
    if duplicate_orders:
        findings.append(
            _finding(
                "ORDER_BACKFILL_AMBIGUOUS",
                "error",
                "event_card_slots",
                duplicate_orders,
                "Duplicate global order cannot be repaired by an arbitrary bout-ID tie-break.",
                affected_count=len(duplicate_orders),
            )
        )
        return None
    current_seeds.sort(key=lambda item: (item["order_overall"], item["bout_id"]))
    section_times: defaultdict[str, list[str]] = defaultdict(list)
    section_locks: defaultdict[str, list[str]] = defaultdict(list)
    for new_order, seed in enumerate(current_seeds, start=1):
        seed["order_overall"] = new_order
        if seed["scheduled_start_time_utc"]:
            section_times[seed["card_section"]].append(seed["scheduled_start_time_utc"])
        if seed["automatic_lock_time_utc"]:
            section_locks[seed["card_section"]].append(seed["automatic_lock_time_utc"])
    timing_values = {
        "section_start_times_utc": {
            section: min(values) for section, values in sorted(section_times.items())
        },
        "section_lock_times_utc": {
            section: min(values) for section, values in sorted(section_locks.items())
        },
    }
    if timing_values["section_start_times_utc"]:
        timing_values["card_start_time_utc"] = min(
            timing_values["section_start_times_utc"].values()
        )
    if timing_values["section_lock_times_utc"]:
        timing_values["picks_lock_time_utc"] = min(
            timing_values["section_lock_times_utc"].values()
        )
    observations.append(
        _observation(
            event_id,
            "event",
            event_id,
            timing_values,
            source_kind="espn_detail"
            if espn_event_id is not None
            else "deterministic_fallback",
            source_event_id=source_event_id,
            suffix="section-timing",
            reason="Derive event timing maps from retained legacy slot anchors.",
            identity_basis="source_alias"
            if espn_event_id is not None
            else "canonical_id",
        )
    )
    for seed in sorted(slot_seeds, key=lambda item: item["bout_id"]):
        source_kind = _source_kind(seed["source"])
        observations.append(
            _observation(
                event_id,
                "slot",
                seed["bout_id"],
                {
                    "is_current": seed["is_current"],
                    "card_section": seed["card_section"],
                    "order_overall": seed["order_overall"],
                    "scheduled_start_time_utc": seed["scheduled_start_time_utc"],
                    "automatic_lock_time_utc": seed["automatic_lock_time_utc"],
                },
                source_kind=source_kind,
                source_event_id=source_event_id,
                suffix="slot",
                observed_at=seed["observed_at"],
                reason="Project legacy slot as the canonical placement authority.",
                identity_basis="canonical_id",
            )
        )
    return observations


def _current_event_projection(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_ids": (
            copy.deepcopy(event.get("source_ids"))
            if isinstance(event.get("source_ids"), Mapping)
            else (
                {"espn_event_id": str(event.get("espn_event_id"))}
                if event.get("espn_event_id") is not None
                else {}
            )
        ),
        "promotion": event.get("promotion"),
        "name": event.get("name"),
        "subtitle": event.get("subtitle"),
        "official_event_date": _date_string(event.get("official_event_date")),
        "official_date_timezone": event.get("official_date_timezone"),
        "month_key": event.get("month_key"),
        "status": event.get("status"),
        "card_start_time_utc": _utc_string(event.get("card_start_time_utc")),
        "picks_lock_time_utc": _utc_string(event.get("picks_lock_time_utc")),
        "section_start_times_utc": copy.deepcopy(
            event.get("section_start_times_utc") or {}
        ),
        "section_lock_times_utc": copy.deepcopy(
            event.get("section_lock_times_utc") or {}
        ),
        "first_final_result_at": _utc_string(event.get("first_final_result_at")),
        "main_event_bout_id": event.get("main_event_bout_id"),
        "listed_bout_count": event.get("listed_bout_count"),
        "mission_eligible_bout_count": event.get("mission_eligible_bout_count"),
        "lifecycle_revision": event.get("lifecycle_revision"),
        "structure_revision": event.get("structure_revision"),
        "timing_revision": event.get("timing_revision"),
        "current_eligibility": copy.deepcopy(event.get("current_eligibility")),
        "evidence": copy.deepcopy(event.get("evidence")),
    }


def _current_bout_projection(bout: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_ids": (
            copy.deepcopy(bout.get("source_ids"))
            if isinstance(bout.get("source_ids"), Mapping)
            else (
                {"espn_competition_id": str(bout.get("espn_competition_id"))}
                if bout.get("espn_competition_id") is not None
                else {}
            )
        ),
        "lineage_id": bout.get("lineage_id"),
        "matchup_revision": bout.get("matchup_revision"),
        "replaces_bout_id": bout.get("replaces_bout_id"),
        "replaced_by_bout_id": bout.get("replaced_by_bout_id"),
        "status": bout.get("status"),
        "fighters": copy.deepcopy(bout.get("fighters")),
        "weight_class": bout.get("weight_class"),
        "gender": bout.get("gender"),
        "scheduled_rounds": bout.get("scheduled_rounds")
        or bout.get("rounds_scheduled"),
        "is_title_fight": bout.get("is_title_fight"),
        "is_bmf_title_fight": bout.get("is_bmf_title_fight"),
        "title_type": bout.get("title_type"),
        "result_revision": bout.get("result_revision"),
        "result": copy.deepcopy(bout.get("result")),
        "evidence": copy.deepcopy(bout.get("evidence")),
    }


def _field_diff(
    current: Mapping[str, Any],
    desired: Mapping[str, Any],
    fields: Sequence[str],
    collection: str,
    counter: Counter[str],
) -> tuple[str, ...]:
    changed = []
    for field in fields:
        if _canonical(current.get(field)) != _canonical(desired.get(field)):
            changed.append(field)
            counter[f"{collection}.{field}"] += 1
    return tuple(changed)


def _operation_counts_and_fields(
    card: LegacyCardDocuments,
    snapshot: Mapping[str, Any],
    slot_plan: Any,
) -> tuple[Counter[str], Counter[str]]:
    counts: Counter[str] = Counter(
        {
            "events_read": 1 if card.event is not None else 0,
            "bouts_read": len(card.bouts),
            "slots_read": len(card.slots),
            "desired_events": 1,
            "desired_bouts": len(snapshot.get("bouts", [])),
            "desired_slots": len(snapshot.get("card_slots", [])),
        }
    )
    fields: Counter[str] = Counter()
    current_event = _current_event_projection(_mapping(card.event))
    desired_event = _mapping(snapshot.get("event"))
    event_changed = _field_diff(
        current_event,
        desired_event,
        EVENT_OWNED_FIELDS,
        "events",
        fields,
    )
    counts["event_updates"] = int(bool(event_changed))
    counts["event_noops"] = int(not event_changed)

    current_bouts = {
        _document_id(item): item
        for item in card.bouts
        if _document_id(item) is not None
    }
    for desired in snapshot.get("bouts", []):
        bout_id = desired.get("bout_id")
        current = current_bouts.get(bout_id)
        if current is None:
            counts["bout_inserts"] += 1
            for field in BOUT_OWNED_FIELDS:
                fields[f"bouts.{field}"] += 1
            continue
        changed = _field_diff(
            _current_bout_projection(current),
            desired,
            BOUT_OWNED_FIELDS,
            "bouts",
            fields,
        )
        counts["bout_updates" if changed else "bout_noops"] += 1

    slot_summary = slot_plan.summary()
    counts["slot_inserts"] = slot_summary["insert_count"]
    counts["slot_updates"] = slot_summary["update_count"]
    counts["slot_noops"] = slot_summary["unchanged_count"]
    counts["slot_conflicts"] = slot_summary["conflict_count"]
    for operation in slot_plan.operations:
        for field in operation.changed_fields:
            fields[f"event_card_slots.{field}"] += 1
    return counts, fields


def _audit_context_findings(
    card: LegacyCardDocuments,
    *,
    has_admin_title_attestation: bool = False,
) -> list[BackfillFinding]:
    audited = audit_legacy_card(card)
    material_codes = {
        "KNOWN_CANCELLATION_NOT_RETAINED",
        "EXPECTED_SECTION_MISSING",
        "UNEXPECTED_SECTION_PRESENT",
        "FLATTENED_STRUCTURE_DISAGREEMENT",
        "EXPECTED_TITLE_BOUTS_MISSING",
    }
    findings = []
    for issue in audited.issues:
        if issue.code not in material_codes:
            continue
        if has_admin_title_attestation and issue.code == "EXPECTED_TITLE_BOUTS_MISSING":
            continue
        severity = issue.severity
        if issue.code == "FLATTENED_STRUCTURE_DISAGREEMENT":
            severity = "warning"
        findings.append(
            BackfillFinding(
                code=issue.code,
                severity=severity,
                scope=issue.collection,
                affected_count=issue.affected_count,
                example_ids=issue.example_ids,
                message=issue.message,
            )
        )
    return findings


def _quality_findings(snapshot: Mapping[str, Any]) -> list[BackfillFinding]:
    findings = []
    quality = _mapping(snapshot.get("quality"))
    for capability in REQUIRED_READY_CAPABILITIES:
        state = _mapping(quality.get("capabilities")).get(capability)
        if state != "ready":
            findings.append(
                _finding(
                    f"CAPABILITY_{capability}_NOT_READY",
                    "blocking",
                    "card_data",
                    (snapshot.get("event", {}).get("event_id"),),
                    f"Desired CardData capability {capability} is {state or 'missing'}.",
                )
            )
    event_status = snapshot.get("event", {}).get("status")
    result_state = _mapping(quality.get("capabilities")).get("RES")
    if event_status == "completed" and result_state != "ready":
        findings.append(
            _finding(
                "CAPABILITY_RES_NOT_READY_FOR_COMPLETED_EVENT",
                "blocking",
                "card_data",
                (snapshot.get("event", {}).get("event_id"),),
                "Completed cards require canonical final results before backfill review.",
            )
        )
    for issue in quality.get("issues", []):
        if not isinstance(issue, Mapping):
            continue
        code = issue.get("code")
        if code not in {
            "TITLE_STATUS_UNKNOWN",
            "EVENT_DATE_REQUIRED",
            "SECTION_UNKNOWN",
            "RESULT_NOT_FINAL",
        }:
            continue
        findings.append(
            _finding(
                str(code),
                "blocking",
                str(issue.get("scope_type") or "card_data"),
                (issue.get("scope_id"),),
                str(issue.get("message") or "Canonical capability remains unresolved."),
            )
        )
    return findings


def _empty_card_result(
    card: LegacyCardDocuments,
    findings: Sequence[BackfillFinding],
) -> BackfillCardDryRun:
    counts = (
        ("events_read", 1 if card.event is not None else 0),
        ("bouts_read", len(card.bouts)),
        ("slots_read", len(card.slots)),
        ("desired_events", 0),
        ("desired_bouts", 0),
        ("desired_slots", 0),
        ("event_updates", 0),
        ("bout_inserts", 0),
        ("bout_updates", 0),
        ("slot_inserts", 0),
        ("slot_updates", 0),
        ("slot_conflicts", 0),
    )
    return BackfillCardDryRun(
        event_id=card.spec.event_id,
        expected_name=card.spec.expected_name,
        scenario_tags=card.spec.scenario_tags,
        review_status="BLOCKED",
        snapshot_id=None,
        slot_plan_id=None,
        counts=counts,
        capability_states=(),
        changed_fields=(),
        findings=tuple(sorted(set(findings), key=_finding_sort_key)),
    )


def build_card_backfill_projection(
    card: LegacyCardDocuments,
    attestation: Optional[AdminTitleAttestation] = None,
) -> CardBackfillProjection:
    """Build desired CardData plus a sanitized deterministic dry-run result."""

    copied = copy.deepcopy(card)
    has_attestation = (
        attestation is not None and copied.spec.event_id in attestation.scope_event_ids
    )
    findings = _audit_context_findings(
        copied,
        has_admin_title_attestation=has_attestation,
    )
    observations = _build_observations(copied, findings, attestation)
    if observations is None:
        return CardBackfillProjection(
            result=_empty_card_result(copied, findings),
            snapshot=None,
            slot_plan=None,
        )
    try:
        normalization = normalize_card_data_v1(observations)
    except Exception as exc:
        findings.append(
            _finding(
                "NORMALIZATION_BACKFILL_FAILED",
                "error",
                "card_data",
                (copied.spec.event_id,),
                f"Legacy projection could not normalize ({type(exc).__name__}).",
            )
        )
        return CardBackfillProjection(
            result=_empty_card_result(copied, findings),
            snapshot=None,
            slot_plan=None,
        )

    snapshot = normalization.snapshot
    findings.extend(_quality_findings(snapshot))
    try:
        slot_plan = plan_slot_reconciliation(snapshot, copied.slots)
    except Exception as exc:
        findings.append(
            _finding(
                "SLOT_RECONCILIATION_PLAN_FAILED",
                "error",
                "event_card_slots",
                (copied.spec.event_id,),
                f"Slot reconciliation could not be planned ({type(exc).__name__}).",
            )
        )
        return CardBackfillProjection(
            result=_empty_card_result(copied, findings),
            snapshot=None,
            slot_plan=None,
        )
    for conflict in slot_plan.conflicts:
        findings.append(
            _finding(
                f"SLOT_PLAN_{conflict.code}",
                "error" if conflict.blocking else "warning",
                "event_card_slots",
                (conflict.bout_id or conflict.slot_id or copied.spec.event_id,),
                conflict.message,
            )
        )
    counts, fields = _operation_counts_and_fields(copied, snapshot, slot_plan)
    quality = _mapping(snapshot.get("quality"))
    capabilities = tuple(sorted(_mapping(quality.get("capabilities")).items()))
    unique_findings = {_canonical(item.as_dict()): item for item in findings}
    final_findings = tuple(sorted(unique_findings.values(), key=_finding_sort_key))
    blocked = (
        any(item.severity in BLOCKING_SEVERITIES for item in final_findings)
        or not slot_plan.safe_to_apply
    )
    return CardBackfillProjection(
        result=BackfillCardDryRun(
            event_id=copied.spec.event_id,
            expected_name=copied.spec.expected_name,
            scenario_tags=copied.spec.scenario_tags,
            review_status="BLOCKED" if blocked else "REVIEWABLE",
            snapshot_id=snapshot.get("snapshot_id"),
            slot_plan_id=slot_plan.plan_id,
            counts=tuple(sorted(counts.items())),
            capability_states=capabilities,
            changed_fields=tuple(sorted(fields.items())),
            findings=final_findings,
        ),
        snapshot=copy.deepcopy(snapshot),
        slot_plan=slot_plan,
    )


def build_card_dry_run(
    card: LegacyCardDocuments,
    attestation: Optional[AdminTitleAttestation] = None,
) -> BackfillCardDryRun:
    """Build one deterministic no-write projection from projected legacy docs."""

    return build_card_backfill_projection(card, attestation).result


def build_backfill_dry_run(
    cards: Iterable[LegacyCardDocuments],
    attestation: Optional[AdminTitleAttestation] = None,
) -> BackfillDryRunReport:
    card_values = tuple(cards)
    if attestation is not None:
        observed_cards = {
            card.spec.event_id: tuple(
                sorted(
                    bout_id
                    for bout_id in (_document_id(bout) for bout in card.bouts)
                    if bout_id is not None
                )
            )
            for card in card_values
        }
        try:
            attestation.validate_observed_scope(observed_cards)
        except AdminTitleAttestationError as exc:
            raise BackfillDryRunConfigurationError(
                "Admin TITLE attestation does not match the selected dry-run scope."
            ) from exc
    results = tuple(
        sorted(
            (build_card_dry_run(card, attestation) for card in card_values),
            key=lambda item: item.event_id,
        )
    )
    if attestation is None:
        return BackfillDryRunReport(results)
    return BackfillDryRunReport(
        cards=results,
        title_attestation_id=attestation.attestation_id,
        title_attestation_decision_ref=attestation.decision_ref,
        title_attestation_scope=attestation.scope_event_ids,
        title_attestation_explicit_bout_count=len(attestation.title_bouts),
        title_attestation_applied_bout_count=sum(
            len(card.bouts)
            for card in card_values
            if card.spec.event_id in attestation.scope_event_ids
        ),
    )


def fetch_backfill_cards(
    database: Any,
    specs: Sequence[GoldenCardSpec] = GOLDEN_CARD_SPECS,
) -> tuple[LegacyCardDocuments, ...]:
    """Read only the approved events and projected fields needed by SCR-011."""

    unknown_ids = sorted({spec.event_id for spec in specs} - set(GOLDEN_EVENT_IDS))
    if unknown_ids:
        raise BackfillDryRunConfigurationError(
            "Backfill specs include event IDs outside the approved Q-014 allowlist."
        )
    cards = []
    for spec in sorted(specs, key=lambda item: item.event_id):
        event = database["events"].find_one(
            {"id": spec.event_id},
            BACKFILL_EVENT_PROJECTION,
            max_time_ms=10_000,
        )
        bout_cursor = database["bouts"].find(
            {"event_id": spec.event_id},
            BACKFILL_BOUT_PROJECTION,
        )
        slot_cursor = database["event_card_slots"].find(
            {"event_id": spec.event_id},
            BACKFILL_SLOT_PROJECTION,
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


def run_backfill_dry_run(
    uri: str,
    database_name: str,
    specs: Sequence[GoldenCardSpec] = GOLDEN_CARD_SPECS,
    attestation: Optional[AdminTitleAttestation] = None,
) -> BackfillDryRunReport:
    """Run bounded majority reads and keep every proposed change in memory."""

    from pymongo import MongoClient, ReadPreference
    from pymongo.read_concern import ReadConcern

    client = MongoClient(
        uri,
        appname="ufc-picks-carddata-backfill-dry-run",
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
        return build_backfill_dry_run(
            fetch_backfill_cards(database, specs),
            attestation,
        )
    finally:
        client.close()


def render_json(report: BackfillDryRunReport) -> str:
    return (
        json.dumps(
            report.as_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n"
    )


def _markdown(value: Any) -> str:
    if value is None:
        return "—"
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def render_markdown(report: BackfillDryRunReport) -> str:
    summary = report.summary()
    counts = summary["counts"]
    lines = [
        "# CardData Backfill / Reconciliation Dry-Run",
        "",
        f"- Report contract: `{REPORT_VERSION}`",
        f"- Projection rules: `{PROJECTION_VERSION}`",
        "- Source: `production MongoDB / strict projected reads`",
        f"- Result: `{'BLOCKED' if report.blocked else 'REVIEWABLE'}`",
        f"- Allowlist: `{', '.join(str(value) for value in GOLDEN_EVENT_IDS)}`",
        "- Selected event scope: "
        f"`{', '.join(str(card.event_id) for card in report.cards)}`",
        f"- Read collections: `{', '.join(READ_COLLECTIONS)}`",
        f"- Proposed-write collections: `{', '.join(WRITE_COLLECTIONS)}`",
        "- Writes executed: `NO`",
        "- Production write authorized: `NO`",
        "- Raw documents retained: `NO`",
        "- Credentials included: `NO`",
        "- Fighter names included: `NO`",
    ]
    if report.title_attestation_id is None:
        lines.append("- Admin TITLE attestation: `NOT APPLIED`")
    else:
        lines.extend(
            [
                f"- Admin TITLE attestation: `{report.title_attestation_id}`",
                "- Admin TITLE authorized use: `CARD_DATA_BACKFILL_DRY_RUN_ONLY`",
                "- Attested TITLE scope: "
                f"`{', '.join(str(value) for value in report.title_attestation_scope)}`",
                "- Attested TITLE bouts applied: "
                f"`{report.title_attestation_applied_bout_count}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            "| Metric | Count |",
            "|---|---:|",
            f"| Cards | {summary['card_count']} |",
            f"| Reviewable cards | {summary['reviewable_card_count']} |",
            f"| Blocked cards | {summary['blocked_card_count']} |",
            f"| Event documents read | {counts.get('events_read', 0)} |",
            f"| Bout documents read | {counts.get('bouts_read', 0)} |",
            f"| Slot documents read | {counts.get('slots_read', 0)} |",
            f"| Proposed event updates | {counts.get('event_updates', 0)} |",
            f"| Proposed bout inserts | {counts.get('bout_inserts', 0)} |",
            f"| Proposed bout updates | {counts.get('bout_updates', 0)} |",
            f"| Proposed slot inserts | {counts.get('slot_inserts', 0)} |",
            f"| Proposed slot updates | {counts.get('slot_updates', 0)} |",
            f"| Slot conflicts | {counts.get('slot_conflicts', 0)} |",
            "",
            "## Proposed changed fields",
            "",
            "Only collection/field names and aggregate document counts are shown; no "
            "before/after values are retained.",
            "",
            "| Collection.field | Documents |",
            "|---|---:|",
        ]
    )
    if summary["changed_fields"]:
        for field, count in summary["changed_fields"].items():
            lines.append(f"| {_markdown(field)} | {count} |")
    else:
        lines.append("| — | 0 |")

    lines.extend(["", "## Cards"])
    for card in report.cards:
        card_counts = dict(card.counts)
        capabilities = (
            ", ".join(f"{name}={state}" for name, state in card.capability_states)
            or "unavailable"
        )
        lines.extend(
            [
                "",
                f"### {card.event_id} — {_markdown(card.expected_name)}",
                "",
                f"- Review status: `{card.review_status}`",
                f"- Scenarios: `{', '.join(card.scenario_tags)}`",
                f"- Snapshot ID: `{_markdown(card.snapshot_id)}`",
                f"- Slot plan ID: `{_markdown(card.slot_plan_id)}`",
                f"- Capabilities: `{capabilities}`",
                "- Documents: "
                f"events={card_counts.get('events_read', 0)}, "
                f"bouts={card_counts.get('bouts_read', 0)}, "
                f"slots={card_counts.get('slots_read', 0)}",
                "- Proposed operations: "
                f"event updates={card_counts.get('event_updates', 0)}, "
                f"bout inserts={card_counts.get('bout_inserts', 0)}, "
                f"bout updates={card_counts.get('bout_updates', 0)}, "
                f"slot inserts={card_counts.get('slot_inserts', 0)}, "
                f"slot updates={card_counts.get('slot_updates', 0)}, "
                f"slot conflicts={card_counts.get('slot_conflicts', 0)}",
                "",
                "| Severity | Code | Scope | Affected | Examples |",
                "|---|---|---|---:|---|",
            ]
        )
        if card.findings:
            for finding in card.findings:
                examples = ", ".join(finding.example_ids) or "—"
                lines.append(
                    f"| {finding.severity} | {finding.code} | "
                    f"{_markdown(finding.scope)} | {finding.affected_count} | "
                    f"{_markdown(examples)} |"
                )
        else:
            lines.append("| — | No findings | — | 0 | — |")

        lines.extend(
            [
                "",
                "Changed fields for this card: "
                + (
                    ", ".join(
                        f"`{_markdown(field)}` ({count})"
                        for field, count in card.changed_fields
                    )
                    if card.changed_fields
                    else "none"
                ),
            ]
        )

    lines.extend(
        [
            "",
            "## Safety and recovery gate",
            "",
            "This artifact is evidence for SCR-011/SCR-012A only. It does not contain an "
            "executable write adapter and it never applies the proposed operations.",
            "",
            "A future production backfill remains blocked until SCR-012 receives separate "
            "human authorization. Before any write, re-read the exact event scope, capture "
            "event-scoped preimage hashes plus recoverable encrypted preimages, regenerate "
            "and approve the exact snapshot/plan IDs, apply atomically with optimistic "
            "revisions, and verify CardData invariants plus zero reconciliation drift. A "
            "restore would require its own approval and use only those scoped preimages.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Project the approved Q-014 production cards into CardData V1 and emit a "
            "sanitized, strict no-write reconciliation report."
        )
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional dotenv file; credentials are never rendered.",
    )
    parser.add_argument(
        "--event-id",
        type=int,
        action="append",
        choices=GOLDEN_EVENT_IDS,
        default=None,
        help="Include an allowlisted card; repeatable. Default: all Q-014 cards.",
    )
    parser.add_argument(
        "--admin-title-attestation",
        type=Path,
        default=None,
        help=(
            "Validated Admin TITLE baseline for this dry-run only; never authorizes "
            "or executes a production write."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
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
        protected_inputs = tuple(
            path
            for path in (args.env_file, args.admin_title_attestation)
            if path is not None
        )
        if args.output is not None:
            if any(
                args.output.resolve() == path.resolve() for path in protected_inputs
            ):
                raise BackfillDryRunConfigurationError(
                    "The report path must not overwrite an input file."
                )
        selected_ids = set(args.event_id or GOLDEN_EVENT_IDS)
        specs = tuple(
            spec for spec in GOLDEN_CARD_SPECS if spec.event_id in selected_ids
        )
        attestation = (
            load_admin_title_attestation(args.admin_title_attestation)
            if args.admin_title_attestation is not None
            else None
        )
        uri, database_name = load_mongo_settings(args.env_file)
        report = run_backfill_dry_run(uri, database_name, specs, attestation)
        rendered = (
            render_json(report) if args.format == "json" else render_markdown(report)
        )
        if args.output is None:
            stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        return report.exit_code
    except (
        AdminTitleAttestationError,
        BackfillDryRunConfigurationError,
        ProductionAuditConfigurationError,
    ) as exc:
        stderr.write(f"error: {exc}\n")
        return EXIT_CONFIGURATION_ERROR
    except Exception as exc:  # Driver messages may contain credentials or endpoints.
        stderr.write(
            "error: production backfill dry-run failed "
            f"({type(exc).__name__}); no raw data was retained and no write occurred.\n"
        )
        return EXIT_CONFIGURATION_ERROR


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BACKFILL_BOUT_PROJECTION",
    "BACKFILL_EVENT_PROJECTION",
    "BACKFILL_SLOT_PROJECTION",
    "BackfillCardDryRun",
    "BackfillDryRunConfigurationError",
    "BackfillDryRunReport",
    "BackfillFinding",
    "CardBackfillProjection",
    "build_backfill_dry_run",
    "build_card_backfill_projection",
    "build_card_dry_run",
    "fetch_backfill_cards",
    "main",
    "render_json",
    "render_markdown",
    "run_backfill_dry_run",
]
