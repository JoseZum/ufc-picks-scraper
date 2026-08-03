"""SCR-015/016 continuous CardData write boundary.

Every ongoing CardData mutation — continuous ESPN observations and Admin
date/timing/lifecycle/title/result commands — must enter through this module.
The boundary owns three responsibilities and nothing else:

* rebuild the previous canonical snapshot from persisted sidecars;
* run :func:`normalize_card_data_v1` and :func:`plan_slot_reconciliation`
  over the merged observation set;
* project the resolved snapshot back into the legacy top-level fields the
  current API/UI still reads, so those fields become *derived*, never an
  independent authority.

The module is pure apart from an injected storage port.  It never opens a
database connection, never imports pymongo/scrapy/requests, and never emits a
delete: cancelled, postponed and replaced bouts are retained with lifecycle
state exactly as SCR-009/SCR-010 require.

Precedence is inherited from the normalizer rather than re-implemented here.
A persisted ``admin_override`` evidence entry outranks every ESPN/Tapology
observation, and TITLE fields are resolved exclusively from Admin candidates,
so a legacy writer cannot regain canonical authority by writing later.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Protocol

from tapology_scraper.card_data_contract import validate_card_data_v1
from tapology_scraper.card_data_normalizer import (
    SOURCE_RANKS,
    CardDataObservation,
    NormalizationInputError,
    Quarantine,
    normalize_card_data_v1,
)
from tapology_scraper.slot_reconciliation import (
    SlotReconciliationInputError,
    SlotReconciliationPlan,
    plan_slot_reconciliation,
    slot_collection_digest,
)


BOUNDARY_VERSION = "canonical-card-boundary/v1"
SNAPSHOT_SIDECAR_FIELD = "card_data_snapshot_v1"
DOCUMENT_SIDECAR_FIELD = "card_data_v1"

#: Canonical event fields the boundary owns.  A legacy writer that ``$set``s
#: any of these re-creates dual authority and is rejected by
#: :func:`assert_legacy_card_update_allowed`.
CANONICAL_EVENT_FIELDS = frozenset(
    {
        SNAPSHOT_SIDECAR_FIELD,
        DOCUMENT_SIDECAR_FIELD,
        "status",
        "official_event_date",
        "official_date_timezone",
        "month_key",
        "card_start_time_utc",
        "picks_lock_time_utc",
        "section_start_times_utc",
        "section_lock_times_utc",
        "first_final_result_at",
        "main_event_bout_id",
        "total_bouts",
        "listed_bout_count",
        "mission_eligible_bout_count",
        "lifecycle_revision",
        "structure_revision",
        "timing_revision",
        "current_eligibility",
        "timing_source",
    }
)

#: Canonical bout fields the boundary owns.
CANONICAL_BOUT_FIELDS = frozenset(
    {
        DOCUMENT_SIDECAR_FIELD,
        "status",
        "result",
        "is_title_fight",
        "is_bmf_title_fight",
        "title_type",
        "lineage_id",
        "matchup_revision",
        "replaces_bout_id",
        "replaced_by_bout_id",
        "result_revision",
        "card_section",
        "card_order",
        "order_overall",
        "order_section",
        "is_main_event",
        "is_co_main",
        "is_co_main_event",
        "scheduled_start_time_utc",
        "automatic_lock_time_utc",
    }
)

#: Canonical slot fields.  ``event_card_slots`` is reconciler-owned entirely.
CANONICAL_SLOT_FIELDS = frozenset(
    {
        "slot_id",
        "is_current",
        "card_section",
        "order_overall",
        "order_section",
        "role",
        "is_main_event",
        "is_co_main",
        "is_co_main_event",
        "scheduled_start_time_utc",
        "automatic_lock_time_utc",
        "structure_revision",
        "evidence",
        "card_snapshot_revision",
        "canonical_slot_version",
        "source",
    }
)

CANONICAL_FIELDS_BY_COLLECTION = {
    "events": CANONICAL_EVENT_FIELDS,
    "bouts": CANONICAL_BOUT_FIELDS,
    "event_card_slots": CANONICAL_SLOT_FIELDS,
}

_MUTATION_OPERATORS = (
    "$set",
    "$setOnInsert",
    "$unset",
    "$inc",
    "$push",
    "$addToSet",
    "$pull",
    "$rename",
    "$min",
    "$max",
    "$mul",
)

_LEGACY_METHOD_BY_FAMILY = {
    "ko_tko": "KO/TKO",
    "submission": "SUB",
    "decision": "DEC",
    "dq": "DQ",
    "other": "OTHER",
}
_LEGACY_OUTCOME = {
    "red_win": "red",
    "blue_win": "blue",
    "draw": "draw",
    "no_contest": "nc",
}


class CanonicalCardStateError(ValueError):
    """Raised when persisted documents cannot form a canonical card state."""


class CanonicalCardWriteError(RuntimeError):
    """Raised when a plan cannot be built or safely applied."""


class LegacyCardWriteViolation(RuntimeError):
    """Raised when a legacy writer tries to mutate a canonical field."""


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


def _document_id(document: Mapping[str, Any]) -> Optional[int]:
    value = document.get("id", document.get("_id"))
    return value if _positive_int(value) else None


def mongo_datetime(value: Any) -> Optional[datetime]:
    """Convert a canonical ISO-8601 UTC string to a naive UTC datetime.

    Legacy documents store naive UTC datetimes; the canonical snapshot stores
    ISO strings.  The conversion is total and lossless for the ``Z`` form the
    normalizer emits.
    """

    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if not _nonempty(value):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def assert_legacy_card_update_allowed(
    collection: str,
    update: Mapping[str, Any],
) -> None:
    """Reject a legacy update document that touches a canonical field.

    Legacy enrichment writers (fighter images, athlete profiles, event
    posters) remain free to update their own fields.  Anything the canonical
    boundary owns must arrive as an observation instead.
    """

    owned = CANONICAL_FIELDS_BY_COLLECTION.get(collection)
    if owned is None:
        return
    if not isinstance(update, Mapping):
        raise LegacyCardWriteViolation("A legacy update must be a mapping.")
    offending: set[str] = set()
    for operator, payload in update.items():
        if operator not in _MUTATION_OPERATORS:
            # A replacement document is always a full-authority write.
            if not str(operator).startswith("$"):
                offending.update(owned & {str(operator)})
                if str(operator) in owned:
                    offending.add(str(operator))
            continue
        for key in _mapping(payload):
            root = str(key).split(".", 1)[0]
            if root in owned:
                offending.add(root)
    if offending:
        raise LegacyCardWriteViolation(
            f"Legacy write to {collection} touches canonical fields: "
            + ", ".join(sorted(offending))
        )


#: Bout fields Admin owns outright (D-DATA-010). Both ``true`` and ``false`` are
#: durable decisions, so a lower-authority writer may not restate either one.
ADMIN_OWNED_BOUT_FIELDS = frozenset(
    {"is_title_fight", "is_bmf_title_fight", "title_type"}
)

ADMIN_SOURCE_KIND = "admin_override"


def admin_owned_fields(bout: Optional[Mapping[str, Any]]) -> set:
    """The canonical fields on this bout that Admin has explicitly decided.

    A bare ``is_title_fight: false`` is indistinguishable from a scraper
    default, so authority is read from the evidence entry Admin writes, not from
    the value itself. A bout that never went through Admin returns an empty set,
    which is why ingesting a brand new card is unaffected.
    """
    if not bout:
        return set()
    evidence = _mapping(_mapping(bout.get(DOCUMENT_SIDECAR_FIELD)).get("evidence"))
    return {
        str(field)
        for field, entry in evidence.items()
        if str(field) in ADMIN_OWNED_BOUT_FIELDS
        and _mapping(entry).get("source_kind") == ADMIN_SOURCE_KIND
    }


def strip_admin_owned(
    update: Mapping[str, Any],
    bout: Optional[Mapping[str, Any]],
) -> dict:
    """Remove the fields Admin decided from a lower-authority update.

    ESPN and Tapology title signals stay advisory (D-DATA-002): everything else
    in the update is written untouched, and only the specific fields Admin
    claimed are held back.
    """
    protected = admin_owned_fields(bout)
    if not protected:
        return dict(update)
    return {key: value for key, value in update.items() if key not in protected}


@dataclass(frozen=True)
class CanonicalCardState:
    """One event's persisted legacy documents plus canonical sidecars."""

    event_id: int
    event: Optional[Mapping[str, Any]]
    bouts: tuple[Mapping[str, Any], ...] = ()
    slots: tuple[Mapping[str, Any], ...] = ()

    @classmethod
    def build(
        cls,
        event_id: Any,
        event: Optional[Mapping[str, Any]],
        bouts: Sequence[Mapping[str, Any]] = (),
        slots: Sequence[Mapping[str, Any]] = (),
    ) -> "CanonicalCardState":
        if not _positive_int(event_id):
            raise CanonicalCardStateError("event_id must be a positive integer.")
        if event is not None and not isinstance(event, Mapping):
            raise CanonicalCardStateError("event must be a document or None.")
        ordered_bouts = []
        for document in _sequence(bouts):
            if not isinstance(document, Mapping):
                raise CanonicalCardStateError("Every bout must be a document.")
            if _document_id(document) is None:
                raise CanonicalCardStateError("Every bout needs a positive ID.")
            ordered_bouts.append(copy.deepcopy(dict(document)))
        ordered_slots = []
        for document in _sequence(slots):
            if not isinstance(document, Mapping):
                raise CanonicalCardStateError("Every slot must be a document.")
            ordered_slots.append(copy.deepcopy(dict(document)))
        ordered_bouts.sort(key=lambda item: _document_id(item) or 0)
        ordered_slots.sort(key=lambda item: (item.get("bout_id") or 0))
        return cls(
            event_id=event_id,
            event=copy.deepcopy(dict(event)) if event is not None else None,
            bouts=tuple(ordered_bouts),
            slots=tuple(ordered_slots),
        )

    @property
    def bout_by_id(self) -> dict[int, Mapping[str, Any]]:
        return {
            _document_id(item): item
            for item in self.bouts
            if _document_id(item) is not None
        }

    @property
    def slot_by_bout(self) -> dict[int, Mapping[str, Any]]:
        return {
            item["bout_id"]: item
            for item in self.slots
            if _positive_int(item.get("bout_id"))
        }

    @property
    def espn_event_alias(self) -> Optional[str]:
        event = _mapping(self.event)
        alias = event.get("espn_event_id") or _mapping(event.get("source_ids")).get(
            "espn_event_id"
        )
        return str(alias) if alias is not None else None


def rebuild_previous_snapshot(
    state: CanonicalCardState,
) -> Optional[Mapping[str, Any]]:
    """Return the persisted canonical snapshot, or bootstrap it from sidecars.

    The SCR-013 backfill persisted per-document ``card_data_v1`` sidecars plus
    canonical slots but no snapshot envelope.  The first continuous run
    therefore bootstraps from those sidecars; every later run reads the
    envelope this boundary persists.
    """

    event = _mapping(state.event)
    stored = event.get(SNAPSHOT_SIDECAR_FIELD)
    if isinstance(stored, Mapping) and _positive_int(
        _mapping(stored.get("event")).get("event_id")
    ):
        return copy.deepcopy(dict(stored))

    canonical_event = _mapping(event.get(DOCUMENT_SIDECAR_FIELD))
    if not _positive_int(canonical_event.get("event_id")):
        return None
    canonical_bouts = []
    for document in state.bouts:
        sidecar = _mapping(document.get(DOCUMENT_SIDECAR_FIELD))
        if _positive_int(sidecar.get("bout_id")):
            canonical_bouts.append(copy.deepcopy(dict(sidecar)))
    if not canonical_bouts:
        return None
    canonical_slots = []
    for document in state.slots:
        if not _positive_int(document.get("bout_id")):
            continue
        slot = {
            key: copy.deepcopy(value)
            for key, value in document.items()
            if key
            in {
                "slot_id",
                "event_id",
                "bout_id",
                "is_current",
                "card_section",
                "order_overall",
                "order_section",
                "role",
                "scheduled_start_time_utc",
                "automatic_lock_time_utc",
                "structure_revision",
                "evidence",
            }
        }
        slot.setdefault("slot_id", f"{state.event_id}:{document['bout_id']}")
        for timing_field in ("scheduled_start_time_utc", "automatic_lock_time_utc"):
            value = slot.get(timing_field)
            if isinstance(value, datetime):
                slot[timing_field] = (
                    value.replace(tzinfo=timezone.utc)
                    if value.tzinfo is None
                    else value.astimezone(timezone.utc)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
        canonical_slots.append(slot)
    canonical_bouts.sort(key=lambda item: item["bout_id"])
    canonical_slots.sort(key=lambda item: item["bout_id"])
    revision = canonical_event.get("structure_revision")
    snapshot_revision = revision if _positive_int(revision) else 1
    generated_at = (
        canonical_event.get("canonical_updated_at")
        or canonical_event.get("created_at")
        or "1970-01-01T00:00:00Z"
    )
    return {
        "contract_version": "card-data/v1",
        "snapshot_id": f"{state.event_id}:{snapshot_revision}",
        "snapshot_revision": snapshot_revision,
        "generated_at": generated_at,
        "event": copy.deepcopy(dict(canonical_event)),
        "bouts": canonical_bouts,
        "card_slots": canonical_slots,
    }


def persistable_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Strip run provenance so the persisted envelope is pure canonical state.

    ``source_run`` describes the run that produced a snapshot, not the card.
    Keeping it out of storage is what makes an immediate replay a true
    zero-diff: the same observations against the committed state resolve to a
    byte-identical envelope even though the second run has a different
    ``previous_snapshot_revision``.  Per-field provenance is unaffected — it
    lives in the persisted ``evidence`` maps.
    """

    return {
        key: copy.deepcopy(value)
        for key, value in snapshot.items()
        if key != "source_run"
    }


#: Per-entity keys that describe *when a fact was last re-observed* rather
#: than what the fact is.  They are stabilized by
#: :func:`stabilize_snapshot_provenance` so a repeated observation of an
#: unchanged card is a true zero-diff.
VOLATILE_PROVENANCE_KEYS = ("evidence", "canonical_updated_at", "source_updated_at")


def _evidence_rank(entry: Any) -> int:
    return SOURCE_RANKS.get(str(_mapping(entry).get("source_kind")), 0)


def _field_value(entity: Mapping[str, Any], key: str) -> Any:
    """Resolve a possibly dotted evidence key against its entity document."""

    value: Any = entity
    for part in str(key).split("."):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _semantic_entity(entity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in entity.items()
        if key not in VOLATILE_PROVENANCE_KEYS
    }


def _stabilize_title_suggestions(
    current: list[Any],
    previous: Sequence[Any],
) -> list[Any]:
    """Keep one entry per external title signal instead of one per run.

    ``_resolve_title`` appends every external suggestion it sees, and each run
    carries a fresh ``observed_at``.  Left alone the list grows without bound
    and mutates the bout document on every pass even though no title fact
    changed.  Re-observing the *same* suggestion from the same reference is
    not new information, so the earliest entry is retained.
    """

    def signature(item: Any) -> str:
        entry = _mapping(item)
        return _canonical(
            [
                entry.get("source_kind"),
                entry.get("source_ref"),
                entry.get("suggested"),
            ]
        )

    merged: dict[str, Any] = {}
    for item in [*previous, *current]:
        if not isinstance(item, Mapping):
            continue
        merged.setdefault(signature(item), copy.deepcopy(dict(item)))
    return [merged[key] for key in sorted(merged)]


def _stabilize_entity(current: dict[str, Any], previous: Mapping[str, Any]) -> None:
    """Restore provenance for every field that did not semantically change.

    Evidence records *what established* a resolved value, not the last run
    that happened to agree with it.  Refreshing it on agreement produced a
    content change with no revision bump, which the slot reconciler correctly
    refuses as ``REVISION_CONTENT_CONFLICT``.  A strictly higher-ranked source
    newly winning a field is a real authority upgrade and is always kept.
    """

    previous_evidence = _mapping(previous.get("evidence"))
    evidence = current.get("evidence")
    if isinstance(evidence, Mapping):
        stabilized = dict(evidence)
        for key, entry in evidence.items():
            prior = previous_evidence.get(key)
            if key == "title_suggestions":
                if isinstance(entry, Sequence) and not isinstance(entry, (str, bytes)):
                    stabilized[key] = _stabilize_title_suggestions(
                        list(entry), _sequence(prior)
                    )
                continue
            if not isinstance(prior, Mapping) or not isinstance(entry, Mapping):
                continue
            if _canonical(_field_value(current, key)) != _canonical(
                _field_value(previous, key)
            ):
                continue
            if _evidence_rank(prior) < _evidence_rank(entry):
                continue
            stabilized[key] = copy.deepcopy(dict(prior))
        current["evidence"] = stabilized

    if _canonical(_semantic_entity(current)) != _canonical(
        _semantic_entity(previous)
    ) or _canonical(current.get("evidence")) != _canonical(previous.get("evidence")):
        return
    for key in ("canonical_updated_at", "source_updated_at"):
        if key in current and key in previous:
            current[key] = copy.deepcopy(previous[key])


def stabilize_snapshot_provenance(
    snapshot: Mapping[str, Any],
    previous: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Strip non-semantic churn from a freshly normalized snapshot.

    The normalizer resolves values and allocates revisions correctly, but it
    also re-stamps evidence and ``canonical_updated_at`` for facts a later
    observation merely re-confirmed.  Because no revision advances for a
    re-confirmation, that churn makes every subsequent continuous pass look
    like an unversioned content change.  This is the boundary's storage
    concern — the same reason :func:`persistable_snapshot` drops run
    provenance — so it is resolved here, before planning any write.
    """

    stabilized = copy.deepcopy(dict(snapshot))
    if not isinstance(previous, Mapping) or not previous:
        return stabilized

    previous_event = _mapping(previous.get("event"))
    event = stabilized.get("event")
    if isinstance(event, dict) and previous_event:
        _stabilize_entity(event, previous_event)

    for collection, key_name in (("bouts", "bout_id"), ("card_slots", "bout_id")):
        previous_by_key = {
            item[key_name]: item
            for item in _sequence(previous.get(collection))
            if isinstance(item, Mapping) and _positive_int(item.get(key_name))
        }
        for item in _sequence(stabilized.get(collection)):
            if not isinstance(item, dict):
                continue
            prior = previous_by_key.get(item.get(key_name))
            if prior is not None:
                _stabilize_entity(item, prior)

    # ``generated_at`` describes the run, not the card, and ``source_run`` is
    # never persisted.  When everything else resolved identically the card did
    # not change, so the stored instant is kept and the replay is a zero-diff.
    ignored = {"generated_at", "source_run"}
    if _canonical(
        {key: value for key, value in stabilized.items() if key not in ignored}
    ) == _canonical(
        {key: value for key, value in previous.items() if key not in ignored}
    ):
        stabilized["generated_at"] = copy.deepcopy(previous.get("generated_at"))
    return stabilized


def _timing_authority(snapshot_event: Mapping[str, Any]) -> str:
    evidence = _mapping(snapshot_event.get("evidence"))
    sources = set()
    for key, value in evidence.items():
        if not str(key).startswith(
            ("section_lock_times_utc", "section_start_times_utc", "official_event_date")
        ):
            continue
        source = _mapping(value).get("source_kind")
        if _nonempty(source):
            sources.add(source)
    if "admin_override" in sources:
        return "admin"
    if {"espn_detail", "espn_summary"} & sources:
        return "espn"
    return "canonical"


def legacy_event_projection(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Project the canonical snapshot into legacy ``events`` fields."""

    event = _mapping(snapshot.get("event"))
    slots = [
        item
        for item in _sequence(snapshot.get("card_slots"))
        if isinstance(item, Mapping) and item.get("is_current") is True
    ]
    projection: dict[str, Any] = {
        "status": event.get("status"),
        "official_event_date": event.get("official_event_date"),
        "official_date_timezone": event.get("official_date_timezone"),
        "month_key": event.get("month_key"),
        "main_event_bout_id": event.get("main_event_bout_id"),
        "listed_bout_count": event.get("listed_bout_count"),
        "mission_eligible_bout_count": event.get("mission_eligible_bout_count"),
        "total_bouts": len(slots),
        "lifecycle_revision": event.get("lifecycle_revision"),
        "structure_revision": event.get("structure_revision"),
        "timing_revision": event.get("timing_revision"),
        "card_start_time_utc": mongo_datetime(event.get("card_start_time_utc")),
        "picks_lock_time_utc": mongo_datetime(event.get("picks_lock_time_utc")),
        "first_final_result_at": mongo_datetime(event.get("first_final_result_at")),
        "section_start_times_utc": {
            section: mongo_datetime(value)
            for section, value in sorted(
                _mapping(event.get("section_start_times_utc")).items()
            )
        },
        "section_lock_times_utc": {
            section: mongo_datetime(value)
            for section, value in sorted(
                _mapping(event.get("section_lock_times_utc")).items()
            )
        },
        "timing_source": _timing_authority(event),
        "current_eligibility": copy.deepcopy(
            dict(_mapping(snapshot.get("current_eligibility")))
        ),
        DOCUMENT_SIDECAR_FIELD: copy.deepcopy(dict(event)),
        SNAPSHOT_SIDECAR_FIELD: persistable_snapshot(snapshot),
    }
    return projection


def legacy_result_projection(
    canonical_result: Optional[Mapping[str, Any]],
    fighters: Sequence[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    """Project a canonical result into the legacy ``bouts.result`` shape."""

    if not isinstance(canonical_result, Mapping) or not canonical_result:
        return None
    outcome = canonical_result.get("outcome")
    legacy_outcome = _LEGACY_OUTCOME.get(str(outcome))
    if legacy_outcome is None:
        return None
    winner_id = canonical_result.get("winner_fighter_id")
    winner_corner = None
    winner_name = None
    for fighter in fighters:
        if not isinstance(fighter, Mapping):
            continue
        if _nonempty(winner_id) and fighter.get("fighter_id") == winner_id:
            winner_corner = fighter.get("corner")
            winner_name = fighter.get("display_name")
    family = str(canonical_result.get("method_family") or "other")
    method = _LEGACY_METHOD_BY_FAMILY.get(family, "OTHER")
    if legacy_outcome == "nc" and family == "other":
        method = "NC"
    seconds = canonical_result.get("ending_time_seconds")
    ending_time = None
    if isinstance(seconds, int) and not isinstance(seconds, bool) and seconds >= 0:
        ending_time = f"{seconds // 60}:{seconds % 60:02d}"
    return {
        "winner": winner_corner,
        "winner_name": winner_name,
        "outcome": legacy_outcome,
        "method": method,
        "method_detail": canonical_result.get("method_detail"),
        "round": canonical_result.get("ending_round"),
        "time": ending_time,
        "revision": canonical_result.get("revision"),
        "status": canonical_result.get("status"),
        "winner_fighter_id": winner_id if _nonempty(winner_id) else None,
        "method_family": family,
        "source": "card_data_v1",
    }


def legacy_bout_projection(
    canonical_bout: Mapping[str, Any],
    canonical_slot: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Project one canonical bout plus its slot into legacy ``bouts`` fields."""

    fighters = [
        item for item in _sequence(canonical_bout.get("fighters")) if isinstance(item, Mapping)
    ]
    projection: dict[str, Any] = {
        "status": canonical_bout.get("status"),
        "weight_class": canonical_bout.get("weight_class"),
        "gender": canonical_bout.get("gender"),
        "scheduled_rounds": canonical_bout.get("scheduled_rounds"),
        "rounds_scheduled": canonical_bout.get("scheduled_rounds"),
        "is_title_fight": canonical_bout.get("is_title_fight"),
        "is_bmf_title_fight": canonical_bout.get("is_bmf_title_fight"),
        "title_type": canonical_bout.get("title_type"),
        "lineage_id": canonical_bout.get("lineage_id"),
        "matchup_revision": canonical_bout.get("matchup_revision"),
        "replaces_bout_id": canonical_bout.get("replaces_bout_id"),
        "replaced_by_bout_id": canonical_bout.get("replaced_by_bout_id"),
        "result_revision": canonical_bout.get("result_revision"),
        "result": legacy_result_projection(canonical_bout.get("result"), fighters),
        DOCUMENT_SIDECAR_FIELD: copy.deepcopy(dict(canonical_bout)),
    }
    for fighter in fighters:
        corner = fighter.get("corner")
        if corner not in {"red", "blue"}:
            continue
        projection[f"fighters.{corner}.fighter_id"] = fighter.get("fighter_id")
        projection[f"fighters.{corner}.fighter_name"] = fighter.get("display_name")
        projection[f"fighters.{corner}.corner"] = corner
    if isinstance(canonical_slot, Mapping) and canonical_slot:
        projection.update(
            {
                "card_section": canonical_slot.get("card_section"),
                "card_order": canonical_slot.get("order_section"),
                "order_overall": canonical_slot.get("order_overall"),
                "order_section": canonical_slot.get("order_section"),
                "is_main_event": canonical_slot.get("role") == "main_event",
                "is_co_main": canonical_slot.get("role") == "co_main",
                "is_co_main_event": canonical_slot.get("role") == "co_main",
                "scheduled_start_time_utc": mongo_datetime(
                    canonical_slot.get("scheduled_start_time_utc")
                ),
                "automatic_lock_time_utc": mongo_datetime(
                    canonical_slot.get("automatic_lock_time_utc")
                ),
            }
        )
    return projection


def legacy_bout_seed(
    event_id: int,
    canonical_bout: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the immutable legacy identity for a newly canonical bout."""

    bout_id = canonical_bout["bout_id"]
    source_ids = _mapping(canonical_bout.get("source_ids"))
    competition_id = source_ids.get("espn_competition_id")
    seed: dict[str, Any] = {
        "_id": bout_id,
        "id": bout_id,
        "event_id": event_id,
        "source": "card_data_v1",
        "slug": f"bout-{bout_id}",
        "fighters": {},
    }
    if competition_id is not None:
        seed["espn_competition_id"] = str(competition_id)
        seed["slug"] = f"espn-{competition_id}"
    for fighter in _sequence(canonical_bout.get("fighters")):
        if not isinstance(fighter, Mapping):
            continue
        corner = fighter.get("corner")
        if corner not in {"red", "blue"}:
            continue
        fighter_source_ids = _mapping(fighter.get("source_ids"))
        entry = {
            "fighter_id": fighter.get("fighter_id"),
            "fighter_name": fighter.get("display_name"),
            "corner": corner,
        }
        if fighter_source_ids.get("espn_athlete_id") is not None:
            entry["espn_id"] = str(fighter_source_ids["espn_athlete_id"])
        seed["fighters"][corner] = entry
    return seed


def _changed_fields(
    current: Mapping[str, Any],
    desired: Mapping[str, Any],
) -> dict[str, Any]:
    changed: dict[str, Any] = {}
    for key, value in sorted(desired.items()):
        if "." in key:
            root, rest = key.split(".", 1)
            observed: Any = current.get(root)
            for part in rest.split("."):
                observed = _mapping(observed).get(part)
        else:
            observed = current.get(key)
        if _canonical(observed) != _canonical(value):
            changed[key] = value
    return changed


@dataclass(frozen=True)
class BoutDocumentWrite:
    bout_id: int
    action: str
    changed_fields: tuple[str, ...]
    values: Mapping[str, Any]
    seed: Optional[Mapping[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "bout_id": self.bout_id,
            "action": self.action,
            "changed_fields": list(self.changed_fields),
            "values": copy.deepcopy(dict(self.values)),
            "seed": copy.deepcopy(dict(self.seed)) if self.seed else None,
        }


@dataclass(frozen=True)
class CardWriteFinding:
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
class CardWritePlan:
    """A fully reviewed, replay-safe canonical write for exactly one event."""

    boundary_version: str
    event_id: int
    snapshot: Optional[Mapping[str, Any]]
    change_set: Optional[Mapping[str, Any]]
    slot_plan: Optional[SlotReconciliationPlan]
    event_update: Mapping[str, Any] = field(default_factory=dict)
    bout_writes: tuple[BoutDocumentWrite, ...] = ()
    quarantines: tuple[Quarantine, ...] = ()
    findings: tuple[CardWriteFinding, ...] = ()

    @property
    def blocked(self) -> bool:
        if self.snapshot is None or self.slot_plan is None:
            return True
        if not self.slot_plan.safe_to_apply:
            return True
        return any(item.severity in {"blocking", "error"} for item in self.findings)

    @property
    def safe_to_apply(self) -> bool:
        return not self.blocked

    @property
    def converged(self) -> bool:
        return (
            self.safe_to_apply
            and not self.event_update
            and not self.bout_writes
            and self.slot_plan is not None
            and self.slot_plan.converged
        )

    @property
    def operation_count(self) -> int:
        slot_operations = len(self.slot_plan.operations) if self.slot_plan else 0
        return (
            (1 if self.event_update else 0) + len(self.bout_writes) + slot_operations
        )

    def summary(self) -> dict[str, Any]:
        return {
            "event_update_field_count": len(self.event_update),
            "bout_insert_count": sum(
                item.action == "insert" for item in self.bout_writes
            ),
            "bout_update_count": sum(
                item.action == "update" for item in self.bout_writes
            ),
            "slot_insert_count": sum(
                operation.action == "insert"
                for operation in (self.slot_plan.operations if self.slot_plan else ())
            ),
            "slot_update_count": sum(
                operation.action == "update"
                for operation in (self.slot_plan.operations if self.slot_plan else ())
            ),
            "slot_conflict_count": len(self.slot_plan.conflicts) if self.slot_plan else 0,
            "quarantine_count": len(self.quarantines),
            "operation_count": self.operation_count,
            "converged": self.converged,
            "safe_to_apply": self.safe_to_apply,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "boundary_version": self.boundary_version,
            "event_id": self.event_id,
            "snapshot_id": (
                self.snapshot.get("snapshot_id") if self.snapshot else None
            ),
            "snapshot_revision": (
                self.snapshot.get("snapshot_revision") if self.snapshot else None
            ),
            "slot_plan_id": self.slot_plan.plan_id if self.slot_plan else None,
            "summary": self.summary(),
            "event_update": copy.deepcopy(dict(self.event_update)),
            "bout_writes": [item.as_dict() for item in self.bout_writes],
            "quarantines": [item.as_dict() for item in self.quarantines],
            "findings": [item.as_dict() for item in self.findings],
        }


def _finding(
    code: str,
    severity: str,
    scope: str,
    ids: Sequence[Any],
    message: str,
) -> CardWriteFinding:
    return CardWriteFinding(
        code=code,
        severity=severity,
        scope=scope,
        example_ids=tuple(sorted({str(item) for item in ids if item is not None})[:5]),
        message=message,
    )


def plan_canonical_card_write(
    state: CanonicalCardState,
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
) -> CardWritePlan:
    """Resolve observations into one replay-safe canonical write plan."""

    if not isinstance(state, CanonicalCardState):
        raise CanonicalCardWriteError("state must be a CanonicalCardState.")
    findings: list[CardWriteFinding] = []
    if not observations:
        return CardWritePlan(
            boundary_version=BOUNDARY_VERSION,
            event_id=state.event_id,
            snapshot=None,
            change_set=None,
            slot_plan=None,
            findings=(
                _finding(
                    "NO_OBSERVATIONS",
                    "error",
                    "card_data",
                    (state.event_id,),
                    "A canonical write requires at least one observation.",
                ),
            ),
        )
    previous = rebuild_previous_snapshot(state)
    try:
        normalization = normalize_card_data_v1(observations, previous)
    except NormalizationInputError as exc:
        return CardWritePlan(
            boundary_version=BOUNDARY_VERSION,
            event_id=state.event_id,
            snapshot=None,
            change_set=None,
            slot_plan=None,
            findings=(
                _finding(
                    "NORMALIZATION_REJECTED",
                    "error",
                    "card_data",
                    (state.event_id,),
                    f"Observation boundary violated ({type(exc).__name__}).",
                ),
            ),
        )
    snapshot = stabilize_snapshot_provenance(normalization.snapshot, previous)
    validation = validate_card_data_v1(snapshot)
    if not validation.is_valid:
        findings.append(
            _finding(
                "SNAPSHOT_CONTRACT_INVALID",
                "error",
                "card_data",
                (state.event_id,),
                "Resolved snapshot violates CardDataContractV1: "
                + ", ".join(sorted(set(validation.codes))[:5]),
            )
        )
    try:
        slot_plan = plan_slot_reconciliation(snapshot, state.slots)
    except SlotReconciliationInputError as exc:
        findings.append(
            _finding(
                "SLOT_PLAN_REJECTED",
                "error",
                "event_card_slots",
                (state.event_id,),
                f"Slot reconciliation could not be planned ({type(exc).__name__}).",
            )
        )
        return CardWritePlan(
            boundary_version=BOUNDARY_VERSION,
            event_id=state.event_id,
            snapshot=snapshot,
            change_set=normalization.change_set,
            slot_plan=None,
            quarantines=normalization.quarantines,
            findings=tuple(findings),
        )
    for conflict in slot_plan.conflicts:
        findings.append(
            _finding(
                f"SLOT_{conflict.code}",
                "error" if conflict.blocking else "warning",
                "event_card_slots",
                (conflict.bout_id or conflict.slot_id or state.event_id,),
                conflict.message,
            )
        )
    for quarantine in normalization.quarantines:
        findings.append(
            _finding(
                f"QUARANTINE_{quarantine.code}",
                "error",
                quarantine.entity_type,
                (quarantine.entity_id or state.event_id,),
                quarantine.message,
            )
        )

    event_desired = legacy_event_projection(snapshot)
    event_update = _changed_fields(_mapping(state.event), event_desired)

    slot_by_bout = {
        item["bout_id"]: item
        for item in _sequence(snapshot.get("card_slots"))
        if isinstance(item, Mapping) and _positive_int(item.get("bout_id"))
    }
    current_bouts = state.bout_by_id
    bout_writes: list[BoutDocumentWrite] = []
    for canonical_bout in _sequence(snapshot.get("bouts")):
        if not isinstance(canonical_bout, Mapping):
            continue
        bout_id = canonical_bout.get("bout_id")
        if not _positive_int(bout_id):
            continue
        desired = legacy_bout_projection(
            canonical_bout, slot_by_bout.get(bout_id)
        )
        existing = current_bouts.get(bout_id)
        if existing is None:
            seed = legacy_bout_seed(state.event_id, canonical_bout)
            bout_writes.append(
                BoutDocumentWrite(
                    bout_id=bout_id,
                    action="insert",
                    changed_fields=tuple(sorted(desired)),
                    values=desired,
                    seed=seed,
                )
            )
            continue
        changed = _changed_fields(existing, desired)
        if changed:
            bout_writes.append(
                BoutDocumentWrite(
                    bout_id=bout_id,
                    action="update",
                    changed_fields=tuple(sorted(changed)),
                    values=changed,
                )
            )
    orphan_bouts = sorted(set(current_bouts) - set(slot_by_bout) - {
        item["bout_id"]
        for item in _sequence(snapshot.get("bouts"))
        if isinstance(item, Mapping) and _positive_int(item.get("bout_id"))
    })
    if orphan_bouts:
        findings.append(
            _finding(
                "PERSISTED_BOUT_OUTSIDE_SNAPSHOT",
                "warning",
                "bouts",
                orphan_bouts,
                "A persisted bout has no canonical projection and is retained untouched.",
            )
        )
    bout_writes.sort(key=lambda item: item.bout_id)
    return CardWritePlan(
        boundary_version=BOUNDARY_VERSION,
        event_id=state.event_id,
        snapshot=snapshot,
        change_set=normalization.change_set,
        slot_plan=slot_plan,
        event_update=event_update,
        bout_writes=tuple(bout_writes),
        quarantines=normalization.quarantines,
        findings=tuple(findings),
    )


@dataclass(frozen=True)
class CardWriteReceipt:
    boundary_version: str
    event_id: int
    dry_run: bool
    applied: bool
    snapshot_id: Optional[str]
    slot_plan_id: Optional[str]
    operation_count: int
    verified_converged: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "boundary_version": self.boundary_version,
            "event_id": self.event_id,
            "dry_run": self.dry_run,
            "applied": self.applied,
            "snapshot_id": self.snapshot_id,
            "slot_plan_id": self.slot_plan_id,
            "operation_count": self.operation_count,
            "verified_converged": self.verified_converged,
        }


class CanonicalCardStore(Protocol):
    """Storage port for one event-scoped canonical card write."""

    def load_card(self, event_id: int) -> CanonicalCardState: ...

    def commit_card_write(self, plan: CardWritePlan) -> None: ...


class InMemoryCanonicalCardStore:
    """Deterministic local double used by tests and dry-run tooling."""

    def __init__(
        self,
        events: Sequence[Mapping[str, Any]] = (),
        bouts: Sequence[Mapping[str, Any]] = (),
        slots: Sequence[Mapping[str, Any]] = (),
    ) -> None:
        self.events = {
            item["id"]: copy.deepcopy(dict(item))
            for item in events
            if _positive_int(item.get("id"))
        }
        self.bouts = {
            _document_id(item): copy.deepcopy(dict(item))
            for item in bouts
            if _document_id(item) is not None
        }
        self.slots = {
            str(item.get("_id") or item.get("id") or item.get("slot_id")): copy.deepcopy(
                dict(item)
            )
            for item in slots
        }
        self.deleted_documents: list[tuple[str, Any]] = []

    def load_card(self, event_id: int) -> CanonicalCardState:
        return CanonicalCardState.build(
            event_id,
            self.events.get(event_id),
            [item for item in self.bouts.values() if item.get("event_id") == event_id],
            [item for item in self.slots.values() if item.get("event_id") == event_id],
        )

    def commit_card_write(self, plan: CardWritePlan) -> None:
        if plan.slot_plan is None:
            raise CanonicalCardWriteError("A committed plan requires a slot plan.")
        observed_digest = slot_collection_digest(
            [
                item
                for item in self.slots.values()
                if item.get("event_id") == plan.event_id
            ]
        )
        if observed_digest != plan.slot_plan.current_digest:
            raise CanonicalCardWriteError(
                "Persisted slots drifted after the plan was reviewed."
            )
        event = self.events.setdefault(plan.event_id, {"id": plan.event_id})
        _apply_paths(event, plan.event_update)
        for write in plan.bout_writes:
            if write.action == "insert":
                document = copy.deepcopy(dict(write.seed or {}))
                document.setdefault("_id", write.bout_id)
                document.setdefault("id", write.bout_id)
                document.setdefault("event_id", plan.event_id)
                self.bouts[write.bout_id] = document
            document = self.bouts.get(write.bout_id)
            if document is None:
                raise CanonicalCardWriteError(
                    f"Bout {write.bout_id} disappeared before the canonical write."
                )
            _apply_paths(document, write.values)
        for operation in plan.slot_plan.operations:
            document = copy.deepcopy(dict(operation.after))
            self.slots[str(document["_id"])] = document


def _apply_paths(document: dict[str, Any], values: Mapping[str, Any]) -> None:
    for key, value in values.items():
        parts = str(key).split(".")
        target = document
        for part in parts[:-1]:
            nested = target.get(part)
            if not isinstance(nested, dict):
                nested = {}
                target[part] = nested
            target = nested
        target[parts[-1]] = copy.deepcopy(value)


class MongoCanonicalCardStore:
    """Adapter over any Mongo-shaped database handle.

    The handle is duck-typed on purpose so this module never imports a driver
    and so tests can inject a local double.  Every write is an update with an
    explicit filter; the adapter never issues a delete.
    """

    def __init__(self, db: Any) -> None:
        self.db = db

    def load_card(self, event_id: int) -> CanonicalCardState:
        event = self.db.events.find_one({"id": event_id})
        bouts = list(self.db.bouts.find({"event_id": event_id}))
        slots = list(self.db.event_card_slots.find({"event_id": event_id}))
        return CanonicalCardState.build(event_id, event, bouts, slots)

    def commit_card_write(self, plan: CardWritePlan) -> None:
        if plan.slot_plan is None:
            raise CanonicalCardWriteError("A committed plan requires a slot plan.")
        if plan.event_update:
            self.db.events.update_one(
                {"id": plan.event_id},
                {"$set": dict(plan.event_update)},
            )
        for write in plan.bout_writes:
            if write.action == "insert":
                self.db.bouts.update_one(
                    {"id": write.bout_id},
                    {
                        "$setOnInsert": dict(write.seed or {}),
                    },
                    upsert=True,
                )
            self.db.bouts.update_one(
                {"id": write.bout_id},
                {"$set": dict(write.values)},
            )
        for operation in plan.slot_plan.operations:
            document = dict(operation.after)
            self.db.event_card_slots.update_one(
                {"_id": document["_id"]},
                {"$set": document},
                upsert=True,
            )


def apply_canonical_card_write(
    plan: CardWritePlan,
    store: CanonicalCardStore,
    *,
    dry_run: bool = True,
) -> CardWriteReceipt:
    """Commit a safe plan. Convergence is NOT verified here.

    The replay check lives in `submit_card_observations`, which still holds the
    observations needed to replan. Callers that reach for this function directly
    get a receipt with `verified_converged=False`, which is the honest answer.
    """

    snapshot_id = plan.snapshot.get("snapshot_id") if plan.snapshot else None
    slot_plan_id = plan.slot_plan.plan_id if plan.slot_plan else None
    if plan.blocked:
        raise CanonicalCardWriteError(
            "A blocked canonical card plan must never be applied."
        )
    if dry_run:
        return CardWriteReceipt(
            boundary_version=BOUNDARY_VERSION,
            event_id=plan.event_id,
            dry_run=True,
            applied=False,
            snapshot_id=snapshot_id,
            slot_plan_id=slot_plan_id,
            operation_count=plan.operation_count,
            verified_converged=plan.converged,
        )
    operation_count = plan.operation_count
    store.commit_card_write(plan)
    return CardWriteReceipt(
        boundary_version=BOUNDARY_VERSION,
        event_id=plan.event_id,
        dry_run=False,
        applied=True,
        snapshot_id=snapshot_id,
        slot_plan_id=slot_plan_id,
        operation_count=operation_count,
        verified_converged=False,
    )


def submit_card_observations(
    store: CanonicalCardStore,
    event_id: int,
    observations: Sequence[CardDataObservation | Mapping[str, Any]],
    *,
    dry_run: bool = True,
    verify_replay: bool = True,
) -> tuple[CardWritePlan, CardWriteReceipt]:
    """Load, plan, optionally commit and prove convergence for one card.

    This is the only entry point ongoing writers should call.  With
    ``verify_replay`` the same observations are replanned against the
    committed state; a non-converged replay raises instead of silently
    leaving dual authority behind.
    """

    state = store.load_card(event_id)
    plan = plan_canonical_card_write(state, observations)
    if plan.blocked:
        return plan, CardWriteReceipt(
            boundary_version=BOUNDARY_VERSION,
            event_id=event_id,
            dry_run=dry_run,
            applied=False,
            snapshot_id=plan.snapshot.get("snapshot_id") if plan.snapshot else None,
            slot_plan_id=plan.slot_plan.plan_id if plan.slot_plan else None,
            operation_count=plan.operation_count,
            verified_converged=False,
        )
    receipt = apply_canonical_card_write(plan, store, dry_run=dry_run)
    if dry_run or not verify_replay:
        return plan, receipt
    replay = plan_canonical_card_write(store.load_card(event_id), observations)
    if not replay.converged:
        raise CanonicalCardWriteError(
            "Canonical card write did not converge on immediate replay: "
            + _canonical(replay.summary())
        )
    return plan, CardWriteReceipt(
        boundary_version=receipt.boundary_version,
        event_id=receipt.event_id,
        dry_run=receipt.dry_run,
        applied=receipt.applied,
        snapshot_id=receipt.snapshot_id,
        slot_plan_id=receipt.slot_plan_id,
        operation_count=receipt.operation_count,
        verified_converged=True,
    )


__all__ = [
    "BOUNDARY_VERSION",
    "ADMIN_OWNED_BOUT_FIELDS",
    "ADMIN_SOURCE_KIND",
    "admin_owned_fields",
    "strip_admin_owned",
    "CANONICAL_BOUT_FIELDS",
    "CANONICAL_EVENT_FIELDS",
    "CANONICAL_FIELDS_BY_COLLECTION",
    "CANONICAL_SLOT_FIELDS",
    "DOCUMENT_SIDECAR_FIELD",
    "SNAPSHOT_SIDECAR_FIELD",
    "VOLATILE_PROVENANCE_KEYS",
    "BoutDocumentWrite",
    "CanonicalCardStateError",
    "CanonicalCardState",
    "CanonicalCardStore",
    "CanonicalCardWriteError",
    "CardWriteFinding",
    "CardWritePlan",
    "CardWriteReceipt",
    "InMemoryCanonicalCardStore",
    "LegacyCardWriteViolation",
    "MongoCanonicalCardStore",
    "apply_canonical_card_write",
    "assert_legacy_card_update_allowed",
    "legacy_bout_projection",
    "legacy_bout_seed",
    "legacy_event_projection",
    "legacy_result_projection",
    "mongo_datetime",
    "persistable_snapshot",
    "plan_canonical_card_write",
    "rebuild_previous_snapshot",
    "stabilize_snapshot_provenance",
    "submit_card_observations",
]
