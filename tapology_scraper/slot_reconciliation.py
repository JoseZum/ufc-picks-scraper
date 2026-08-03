"""Dry-run-first, idempotent persistence planning for CardData V1 slots.

SCR-009 deliberately separates three concerns:

* :func:`plan_slot_reconciliation` is a pure diff from persisted slot
  documents to the desired slots produced by ``CardDataNormalizerV1``;
* an injected storage port owns the event-scoped atomic compare-and-set;
* :func:`apply_slot_reconciliation` verifies the exact reviewed plan ID and
  the final converged documents.

The module never opens a database connection and is not wired into the current
ESPN/Admin writers.  It never produces delete operations.  A persisted slot
that is absent from the desired canonical snapshot blocks the plan so SCR-010,
not a storage adapter, decides cancellation/removal semantics.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Optional, Protocol

from tapology_scraper.card_data_contract import validate_card_data_v1


PLAN_VERSION = "slot-reconciliation-plan/v1"
SLOT_STORAGE_VERSION = "card-slot/v1"
MISSING_MARKER = {"$card_data_missing": True}

OWNED_SLOT_FIELDS = (
    "_id",
    "id",
    "slot_id",
    "event_id",
    "bout_id",
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
)
FINGERPRINT_FIELDS = tuple(
    field for field in OWNED_SLOT_FIELDS if field != "_id"
)


class SlotReconciliationInputError(ValueError):
    """Raised when a desired snapshot/current document set is malformed."""


class UnsafeSlotReconciliationPlan(RuntimeError):
    """Raised when a caller attempts to apply a plan with blocking conflicts."""


class SlotReconciliationApplyError(RuntimeError):
    """Raised when optimistic/atomic storage application cannot be verified."""


@dataclass(frozen=True)
class SlotConflict:
    code: str
    slot_id: Optional[str]
    bout_id: Optional[int]
    message: str
    blocking: bool = True
    current_structure_revision: Optional[int] = None
    desired_structure_revision: Optional[int] = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SlotOperation:
    operation_id: str
    action: str
    slot_id: str
    event_id: int
    bout_id: int
    expected_fingerprint: Optional[str]
    desired_fingerprint: str
    expected_structure_revision: Optional[int]
    desired_structure_revision: int
    changed_fields: tuple[str, ...]
    before: Optional[Mapping[str, Any]]
    after: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "action": self.action,
            "slot_id": self.slot_id,
            "event_id": self.event_id,
            "bout_id": self.bout_id,
            "expected_fingerprint": self.expected_fingerprint,
            "desired_fingerprint": self.desired_fingerprint,
            "expected_structure_revision": self.expected_structure_revision,
            "desired_structure_revision": self.desired_structure_revision,
            "changed_fields": list(self.changed_fields),
            "before": copy.deepcopy(dict(self.before))
            if isinstance(self.before, Mapping)
            else None,
            "after": copy.deepcopy(dict(self.after)),
        }


@dataclass(frozen=True)
class SlotReconciliationPlan:
    plan_version: str
    plan_id: str
    event_id: int
    snapshot_id: str
    previous_snapshot_revision: Optional[int]
    new_snapshot_revision: int
    desired_structure_revision: int
    current_digest: str
    desired_digest: str
    desired_documents: tuple[Mapping[str, Any], ...]
    operations: tuple[SlotOperation, ...]
    unchanged_slot_ids: tuple[str, ...]
    conflicts: tuple[SlotConflict, ...]

    @property
    def safe_to_apply(self) -> bool:
        return not any(item.blocking for item in self.conflicts)

    @property
    def converged(self) -> bool:
        return self.safe_to_apply and not self.operations

    def summary(self) -> dict[str, Any]:
        return {
            "desired_count": len(self.desired_documents),
            "insert_count": sum(
                operation.action == "insert" for operation in self.operations
            ),
            "update_count": sum(
                operation.action == "update" for operation in self.operations
            ),
            "unchanged_count": len(self.unchanged_slot_ids),
            "conflict_count": len(self.conflicts),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "plan_version": self.plan_version,
            "plan_id": self.plan_id,
            "event_id": self.event_id,
            "snapshot_id": self.snapshot_id,
            "previous_snapshot_revision": self.previous_snapshot_revision,
            "new_snapshot_revision": self.new_snapshot_revision,
            "desired_structure_revision": self.desired_structure_revision,
            "safe_to_apply": self.safe_to_apply,
            "converged": self.converged,
            "current_digest": self.current_digest,
            "desired_digest": self.desired_digest,
            "summary": self.summary(),
            "desired_documents": [
                copy.deepcopy(dict(document)) for document in self.desired_documents
            ],
            "operations": [operation.as_dict() for operation in self.operations],
            "unchanged_slot_ids": list(self.unchanged_slot_ids),
            "conflicts": [conflict.as_dict() for conflict in self.conflicts],
        }


@dataclass(frozen=True)
class SlotCommitReceipt:
    """Storage-port receipt for one event-scoped atomic commit."""

    plan_id: str
    previous_digest: str
    applied_operation_ids: tuple[str, ...]
    final_documents: tuple[Mapping[str, Any], ...]


class SlotReconciliationStore(Protocol):
    """Port a Mongo/local adapter must implement atomically for one event.

    ``commit_slot_plan`` must compare the persisted event-slot digest to
    ``plan.current_digest`` before performing any operation, apply every
    operation or none, compare each update's expected fingerprint, and update
    only the declared canonical fields so unrelated legacy fields survive.
    """

    def commit_slot_plan(
        self, plan: SlotReconciliationPlan
    ) -> SlotCommitReceipt: ...


@dataclass(frozen=True)
class SlotApplyResult:
    plan_id: str
    dry_run: bool
    safe_to_apply: bool
    attempted_operation_count: int
    applied_operation_ids: tuple[str, ...]
    verified_converged: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    digest = hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _owned_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: copy.deepcopy(document[field])
        if field in document
        else copy.deepcopy(MISSING_MARKER)
        for field in OWNED_SLOT_FIELDS
    }


def slot_storage_fingerprint(document: Mapping[str, Any]) -> str:
    """Hash every canonical storage field except the stored fingerprint itself."""

    projection = {
        field: copy.deepcopy(document[field])
        if field in document
        else copy.deepcopy(MISSING_MARKER)
        for field in FINGERPRINT_FIELDS
    }
    return _hash(projection)


def slot_collection_digest(documents: Sequence[Mapping[str, Any]]) -> str:
    """Deterministic digest used as the event-scoped optimistic CAS token."""

    values = [
        {
            "storage_id": str(
                document.get("_id")
                or document.get("slot_id")
                or document.get("id")
                or ""
            ),
            "projection": _owned_projection(document),
            "stored_fingerprint": document.get("reconciliation_fingerprint"),
        }
        for document in documents
    ]
    values.sort(key=_canonical)
    return _hash(values)


def legacy_bout_slot_projection(slot: Mapping[str, Any]) -> dict[str, Any]:
    """Read-model projection for legacy BoutResponse fields; never persisted here."""

    role = slot.get("role")
    return {
        "card_section": slot.get("card_section"),
        "order_overall": slot.get("order_overall"),
        "order_section": slot.get("order_section"),
        "card_order": slot.get("order_overall"),
        "is_main_event": role == "main_event",
        "is_co_main": role == "co_main",
        "is_co_main_event": role == "co_main",
        "scheduled_start_time_utc": slot.get("scheduled_start_time_utc"),
        "automatic_lock_time_utc": slot.get("automatic_lock_time_utc"),
    }


def _desired_document(
    slot: Mapping[str, Any], snapshot_revision: int
) -> dict[str, Any]:
    event_id = slot["event_id"]
    bout_id = slot["bout_id"]
    slot_id = f"{event_id}:{bout_id}"
    role = slot.get("role")
    document = {
        "_id": slot_id,
        "id": slot_id,
        "slot_id": slot_id,
        "event_id": event_id,
        "bout_id": bout_id,
        "is_current": slot.get("is_current"),
        "card_section": slot.get("card_section"),
        "order_overall": slot.get("order_overall"),
        "order_section": slot.get("order_section"),
        "role": role,
        "is_main_event": role == "main_event",
        "is_co_main": role == "co_main",
        "is_co_main_event": role == "co_main",
        "scheduled_start_time_utc": slot.get("scheduled_start_time_utc"),
        "automatic_lock_time_utc": slot.get("automatic_lock_time_utc"),
        "structure_revision": slot.get("structure_revision"),
        "evidence": copy.deepcopy(slot.get("evidence", {})),
        "card_snapshot_revision": snapshot_revision,
        "canonical_slot_version": SLOT_STORAGE_VERSION,
        "source": "card_data_v1",
    }
    document["reconciliation_fingerprint"] = slot_storage_fingerprint(document)
    return document


def _slot_identity(document: Mapping[str, Any]) -> tuple[Optional[str], Optional[int]]:
    raw_slot_id = document.get("slot_id") or document.get("id") or document.get("_id")
    slot_id = str(raw_slot_id) if raw_slot_id is not None else None
    bout_id = document.get("bout_id")
    return slot_id, bout_id if _positive_int(bout_id) else None


def _changed_fields(
    current: Mapping[str, Any], desired: Mapping[str, Any]
) -> tuple[str, ...]:
    return tuple(
        field
        for field in OWNED_SLOT_FIELDS
        if field not in current or _canonical(current.get(field)) != _canonical(desired.get(field))
    ) + (
        ("reconciliation_fingerprint",)
        if current.get("reconciliation_fingerprint")
        != desired.get("reconciliation_fingerprint")
        else ()
    )


def _operation(
    action: str,
    current: Optional[Mapping[str, Any]],
    desired: Mapping[str, Any],
    changed_fields: Sequence[str],
) -> SlotOperation:
    expected_fingerprint = (
        slot_storage_fingerprint(current) if isinstance(current, Mapping) else None
    )
    payload = {
        "action": action,
        "slot_id": desired["slot_id"],
        "expected_fingerprint": expected_fingerprint,
        "desired_fingerprint": desired["reconciliation_fingerprint"],
        "changed_fields": list(changed_fields),
    }
    operation_id = (
        f"slotop_{desired['event_id']}_{desired['bout_id']}_"
        f"{_hash(payload).split(':', 1)[1][:16]}"
    )
    return SlotOperation(
        operation_id=operation_id,
        action=action,
        slot_id=desired["slot_id"],
        event_id=desired["event_id"],
        bout_id=desired["bout_id"],
        expected_fingerprint=expected_fingerprint,
        desired_fingerprint=desired["reconciliation_fingerprint"],
        expected_structure_revision=(
            current.get("structure_revision")
            if isinstance(current, Mapping)
            and isinstance(current.get("structure_revision"), int)
            else None
        ),
        desired_structure_revision=desired["structure_revision"],
        changed_fields=tuple(changed_fields),
        before=copy.deepcopy(dict(current))
        if isinstance(current, Mapping)
        else None,
        after=copy.deepcopy(dict(desired)),
    )


def _conflict_sort_key(item: SlotConflict) -> tuple[Any, ...]:
    return (
        item.code,
        "" if item.slot_id is None else item.slot_id,
        -1 if item.bout_id is None else item.bout_id,
        item.message,
    )


def plan_slot_reconciliation(
    desired_snapshot: Mapping[str, Any],
    current_documents: Sequence[Mapping[str, Any]],
) -> SlotReconciliationPlan:
    """Build a deterministic insert/update plan without mutating either input."""

    if not isinstance(desired_snapshot, Mapping):
        raise SlotReconciliationInputError("desired_snapshot must be an object.")
    validation = validate_card_data_v1(desired_snapshot)
    if not validation.is_valid:
        codes = ", ".join(validation.codes)
        raise SlotReconciliationInputError(
            f"desired_snapshot violates CardDataContractV1: {codes}"
        )
    if not _is_sequence(current_documents):
        raise SlotReconciliationInputError("current_documents must be an array.")
    if any(not isinstance(document, Mapping) for document in current_documents):
        raise SlotReconciliationInputError(
            "Every current slot document must be an object."
        )

    event = desired_snapshot["event"]
    event_id = event["event_id"]
    snapshot_id = desired_snapshot["snapshot_id"]
    snapshot_revision = desired_snapshot["snapshot_revision"]
    previous_snapshot_revision = desired_snapshot.get("source_run", {}).get(
        "previous_snapshot_revision"
    )
    desired_structure_revision = event["structure_revision"]
    raw_current = [copy.deepcopy(dict(item)) for item in current_documents]
    raw_desired = [
        copy.deepcopy(dict(item)) for item in desired_snapshot.get("card_slots", [])
    ]
    conflicts: list[SlotConflict] = []

    if desired_snapshot.get("quality", {}).get("capabilities", {}).get(
        "STRUCT"
    ) != "ready":
        conflicts.append(
            SlotConflict(
                code="STRUCT_CAPABILITY_NOT_READY",
                slot_id=None,
                bout_id=None,
                message="Canonical STRUCT capability must be ready before persistence.",
                desired_structure_revision=desired_structure_revision,
            )
        )

    desired_by_bout: dict[int, dict[str, Any]] = {}
    desired_documents = []
    for slot in raw_desired:
        if slot.get("event_id") != event_id or not _positive_int(slot.get("bout_id")):
            raise SlotReconciliationInputError(
                "Every desired slot must belong to the canonical event and bout."
            )
        if slot["bout_id"] in desired_by_bout:
            raise SlotReconciliationInputError(
                f"Duplicate desired bout_id {slot['bout_id']}."
            )
        desired = _desired_document(slot, snapshot_revision)
        desired_by_bout[slot["bout_id"]] = desired
        desired_documents.append(desired)
    desired_documents.sort(key=lambda item: (item["bout_id"], item["slot_id"]))

    current_by_bout: dict[int, dict[str, Any]] = {}
    duplicate_bouts: set[int] = set()
    storage_ids: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for document in raw_current:
        slot_id, bout_id = _slot_identity(document)
        if document.get("event_id") != event_id:
            conflicts.append(
                SlotConflict(
                    code="CROSS_EVENT_SLOT_INPUT",
                    slot_id=slot_id,
                    bout_id=bout_id,
                    message="Planner input contains a slot from another event.",
                    current_structure_revision=document.get("structure_revision"),
                    desired_structure_revision=desired_structure_revision,
                )
            )
            continue
        if slot_id is None or bout_id is None:
            conflicts.append(
                SlotConflict(
                    code="SLOT_IDENTITY_INVALID",
                    slot_id=slot_id,
                    bout_id=bout_id,
                    message="Persisted slot needs a stable slot ID and positive bout ID.",
                    current_structure_revision=document.get("structure_revision"),
                    desired_structure_revision=desired_structure_revision,
                )
            )
            continue
        storage_ids[slot_id].append(document)
        if bout_id in current_by_bout:
            duplicate_bouts.add(bout_id)
        else:
            current_by_bout[bout_id] = document

    for slot_id, documents in sorted(storage_ids.items()):
        if len(documents) > 1:
            conflicts.append(
                SlotConflict(
                    code="DUPLICATE_STORAGE_SLOT_ID",
                    slot_id=slot_id,
                    bout_id=None,
                    message="Multiple persisted documents share one slot identity.",
                    desired_structure_revision=desired_structure_revision,
                )
            )
    for bout_id in sorted(duplicate_bouts):
        conflicts.append(
            SlotConflict(
                code="DUPLICATE_PERSISTED_BOUT_SLOT",
                slot_id=None,
                bout_id=bout_id,
                message="One event has multiple persisted slots for the same bout.",
                desired_structure_revision=desired_structure_revision,
            )
        )

    operations = []
    unchanged = []
    for bout_id, desired in sorted(desired_by_bout.items()):
        if bout_id in duplicate_bouts:
            continue
        current = current_by_bout.get(bout_id)
        if current is None:
            operations.append(
                _operation("insert", None, desired, tuple(OWNED_SLOT_FIELDS))
            )
            continue
        current_slot_id, _ = _slot_identity(current)
        expected_slot_id = desired["slot_id"]
        if current_slot_id != expected_slot_id or str(
            current.get("_id", expected_slot_id)
        ) != expected_slot_id:
            conflicts.append(
                SlotConflict(
                    code="SLOT_STORAGE_ID_CONFLICT",
                    slot_id=current_slot_id,
                    bout_id=bout_id,
                    message="Persisted slot identity does not match event_id:bout_id.",
                    current_structure_revision=current.get("structure_revision"),
                    desired_structure_revision=desired["structure_revision"],
                )
            )
            continue

        current_revision = current.get("structure_revision")
        current_snapshot_revision = current.get("card_snapshot_revision")
        if isinstance(current_snapshot_revision, int) and (
            current_snapshot_revision > snapshot_revision
        ):
            conflicts.append(
                SlotConflict(
                    code="STALE_DESIRED_SNAPSHOT",
                    slot_id=expected_slot_id,
                    bout_id=bout_id,
                    message="Persisted slot belongs to a newer canonical snapshot.",
                    current_structure_revision=current_revision,
                    desired_structure_revision=desired["structure_revision"],
                )
            )
            continue
        if isinstance(current_revision, int) and current_revision > desired[
            "structure_revision"
        ]:
            conflicts.append(
                SlotConflict(
                    code="STALE_DESIRED_STRUCTURE",
                    slot_id=expected_slot_id,
                    bout_id=bout_id,
                    message="Persisted slot has a newer structure revision.",
                    current_structure_revision=current_revision,
                    desired_structure_revision=desired["structure_revision"],
                )
            )
            continue

        stored_fingerprint = current.get("reconciliation_fingerprint")
        actual_fingerprint = slot_storage_fingerprint(current)
        if (
            current.get("canonical_slot_version") == SLOT_STORAGE_VERSION
            and stored_fingerprint != actual_fingerprint
        ):
            conflicts.append(
                SlotConflict(
                    code="STORED_FINGERPRINT_MISMATCH",
                    slot_id=expected_slot_id,
                    bout_id=bout_id,
                    message="Canonical slot fields changed outside reconciliation.",
                    current_structure_revision=current_revision,
                    desired_structure_revision=desired["structure_revision"],
                )
            )
            continue

        changed = _changed_fields(current, desired)
        if not changed:
            unchanged.append(expected_slot_id)
            continue
        if (
            current.get("canonical_slot_version") == SLOT_STORAGE_VERSION
            and current_snapshot_revision == snapshot_revision
        ):
            conflicts.append(
                SlotConflict(
                    code="REVISION_CONTENT_CONFLICT",
                    slot_id=expected_slot_id,
                    bout_id=bout_id,
                    message="Equal canonical snapshot revisions contain different fields.",
                    current_structure_revision=current_revision,
                    desired_structure_revision=desired["structure_revision"],
                )
            )
            continue
        operations.append(_operation("update", current, desired, changed))

    for bout_id, current in sorted(current_by_bout.items()):
        if bout_id in desired_by_bout or bout_id in duplicate_bouts:
            continue
        slot_id, _ = _slot_identity(current)
        conflicts.append(
            SlotConflict(
                code="UNEXPECTED_PERSISTED_SLOT",
                slot_id=slot_id,
                bout_id=bout_id,
                message=(
                    "Persisted slot is absent from the desired snapshot; SCR-010 must "
                    "decide its lifecycle before any retirement."
                ),
                current_structure_revision=current.get("structure_revision"),
                desired_structure_revision=desired_structure_revision,
            )
        )

    operations.sort(key=lambda item: (item.bout_id, item.action, item.slot_id))
    conflicts = sorted(set(conflicts), key=_conflict_sort_key)
    unchanged = sorted(set(unchanged))
    current_digest = slot_collection_digest(raw_current)
    desired_digest = slot_collection_digest(desired_documents)
    plan_payload = {
        "plan_version": PLAN_VERSION,
        "event_id": event_id,
        "snapshot_id": snapshot_id,
        "previous_snapshot_revision": previous_snapshot_revision,
        "new_snapshot_revision": snapshot_revision,
        "current_digest": current_digest,
        "desired_digest": desired_digest,
        "operations": [operation.as_dict() for operation in operations],
        "conflicts": [conflict.as_dict() for conflict in conflicts],
    }
    plan_id = (
        f"slotplan_{event_id}_{snapshot_revision}_"
        f"{_hash(plan_payload).split(':', 1)[1][:16]}"
    )
    return SlotReconciliationPlan(
        plan_version=PLAN_VERSION,
        plan_id=plan_id,
        event_id=event_id,
        snapshot_id=snapshot_id,
        previous_snapshot_revision=previous_snapshot_revision,
        new_snapshot_revision=snapshot_revision,
        desired_structure_revision=desired_structure_revision,
        current_digest=current_digest,
        desired_digest=desired_digest,
        desired_documents=tuple(copy.deepcopy(desired_documents)),
        operations=tuple(operations),
        unchanged_slot_ids=tuple(unchanged),
        conflicts=tuple(conflicts),
    )


def _convergence_conflicts(
    plan: SlotReconciliationPlan,
    final_documents: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    expected = {
        document["slot_id"]: document for document in plan.desired_documents
    }
    actual: dict[str, Mapping[str, Any]] = {}
    issues = []
    for document in final_documents:
        if document.get("event_id") != plan.event_id:
            issues.append("CROSS_EVENT_FINAL_DOCUMENT")
            continue
        slot_id, _ = _slot_identity(document)
        if slot_id is None or slot_id in actual:
            issues.append("FINAL_SLOT_IDENTITY_INVALID")
            continue
        actual[slot_id] = document
    if set(actual) != set(expected):
        issues.append("FINAL_SLOT_SET_MISMATCH")
    for slot_id in sorted(set(actual) & set(expected)):
        if _canonical(_owned_projection(actual[slot_id])) != _canonical(
            _owned_projection(expected[slot_id])
        ):
            issues.append(f"FINAL_SLOT_CONTENT_MISMATCH:{slot_id}")
        if actual[slot_id].get("reconciliation_fingerprint") != expected[
            slot_id
        ].get("reconciliation_fingerprint"):
            issues.append(f"FINAL_SLOT_FINGERPRINT_MISMATCH:{slot_id}")
    if slot_collection_digest(final_documents) != plan.desired_digest:
        issues.append("FINAL_EVENT_DIGEST_MISMATCH")
    return tuple(sorted(set(issues)))


def apply_slot_reconciliation(
    plan: SlotReconciliationPlan,
    store: Optional[SlotReconciliationStore] = None,
    *,
    dry_run: bool = True,
    expected_plan_id: Optional[str] = None,
) -> SlotApplyResult:
    """Apply one exact reviewed plan through an atomic injected storage port."""

    if not isinstance(plan, SlotReconciliationPlan):
        raise SlotReconciliationInputError("plan must be a SlotReconciliationPlan.")
    if dry_run:
        return SlotApplyResult(
            plan_id=plan.plan_id,
            dry_run=True,
            safe_to_apply=plan.safe_to_apply,
            attempted_operation_count=len(plan.operations),
            applied_operation_ids=(),
            verified_converged=plan.converged,
        )
    if expected_plan_id != plan.plan_id:
        raise SlotReconciliationApplyError(
            "Write application requires the exact reviewed expected_plan_id."
        )
    if not plan.safe_to_apply:
        codes = ", ".join(conflict.code for conflict in plan.conflicts)
        raise UnsafeSlotReconciliationPlan(
            f"Slot plan has blocking conflicts: {codes or 'unknown'}"
        )
    if store is None:
        raise SlotReconciliationApplyError(
            "A storage port is required when dry_run is false."
        )

    receipt = store.commit_slot_plan(plan)
    if receipt.plan_id != plan.plan_id:
        raise SlotReconciliationApplyError("Storage receipt plan_id mismatch.")
    if receipt.previous_digest != plan.current_digest:
        raise SlotReconciliationApplyError(
            "Storage did not confirm the reviewed current event digest."
        )
    expected_operation_ids = tuple(
        operation.operation_id for operation in plan.operations
    )
    if tuple(sorted(receipt.applied_operation_ids)) != tuple(
        sorted(expected_operation_ids)
    ):
        raise SlotReconciliationApplyError(
            "Storage receipt does not cover every planned operation exactly once."
        )
    convergence_issues = _convergence_conflicts(plan, receipt.final_documents)
    if convergence_issues:
        raise SlotReconciliationApplyError(
            "Storage commit did not converge: " + ", ".join(convergence_issues)
        )
    return SlotApplyResult(
        plan_id=plan.plan_id,
        dry_run=False,
        safe_to_apply=True,
        attempted_operation_count=len(plan.operations),
        applied_operation_ids=expected_operation_ids,
        verified_converged=True,
    )


__all__ = [
    "OWNED_SLOT_FIELDS",
    "PLAN_VERSION",
    "SLOT_STORAGE_VERSION",
    "SlotApplyResult",
    "SlotCommitReceipt",
    "SlotConflict",
    "SlotOperation",
    "SlotReconciliationApplyError",
    "SlotReconciliationInputError",
    "SlotReconciliationPlan",
    "SlotReconciliationStore",
    "UnsafeSlotReconciliationPlan",
    "apply_slot_reconciliation",
    "legacy_bout_slot_projection",
    "plan_slot_reconciliation",
    "slot_collection_digest",
    "slot_storage_fingerprint",
]
