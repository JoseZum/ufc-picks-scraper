"""Guarded CardData production-backfill package and Mongo adapter.

SCR-012B separates preparation from execution:

* production reads build a deterministic, sanitized run manifest;
* exact owned-field preimages are encrypted outside the workspace;
* the concrete Mongo adapter remains dry-run by default and requires a
  separately validated authorization naming the exact run and slot plans;
* every production mutation is bounded to the three approved events and three
  CardData collections, uses one transaction, and verifies convergence.

The CLI in this module only prepares a no-write package. It intentionally has
no command-line switch that can execute the Mongo adapter.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TextIO

from bson import json_util
from cryptography.fernet import Fernet, InvalidToken

from tapology_scraper.admin_title_attestation import (
    AdminTitleAttestation,
    AdminTitleAttestationError,
    load_admin_title_attestation,
)
from tapology_scraper.backfill_reconciliation_report import (
    BACKFILL_BOUT_PROJECTION,
    BACKFILL_EVENT_PROJECTION,
    BACKFILL_SLOT_PROJECTION,
    BOUT_OWNED_FIELDS,
    EVENT_OWNED_FIELDS,
    BackfillCardDryRun,
    BackfillDryRunConfigurationError,
    build_card_backfill_projection,
    fetch_backfill_cards,
)
from tapology_scraper.production_card_audit import (
    GOLDEN_CARD_SPECS,
    GoldenCardSpec,
    LegacyCardDocuments,
    ProductionAuditConfigurationError,
    load_mongo_settings,
)
from tapology_scraper.slot_reconciliation import (
    OWNED_SLOT_FIELDS,
    SlotOperation,
    SlotReconciliationPlan,
)


PACKAGE_SCHEMA_VERSION = "production-carddata-backfill-package/v1"
PREIMAGE_ARCHIVE_VERSION = "production-carddata-preimages/v1"
AUTHORIZATION_SCHEMA_VERSION = "production-carddata-write-authorization/v1"
TARGET_EVENT_IDS = (136871, 142341, 142997)
TARGET_SPECS = tuple(
    spec for spec in GOLDEN_CARD_SPECS if spec.event_id in TARGET_EVENT_IDS
)
TARGET_COLLECTIONS = ("events", "bouts", "event_card_slots")
SLOT_STATE_FIELDS = (*OWNED_SLOT_FIELDS, "reconciliation_fingerprint")
EVENT_STATE_FIELDS = (*EVENT_OWNED_FIELDS, "card_data_v1")
BOUT_STATE_FIELDS = (*BOUT_OWNED_FIELDS, "card_data_v1")
EXIT_PREPARED = 0
EXIT_BLOCKED = 1
EXIT_CONFIGURATION_ERROR = 2


class ProductionBackfillPackageError(ValueError):
    """Raised when an execution package is incomplete, stale or unsafe."""


class ProductionBackfillDriftError(RuntimeError):
    """Raised before mutation when production differs from the reviewed run."""


class ProductionBackfillExecutionError(RuntimeError):
    """Raised when a guarded transaction cannot be verified."""


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _utc_timestamp(value: Any) -> Optional[str]:
    if not _nonempty(value):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json_util.dumps(
        value,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _document_snapshot(
    document: Mapping[str, Any],
    fields: Sequence[str],
) -> dict[str, Any]:
    return {
        "values": {
            field: copy.deepcopy(document[field])
            for field in fields
            if field in document
        },
        "missing_fields": sorted(field for field in fields if field not in document),
    }


def _field_changed(
    current: Mapping[str, Any],
    desired: Mapping[str, Any],
    field: str,
) -> bool:
    if (field in current) != (field in desired):
        return True
    if field not in current:
        return False
    return _canonical(current[field]) != _canonical(desired[field])


def _changed_fields(
    current: Mapping[str, Any],
    desired: Mapping[str, Any],
    fields: Sequence[str],
) -> tuple[str, ...]:
    return tuple(field for field in fields if _field_changed(current, desired, field))


def _document_id(document: Mapping[str, Any]) -> Optional[int]:
    value = document.get("id") or document.get("bout_id")
    return value if _positive_int(value) else None


def _event_state_payload(card: LegacyCardDocuments) -> dict[str, Any]:
    if not isinstance(card.event, Mapping):
        raise ProductionBackfillPackageError(
            "Every target requires one event document."
        )
    bouts = []
    for document in card.bouts:
        bout_id = _document_id(document)
        if bout_id is None:
            raise ProductionBackfillPackageError(
                "Target contains a bout without a stable ID."
            )
        bouts.append(
            {
                "identity": {"event_id": card.spec.event_id, "id": bout_id},
                "owned": _document_snapshot(document, BOUT_STATE_FIELDS),
            }
        )
    slots = []
    for document in card.slots:
        bout_id = document.get("bout_id")
        if not _positive_int(bout_id):
            raise ProductionBackfillPackageError(
                "Target contains a slot without a bout ID."
            )
        slots.append(
            {
                "identity": {
                    "event_id": card.spec.event_id,
                    "bout_id": bout_id,
                    "storage_id": str(
                        document.get("_id")
                        or document.get("slot_id")
                        or document.get("id")
                        or ""
                    ),
                },
                "owned": _document_snapshot(document, SLOT_STATE_FIELDS),
            }
        )
    bouts.sort(key=lambda item: item["identity"]["id"])
    slots.sort(
        key=lambda item: (
            item["identity"]["bout_id"],
            item["identity"]["storage_id"],
        )
    )
    return {
        "event_id": card.spec.event_id,
        "event": {
            "identity": {"id": card.spec.event_id},
            "owned": _document_snapshot(card.event, EVENT_STATE_FIELDS),
        },
        "bouts": bouts,
        "slots": slots,
    }


def _desired_state_payload(
    event_id: int,
    current_card: LegacyCardDocuments,
    snapshot: Mapping[str, Any],
    slot_plan: SlotReconciliationPlan,
) -> dict[str, Any]:
    event = snapshot.get("event")
    if not isinstance(event, Mapping) or event.get("event_id") != event_id:
        raise ProductionBackfillPackageError("Desired event identity is invalid.")
    current_bouts = {
        _document_id(item): item
        for item in current_card.bouts
        if _document_id(item) is not None
    }
    bouts = []
    for document in snapshot.get("bouts", []):
        if not isinstance(document, Mapping) or not _positive_int(
            document.get("bout_id")
        ):
            raise ProductionBackfillPackageError("Desired bout identity is invalid.")
        current = current_bouts.get(document["bout_id"])
        if current is None:
            raise ProductionBackfillPackageError(
                "Desired bout is absent from the approved current card."
            )
        stored = copy.deepcopy(dict(current))
        stored["card_data_v1"] = copy.deepcopy(dict(document))
        bouts.append(
            {
                "identity": {"event_id": event_id, "id": document["bout_id"]},
                "owned": _document_snapshot(stored, BOUT_STATE_FIELDS),
            }
        )
    slots = [
        {
            "identity": {
                "event_id": event_id,
                "bout_id": document["bout_id"],
                "storage_id": str(document["_id"]),
            },
            "owned": _document_snapshot(document, SLOT_STATE_FIELDS),
        }
        for document in slot_plan.desired_documents
    ]
    bouts.sort(key=lambda item: item["identity"]["id"])
    slots.sort(
        key=lambda item: (
            item["identity"]["bout_id"],
            item["identity"]["storage_id"],
        )
    )
    stored_event = copy.deepcopy(dict(current_card.event or {}))
    stored_event["card_data_v1"] = copy.deepcopy(dict(event))
    return {
        "event_id": event_id,
        "event": {
            "identity": {"id": event_id},
            "owned": _document_snapshot(stored_event, EVENT_STATE_FIELDS),
        },
        "bouts": bouts,
        "slots": slots,
    }


@dataclass(frozen=True)
class DocumentWritePlan:
    collection: str
    document_id: int
    changed_fields: tuple[str, ...]
    desired_values: Mapping[str, Any]
    expected_document_digest: str


@dataclass(frozen=True)
class EventPackageManifest:
    event_id: int
    preimage_digest: str
    desired_state_digest: str
    snapshot_id: str
    snapshot_digest: str
    slot_plan_id: str
    slot_current_digest: str
    slot_desired_digest: str
    event_update_count: int
    bout_update_count: int
    slot_insert_count: int
    slot_update_count: int
    slot_conflict_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackfillRunManifest:
    run_id: str
    created_at: str
    attestation_id: str
    event_plans: tuple[EventPackageManifest, ...]
    preimage_set_digest: str
    desired_set_digest: str
    encrypted_archive_digest: Optional[str] = None

    @property
    def target_event_ids(self) -> tuple[int, ...]:
        return tuple(item.event_id for item in self.event_plans)

    @property
    def slot_plan_ids(self) -> tuple[str, ...]:
        return tuple(item.slot_plan_id for item in self.event_plans)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PACKAGE_SCHEMA_VERSION,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "target_event_ids": list(self.target_event_ids),
            "target_collections": list(TARGET_COLLECTIONS),
            "admin_title_attestation_id": self.attestation_id,
            "dry_run": True,
            "write_executed": False,
            "production_write_authorized": False,
            "deletes_proposed": 0,
            "preimage_set_digest": self.preimage_set_digest,
            "desired_set_digest": self.desired_set_digest,
            "encrypted_archive": {
                "written_outside_workspace": self.encrypted_archive_digest is not None,
                "ciphertext_digest": self.encrypted_archive_digest,
                "path_included": False,
                "key_included": False,
            },
            "event_plans": [item.as_dict() for item in self.event_plans],
            "required_execution_guards": [
                "Separate exact production authorization naming this run and every slot plan.",
                "Fresh in-transaction preimage digests must match or already be desired.",
                "One transaction covers every selected event and collection mutation.",
                "Only declared CardData-owned fields may change; deletes are forbidden.",
                "Post-commit state and immediate replay must be fully converged.",
            ],
        }


@dataclass(frozen=True)
class EventBackfillPlan:
    spec: GoldenCardSpec
    result: BackfillCardDryRun
    snapshot: Mapping[str, Any]
    slot_plan: SlotReconciliationPlan
    preimage_payload: Mapping[str, Any]
    desired_state_payload: Mapping[str, Any]
    event_write: Optional[DocumentWritePlan]
    bout_writes: tuple[DocumentWritePlan, ...]


@dataclass(frozen=True)
class PreparedBackfillRun:
    manifest: BackfillRunManifest
    event_plans: tuple[EventBackfillPlan, ...]
    preimage_archive: Mapping[str, Any]
    attestation: AdminTitleAttestation


def _event_write_plan(
    current: Mapping[str, Any],
    desired: Mapping[str, Any],
    event_id: int,
) -> Optional[DocumentWritePlan]:
    if not _field_changed(current, {"card_data_v1": desired}, "card_data_v1"):
        return None
    return DocumentWritePlan(
        collection="events",
        document_id=event_id,
        changed_fields=("card_data_v1",),
        desired_values={"card_data_v1": copy.deepcopy(dict(desired))},
        expected_document_digest=_hash(_document_snapshot(current, ("card_data_v1",))),
    )


def _bout_write_plans(
    current_documents: Sequence[Mapping[str, Any]],
    desired_documents: Sequence[Mapping[str, Any]],
    event_id: int,
) -> tuple[DocumentWritePlan, ...]:
    current = {
        _document_id(item): item
        for item in current_documents
        if _document_id(item) is not None
    }
    desired_ids = {item.get("bout_id") for item in desired_documents}
    if set(current) != desired_ids:
        raise ProductionBackfillPackageError(
            f"Event {event_id} bout identity set changed; inserts/deletes are not approved."
        )
    writes = []
    for desired in sorted(desired_documents, key=lambda item: item["bout_id"]):
        bout_id = desired["bout_id"]
        before = current[bout_id]
        if not _field_changed(before, {"card_data_v1": desired}, "card_data_v1"):
            continue
        writes.append(
            DocumentWritePlan(
                collection="bouts",
                document_id=bout_id,
                changed_fields=("card_data_v1",),
                desired_values={"card_data_v1": copy.deepcopy(dict(desired))},
                expected_document_digest=_hash(
                    _document_snapshot(before, ("card_data_v1",))
                ),
            )
        )
    return tuple(writes)


def prepare_backfill_run(
    cards: Sequence[LegacyCardDocuments],
    attestation: AdminTitleAttestation,
    *,
    created_at: str,
) -> PreparedBackfillRun:
    """Create a deterministic run in memory without writing any data."""

    normalized_created_at = _utc_timestamp(created_at)
    if normalized_created_at is None:
        raise ProductionBackfillPackageError(
            "created_at must be an explicit UTC timestamp."
        )
    card_ids = tuple(sorted(card.spec.event_id for card in cards))
    if card_ids != TARGET_EVENT_IDS or len(set(card_ids)) != len(card_ids):
        raise ProductionBackfillPackageError(
            "Package scope must be exactly events 136871, 142341 and 142997."
        )
    if attestation.scope_event_ids != TARGET_EVENT_IDS:
        raise ProductionBackfillPackageError(
            "Admin TITLE attestation scope must exactly match the package scope."
        )
    if attestation.production_write_authorized:
        raise ProductionBackfillPackageError(
            "The package attestation must remain restricted to dry-run use."
        )

    event_plans = []
    event_manifests = []
    for card in sorted(cards, key=lambda item: item.spec.event_id):
        projection = build_card_backfill_projection(card, attestation)
        if (
            projection.result.review_status != "REVIEWABLE"
            or projection.snapshot is None
            or projection.slot_plan is None
            or not projection.slot_plan.safe_to_apply
        ):
            raise ProductionBackfillPackageError(
                f"Event {card.spec.event_id} is not reviewable for package preparation."
            )
        snapshot = projection.snapshot
        slot_plan = projection.slot_plan
        preimage_payload = _event_state_payload(card)
        desired_payload = _desired_state_payload(
            card.spec.event_id,
            card,
            snapshot,
            slot_plan,
        )
        desired_event = snapshot["event"]
        event_write = _event_write_plan(card.event, desired_event, card.spec.event_id)
        bout_writes = _bout_write_plans(
            card.bouts,
            snapshot["bouts"],
            card.spec.event_id,
        )
        summary = slot_plan.summary()
        counts = dict(projection.result.counts)
        if counts.get("event_updates", 0) != int(event_write is not None):
            raise ProductionBackfillPackageError(
                "Event operation count is inconsistent."
            )
        if counts.get("bout_updates", 0) != len(bout_writes):
            raise ProductionBackfillPackageError(
                "Bout operation count is inconsistent."
            )
        event_plan = EventBackfillPlan(
            spec=card.spec,
            result=projection.result,
            snapshot=copy.deepcopy(snapshot),
            slot_plan=slot_plan,
            preimage_payload=copy.deepcopy(preimage_payload),
            desired_state_payload=copy.deepcopy(desired_payload),
            event_write=event_write,
            bout_writes=bout_writes,
        )
        event_plans.append(event_plan)
        event_manifests.append(
            EventPackageManifest(
                event_id=card.spec.event_id,
                preimage_digest=_hash(preimage_payload),
                desired_state_digest=_hash(desired_payload),
                snapshot_id=str(snapshot["snapshot_id"]),
                snapshot_digest=_hash(snapshot),
                slot_plan_id=slot_plan.plan_id,
                slot_current_digest=slot_plan.current_digest,
                slot_desired_digest=slot_plan.desired_digest,
                event_update_count=int(event_write is not None),
                bout_update_count=len(bout_writes),
                slot_insert_count=summary["insert_count"],
                slot_update_count=summary["update_count"],
                slot_conflict_count=summary["conflict_count"],
            )
        )

    preimage_set_digest = _hash(
        [
            {"event_id": item.event_id, "digest": item.preimage_digest}
            for item in event_manifests
        ]
    )
    desired_set_digest = _hash(
        [
            {"event_id": item.event_id, "digest": item.desired_state_digest}
            for item in event_manifests
        ]
    )
    run_payload = {
        "schema_version": PACKAGE_SCHEMA_VERSION,
        "attestation_id": attestation.attestation_id,
        "target_event_ids": list(TARGET_EVENT_IDS),
        "preimage_set_digest": preimage_set_digest,
        "desired_set_digest": desired_set_digest,
        "slot_plan_ids": [item.slot_plan_id for item in event_manifests],
    }
    run_id = "carddata_backfill_" + _hash(run_payload).split(":", 1)[1][:24]
    manifest = BackfillRunManifest(
        run_id=run_id,
        created_at=normalized_created_at,
        attestation_id=attestation.attestation_id,
        event_plans=tuple(event_manifests),
        preimage_set_digest=preimage_set_digest,
        desired_set_digest=desired_set_digest,
    )
    archive = {
        "archive_version": PREIMAGE_ARCHIVE_VERSION,
        "run_id": run_id,
        "created_at": normalized_created_at,
        "attestation_id": attestation.attestation_id,
        "target_event_ids": list(TARGET_EVENT_IDS),
        "preimage_set_digest": preimage_set_digest,
        "events": [copy.deepcopy(item.preimage_payload) for item in event_plans],
    }
    return PreparedBackfillRun(
        manifest=manifest,
        event_plans=tuple(event_plans),
        preimage_archive=archive,
        attestation=attestation,
    )


def encrypt_preimage_archive(
    archive: Mapping[str, Any],
    key: bytes,
) -> tuple[bytes, str]:
    try:
        cipher = Fernet(key)
    except (TypeError, ValueError) as exc:
        raise ProductionBackfillPackageError(
            "Preimage key is not a valid Fernet key."
        ) from exc
    plaintext = _canonical(archive).encode("utf-8")
    ciphertext = cipher.encrypt(plaintext)
    return ciphertext, "sha256:" + hashlib.sha256(ciphertext).hexdigest()


def decrypt_preimage_archive(
    ciphertext: bytes,
    key: bytes,
    *,
    expected_preimage_set_digest: str,
) -> Mapping[str, Any]:
    try:
        plaintext = Fernet(key).decrypt(ciphertext)
        archive = json_util.loads(plaintext.decode("utf-8"))
    except (InvalidToken, TypeError, ValueError, UnicodeError) as exc:
        raise ProductionBackfillPackageError(
            "Encrypted preimage archive is invalid."
        ) from exc
    if not isinstance(archive, Mapping):
        raise ProductionBackfillPackageError(
            "Decrypted preimage archive is not an object."
        )
    if archive.get("archive_version") != PREIMAGE_ARCHIVE_VERSION:
        raise ProductionBackfillPackageError("Unsupported preimage archive version.")
    if archive.get("preimage_set_digest") != expected_preimage_set_digest:
        raise ProductionBackfillPackageError(
            "Preimage archive digest does not match the run."
        )
    events = archive.get("events")
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes, bytearray)):
        raise ProductionBackfillPackageError("Preimage archive events are invalid.")
    actual_digest = _hash(
        [
            {"event_id": item.get("event_id"), "digest": _hash(item)}
            for item in events
            if isinstance(item, Mapping)
        ]
    )
    if actual_digest != expected_preimage_set_digest:
        raise ProductionBackfillPackageError(
            "Preimage archive contents failed integrity validation."
        )
    return archive


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_private_path(path: Path, protected_roots: Sequence[Path]) -> Path:
    resolved = path.expanduser().resolve()
    if any(_is_within(resolved, root) for root in protected_roots):
        raise ProductionBackfillPackageError(
            "Encrypted preimages and keys must be stored outside the workspace."
        )
    if not resolved.parent.is_dir():
        raise ProductionBackfillPackageError(
            "Private output parent directory must already exist."
        )
    return resolved


def _write_exclusive(path: Path, content: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise ProductionBackfillPackageError(
            "Refusing to overwrite an existing file."
        ) from exc
    except OSError as exc:
        raise ProductionBackfillPackageError(
            f"Private artifact could not be created ({type(exc).__name__})."
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def create_preimage_key_file(path: Path, protected_roots: Sequence[Path]) -> bytes:
    resolved = require_private_path(path, protected_roots)
    key = Fernet.generate_key()
    _write_exclusive(resolved, key + b"\n")
    return key


def load_preimage_key_file(path: Path, protected_roots: Sequence[Path]) -> bytes:
    resolved = require_private_path(path, protected_roots)
    if not resolved.is_file():
        raise ProductionBackfillPackageError("Preimage key file does not exist.")
    try:
        key = resolved.read_bytes().strip()
        Fernet(key)
    except (OSError, TypeError, ValueError) as exc:
        raise ProductionBackfillPackageError("Preimage key file is invalid.") from exc
    return key


def write_encrypted_preimages(
    run: PreparedBackfillRun,
    output_path: Path,
    key: bytes,
    protected_roots: Sequence[Path],
) -> PreparedBackfillRun:
    resolved = require_private_path(output_path, protected_roots)
    ciphertext, digest = encrypt_preimage_archive(run.preimage_archive, key)
    _write_exclusive(resolved, ciphertext)
    return replace(
        run,
        manifest=replace(run.manifest, encrypted_archive_digest=digest),
    )


def render_manifest_json(manifest: BackfillRunManifest) -> str:
    return json.dumps(manifest.as_dict(), indent=2, sort_keys=True) + "\n"


def render_manifest_markdown(manifest: BackfillRunManifest) -> str:
    lines = [
        "# SCR-012B Final CardData Backfill Package",
        "",
        f"- Run ID: `{manifest.run_id}`",
        f"- Created at: `{manifest.created_at}`",
        f"- Result: `{'PREPARED' if manifest.encrypted_archive_digest else 'BLOCKED'}`",
        f"- Target events: `{', '.join(str(value) for value in manifest.target_event_ids)}`",
        f"- Target collections: `{', '.join(TARGET_COLLECTIONS)}`",
        f"- Admin TITLE attestation: `{manifest.attestation_id}`",
        "- Writes executed: `NO`",
        "- Production write authorized: `NO`",
        "- Deletes proposed: `0`",
        "- Encrypted preimages stored outside workspace: "
        f"`{'YES' if manifest.encrypted_archive_digest else 'NO'}`",
        "- Encryption key included: `NO`",
        "- Private path included: `NO`",
        "",
        "## Exact event plans",
        "",
        "| Event | Snapshot | Slot plan | Event updates | Bout updates | Slot inserts | Slot updates | Conflicts |",
        "|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for item in manifest.event_plans:
        lines.append(
            f"| {item.event_id} | `{item.snapshot_id}` | `{item.slot_plan_id}` | "
            f"{item.event_update_count} | {item.bout_update_count} | "
            f"{item.slot_insert_count} | {item.slot_update_count} | "
            f"{item.slot_conflict_count} |"
        )
    lines.extend(
        [
            "",
            "## Integrity tokens",
            "",
            f"- Preimage set digest: `{manifest.preimage_set_digest}`",
            f"- Desired set digest: `{manifest.desired_set_digest}`",
            f"- Encrypted archive digest: `{manifest.encrypted_archive_digest or '—'}`",
            "",
            "## Remaining production gate",
            "",
            "This package is intentionally no-write. Production execution requires a "
            "separate validated authorization that names this exact run ID, all three "
            "event IDs and all three slot plan IDs. The adapter must then re-read inside "
            "one transaction, reject drift before mutation, preserve unrelated fields, "
            "perform zero deletes and prove post-commit convergence.",
        ]
    )
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class ProductionWriteAuthorization:
    authorized_by: str
    authorized_at: str
    reason: str
    run_id: str
    target_event_ids: tuple[int, ...]
    slot_plan_ids: tuple[tuple[int, str], ...]
    production_write_authorized: bool
    authorization_id: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": AUTHORIZATION_SCHEMA_VERSION,
            "authorized_by": self.authorized_by,
            "authorized_at": self.authorized_at,
            "reason": self.reason,
            "run_id": self.run_id,
            "target_event_ids": list(self.target_event_ids),
            "slot_plan_ids": [
                {"event_id": event_id, "plan_id": plan_id}
                for event_id, plan_id in self.slot_plan_ids
            ],
            "production_write_authorized": self.production_write_authorized,
        }

    def validate_run(self, manifest: BackfillRunManifest) -> None:
        expected_plans = tuple(
            (item.event_id, item.slot_plan_id) for item in manifest.event_plans
        )
        if self.run_id != manifest.run_id:
            raise ProductionBackfillPackageError(
                "Production authorization names a different run ID."
            )
        if self.target_event_ids != manifest.target_event_ids:
            raise ProductionBackfillPackageError(
                "Production authorization names a different event scope."
            )
        if self.slot_plan_ids != expected_plans:
            raise ProductionBackfillPackageError(
                "Production authorization names different slot plan IDs."
            )
        if self.production_write_authorized is not True:
            raise ProductionBackfillPackageError(
                "Production authorization does not explicitly permit the write."
            )


def parse_production_write_authorization(value: Any) -> ProductionWriteAuthorization:
    if not isinstance(value, Mapping):
        raise ProductionBackfillPackageError(
            "Production authorization must be an object."
        )
    if value.get("schema_version") != AUTHORIZATION_SCHEMA_VERSION:
        raise ProductionBackfillPackageError(
            "Unsupported production authorization schema."
        )
    authorized_by = value.get("authorized_by")
    authorized_at = _utc_timestamp(value.get("authorized_at"))
    reason = value.get("reason")
    run_id = value.get("run_id")
    if not all(_nonempty(item) for item in (authorized_by, reason, run_id)):
        raise ProductionBackfillPackageError(
            "Production authorization requires author, reason and run ID."
        )
    if authorized_at is None:
        raise ProductionBackfillPackageError(
            "Production authorization time must be explicit UTC."
        )
    raw_events = value.get("target_event_ids")
    if not isinstance(raw_events, Sequence) or isinstance(
        raw_events, (str, bytes, bytearray)
    ):
        raise ProductionBackfillPackageError("Authorization event scope is invalid.")
    event_ids = tuple(raw_events)
    if event_ids != TARGET_EVENT_IDS:
        raise ProductionBackfillPackageError(
            "Authorization event scope must be the exact approved ordered scope."
        )
    raw_plans = value.get("slot_plan_ids")
    if not isinstance(raw_plans, Sequence) or isinstance(
        raw_plans, (str, bytes, bytearray)
    ):
        raise ProductionBackfillPackageError("Authorization slot plans are invalid.")
    plans = []
    for raw in raw_plans:
        if not isinstance(raw, Mapping):
            raise ProductionBackfillPackageError(
                "Every authorized slot plan must be an object."
            )
        event_id = raw.get("event_id")
        plan_id = raw.get("plan_id")
        if not _positive_int(event_id) or not _nonempty(plan_id):
            raise ProductionBackfillPackageError(
                "Every authorized slot plan requires event_id and plan_id."
            )
        plans.append((event_id, plan_id.strip()))
    if tuple(event_id for event_id, _ in plans) != TARGET_EVENT_IDS:
        raise ProductionBackfillPackageError(
            "Authorization must name one ordered slot plan per target event."
        )
    if value.get("production_write_authorized") is not True:
        raise ProductionBackfillPackageError(
            "Production write must be explicitly authorized as true."
        )
    provisional = ProductionWriteAuthorization(
        authorized_by=authorized_by.strip(),
        authorized_at=authorized_at,
        reason=reason.strip(),
        run_id=run_id.strip(),
        target_event_ids=event_ids,
        slot_plan_ids=tuple(plans),
        production_write_authorized=True,
        authorization_id="",
    )
    authorization_id = _hash(provisional.canonical_payload())
    expected_id = value.get("authorization_id")
    if expected_id is not None and expected_id != authorization_id:
        raise ProductionBackfillPackageError(
            "Production authorization ID does not match its payload."
        )
    return replace(provisional, authorization_id=authorization_id)


def load_production_write_authorization(path: Path) -> ProductionWriteAuthorization:
    if not path.is_file():
        raise ProductionBackfillPackageError(
            "Production authorization file does not exist."
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionBackfillPackageError(
            f"Production authorization could not be read ({type(exc).__name__})."
        ) from exc
    return parse_production_write_authorization(value)


@dataclass(frozen=True)
class BackfillExecutionReceipt:
    run_id: str
    authorization_id: Optional[str]
    dry_run: bool
    write_executed: bool
    event_states: tuple[tuple[int, str], ...]
    transaction_committed: bool
    post_commit_verified: bool
    replay_converged: bool

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["event_states"] = [
            {"event_id": event_id, "state": state}
            for event_id, state in self.event_states
        ]
        return value


def _fetch_card_with_session(
    database: Any,
    spec: GoldenCardSpec,
    session: Any,
) -> LegacyCardDocuments:
    event = database["events"].find_one(
        {"id": spec.event_id},
        BACKFILL_EVENT_PROJECTION,
        max_time_ms=10_000,
        session=session,
    )
    bouts = tuple(
        database["bouts"]
        .find(
            {"event_id": spec.event_id},
            BACKFILL_BOUT_PROJECTION,
            session=session,
        )
        .sort("id", 1)
        .max_time_ms(10_000)
    )
    slots = tuple(
        database["event_card_slots"]
        .find(
            {"event_id": spec.event_id},
            BACKFILL_SLOT_PROJECTION,
            session=session,
        )
        .sort("bout_id", 1)
        .max_time_ms(10_000)
    )
    return LegacyCardDocuments(spec=spec, event=event, bouts=bouts, slots=slots)


def _assert_write_result(result: Any, collection: str, document_id: Any) -> None:
    if getattr(result, "matched_count", 0) != 1:
        raise ProductionBackfillExecutionError(
            f"Guarded {collection} update did not match its expected document."
        )
    if getattr(result, "acknowledged", True) is not True:
        raise ProductionBackfillExecutionError(
            f"Guarded {collection} update was not acknowledged."
        )


class MongoCardDataBackfillAdapter:
    """Concrete exact-run adapter; no CLI path invokes :meth:`execute`."""

    def __init__(self, client: Any, database_name: str):
        if not _nonempty(database_name):
            raise ProductionBackfillPackageError("Database name is required.")
        self._client = client
        self._database_name = database_name

    @staticmethod
    def _apply_event(
        database: Any,
        plan: EventBackfillPlan,
        session: Any,
    ) -> None:
        event_id = plan.spec.event_id
        if plan.event_write is not None:
            result = database["events"].update_one(
                {"id": event_id},
                {"$set": copy.deepcopy(dict(plan.event_write.desired_values))},
                session=session,
            )
            _assert_write_result(result, "events", event_id)
        for operation in plan.bout_writes:
            result = database["bouts"].update_one(
                {"id": operation.document_id, "event_id": event_id},
                {"$set": copy.deepcopy(dict(operation.desired_values))},
                session=session,
            )
            _assert_write_result(result, "bouts", operation.document_id)
        for operation in plan.slot_plan.operations:
            MongoCardDataBackfillAdapter._apply_slot_operation(
                database,
                operation,
                session,
            )

    @staticmethod
    def _apply_slot_operation(
        database: Any,
        operation: SlotOperation,
        session: Any,
    ) -> None:
        collection = database["event_card_slots"]
        if operation.action == "insert":
            result = collection.insert_one(
                copy.deepcopy(dict(operation.after)),
                session=session,
            )
            if getattr(result, "acknowledged", True) is not True:
                raise ProductionBackfillExecutionError(
                    "Guarded event_card_slots insert was not acknowledged."
                )
            return
        if operation.action != "update":
            raise ProductionBackfillExecutionError("Unsupported slot operation action.")
        if "_id" in operation.changed_fields:
            raise ProductionBackfillExecutionError(
                "Slot storage identity cannot be updated."
            )
        values = {
            field: copy.deepcopy(operation.after[field])
            for field in operation.changed_fields
        }
        result = collection.update_one(
            {
                "_id": operation.slot_id,
                "event_id": operation.event_id,
                "bout_id": operation.bout_id,
            },
            {"$set": values},
            session=session,
        )
        _assert_write_result(result, "event_card_slots", operation.slot_id)

    @staticmethod
    def _state_for_plan(
        database: Any,
        plan: EventBackfillPlan,
        session: Any,
    ) -> tuple[str, LegacyCardDocuments]:
        card = _fetch_card_with_session(database, plan.spec, session)
        digest = _hash(_event_state_payload(card))
        event_manifest_preimage = _hash(plan.preimage_payload)
        event_manifest_desired = _hash(plan.desired_state_payload)
        if digest == event_manifest_preimage:
            return "PREIMAGE_MATCH", card
        if digest == event_manifest_desired:
            return "ALREADY_CONVERGED", card
        raise ProductionBackfillDriftError(
            f"Event {plan.spec.event_id} differs from both reviewed and desired state."
        )

    @staticmethod
    def _verify_replay(
        cards: Sequence[LegacyCardDocuments],
        run: PreparedBackfillRun,
    ) -> bool:
        for card in cards:
            projection = build_card_backfill_projection(card, run.attestation)
            if (
                projection.result.review_status != "REVIEWABLE"
                or projection.snapshot is None
                or projection.slot_plan is None
                or projection.slot_plan.operations
                or projection.slot_plan.conflicts
            ):
                return False
            desired_event = projection.snapshot["event"]
            if (
                _event_write_plan(card.event, desired_event, card.spec.event_id)
                is not None
            ):
                return False
            if _bout_write_plans(
                card.bouts,
                projection.snapshot["bouts"],
                card.spec.event_id,
            ):
                return False
        return True

    def execute(
        self,
        run: PreparedBackfillRun,
        authorization: Optional[ProductionWriteAuthorization] = None,
        *,
        execute: bool = False,
    ) -> BackfillExecutionReceipt:
        """Dry-run by default; execute only one exact separately authorized run."""

        if not execute:
            return BackfillExecutionReceipt(
                run_id=run.manifest.run_id,
                authorization_id=None,
                dry_run=True,
                write_executed=False,
                event_states=tuple(
                    (plan.spec.event_id, "PLANNED") for plan in run.event_plans
                ),
                transaction_committed=False,
                post_commit_verified=False,
                replay_converged=False,
            )
        if authorization is None:
            raise ProductionBackfillPackageError(
                "Execution requires a separate exact production authorization."
            )
        authorization.validate_run(run.manifest)
        database = self._client.get_database(self._database_name)
        event_states: list[tuple[int, str]] = []
        try:
            from pymongo import ReadPreference
            from pymongo.read_concern import ReadConcern
            from pymongo.write_concern import WriteConcern

            with self._client.start_session() as session:
                with session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern("majority"),
                    read_preference=ReadPreference.PRIMARY,
                ):
                    preflight_states = []
                    for plan in run.event_plans:
                        state, _ = self._state_for_plan(database, plan, session)
                        event_states.append((plan.spec.event_id, state))
                        preflight_states.append((plan, state))
                    for plan, state in preflight_states:
                        if state == "PREIMAGE_MATCH":
                            self._apply_event(database, plan, session)
                        verified_state, _ = self._state_for_plan(
                            database,
                            plan,
                            session,
                        )
                        if verified_state != "ALREADY_CONVERGED":
                            raise ProductionBackfillExecutionError(
                                "In-transaction state did not converge."
                            )
        except (
            ProductionBackfillDriftError,
            ProductionBackfillExecutionError,
        ):
            raise
        except Exception as exc:
            raise ProductionBackfillExecutionError(
                f"Production transaction failed ({type(exc).__name__})."
            ) from exc

        post_cards = tuple(
            _fetch_card_with_session(database, plan.spec, None)
            for plan in run.event_plans
        )
        for plan, card in zip(run.event_plans, post_cards):
            if _hash(_event_state_payload(card)) != _hash(plan.desired_state_payload):
                raise ProductionBackfillExecutionError(
                    f"Post-commit state verification failed for event {plan.spec.event_id}."
                )
        replay_converged = self._verify_replay(post_cards, run)
        if not replay_converged:
            raise ProductionBackfillExecutionError(
                "Post-commit immediate replay did not converge."
            )
        return BackfillExecutionReceipt(
            run_id=run.manifest.run_id,
            authorization_id=authorization.authorization_id,
            dry_run=False,
            write_executed=any(state == "PREIMAGE_MATCH" for _, state in event_states),
            event_states=tuple(event_states),
            transaction_committed=True,
            post_commit_verified=True,
            replay_converged=True,
        )


def run_package_read(
    uri: str,
    database_name: str,
    attestation: AdminTitleAttestation,
    *,
    created_at: str,
) -> PreparedBackfillRun:
    """Perform bounded majority reads and prepare the exact run in memory."""

    from pymongo import MongoClient, ReadPreference
    from pymongo.read_concern import ReadConcern

    client = MongoClient(
        uri,
        appname="ufc-picks-carddata-backfill-package",
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
        cards = fetch_backfill_cards(database, TARGET_SPECS)
        return prepare_backfill_run(cards, attestation, created_at=created_at)
    finally:
        client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare the exact SCR-012B CardData package and encrypted preimages. "
            "This command has no production-write mode."
        )
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="Ignored local dotenv containing the read-only production URI.",
    )
    parser.add_argument(
        "--admin-title-attestation",
        type=Path,
        required=True,
        help="Validated D-DATA-012 dry-run-only Admin TITLE artifact.",
    )
    parser.add_argument(
        "--preimage-key-file",
        type=Path,
        required=True,
        help="Fernet key path outside the workspace; never rendered.",
    )
    parser.add_argument(
        "--create-key-file",
        action="store_true",
        help="Create the explicitly named key file exclusively; never overwrite.",
    )
    parser.add_argument(
        "--preimage-output",
        type=Path,
        required=True,
        help="Encrypted preimage archive path outside the workspace.",
    )
    parser.add_argument(
        "--manifest-output",
        type=Path,
        required=True,
        help="Exclusive sanitized manifest output path.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
    )
    parser.add_argument(
        "--created-at",
        default=None,
        help="Explicit UTC package time; default is the current UTC time.",
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
        coordinator_root = Path(__file__).resolve().parents[2]
        protected_roots = (coordinator_root,)
        key_path = require_private_path(args.preimage_key_file, protected_roots)
        preimage_path = require_private_path(args.preimage_output, protected_roots)
        manifest_path = args.manifest_output.expanduser().resolve()
        all_paths = (
            args.env_file.expanduser().resolve(),
            args.admin_title_attestation.expanduser().resolve(),
            key_path,
            preimage_path,
            manifest_path,
        )
        if len(set(all_paths)) != len(all_paths):
            raise ProductionBackfillPackageError(
                "Environment, attestation, key, preimage and manifest paths must differ."
            )
        if preimage_path.exists() or manifest_path.exists():
            raise ProductionBackfillPackageError(
                "Refusing to overwrite an existing package artifact."
            )
        if not manifest_path.parent.is_dir():
            raise ProductionBackfillPackageError(
                "Manifest output parent directory must already exist."
            )
        if args.create_key_file:
            if key_path.exists():
                raise ProductionBackfillPackageError(
                    "Refusing to overwrite an existing preimage key."
                )
        elif not key_path.is_file():
            raise ProductionBackfillPackageError(
                "Preimage key does not exist; use --create-key-file once."
            )

        created_at = args.created_at or datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        attestation = load_admin_title_attestation(args.admin_title_attestation)
        uri, database_name = load_mongo_settings(args.env_file)
        run = run_package_read(
            uri,
            database_name,
            attestation,
            created_at=created_at,
        )
        key = (
            create_preimage_key_file(key_path, protected_roots)
            if args.create_key_file
            else load_preimage_key_file(key_path, protected_roots)
        )
        run = write_encrypted_preimages(
            run,
            preimage_path,
            key,
            protected_roots,
        )
        rendered = (
            render_manifest_json(run.manifest)
            if args.format == "json"
            else render_manifest_markdown(run.manifest)
        )
        _write_exclusive(manifest_path, rendered.encode("utf-8"))
        stdout.write(f"prepared_run_id={run.manifest.run_id}\n")
        stdout.write("writes_executed=0\n")
        stdout.write("production_write_authorized=false\n")
        return EXIT_PREPARED
    except (
        AdminTitleAttestationError,
        BackfillDryRunConfigurationError,
        ProductionAuditConfigurationError,
        ProductionBackfillPackageError,
    ) as exc:
        stderr.write(f"error: {exc}\n")
        return EXIT_CONFIGURATION_ERROR
    except Exception as exc:
        stderr.write(
            "error: production package preparation failed "
            f"({type(exc).__name__}); no production write occurred.\n"
        )
        return EXIT_CONFIGURATION_ERROR


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUTHORIZATION_SCHEMA_VERSION",
    "BackfillExecutionReceipt",
    "BackfillRunManifest",
    "DocumentWritePlan",
    "EventBackfillPlan",
    "EventPackageManifest",
    "MongoCardDataBackfillAdapter",
    "PACKAGE_SCHEMA_VERSION",
    "PREIMAGE_ARCHIVE_VERSION",
    "PreparedBackfillRun",
    "ProductionBackfillDriftError",
    "ProductionBackfillExecutionError",
    "ProductionBackfillPackageError",
    "ProductionWriteAuthorization",
    "TARGET_COLLECTIONS",
    "TARGET_EVENT_IDS",
    "create_preimage_key_file",
    "decrypt_preimage_archive",
    "encrypt_preimage_archive",
    "load_preimage_key_file",
    "load_production_write_authorization",
    "main",
    "parse_production_write_authorization",
    "prepare_backfill_run",
    "render_manifest_json",
    "render_manifest_markdown",
    "require_private_path",
    "run_package_read",
    "write_encrypted_preimages",
]
