"""Validated Admin TITLE baseline input for CardData dry-runs.

The artifact handled here is an authority declaration, not a database command.
It expands a small explicit title-bout list into complete per-bout Admin values
for an observed card scope.  The module has no network/database dependency and
cannot authorize or execute a production write.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ATTESTATION_SCHEMA_VERSION = "admin-title-attestation/v1"
DRY_RUN_AUTHORIZED_USE = "CARD_DATA_BACKFILL_DRY_RUN_ONLY"
TITLE_TYPES = {"undisputed", "interim", "bmf", "other"}


class AdminTitleAttestationError(ValueError):
    """Raised when an attestation is incomplete, inconsistent or out of scope."""


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


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


@dataclass(frozen=True)
class AdminTitleAssignment:
    event_id: int
    bout_id: int
    title_type: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "bout_id": self.bout_id,
            "title_type": self.title_type,
        }


@dataclass(frozen=True)
class AdminTitleAttestation:
    attested_by: str
    attested_at: str
    decision_ref: str
    reason: str
    scope_event_ids: tuple[int, ...]
    title_bouts: tuple[AdminTitleAssignment, ...]
    all_other_bouts_non_title: bool
    authorized_use: str
    production_write_authorized: bool
    attestation_id: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "attested_by": self.attested_by,
            "attested_at": self.attested_at,
            "decision_ref": self.decision_ref,
            "reason": self.reason,
            "scope_event_ids": list(self.scope_event_ids),
            "title_bouts": [item.as_dict() for item in self.title_bouts],
            "all_other_bouts_non_title": self.all_other_bouts_non_title,
            "authorized_use": self.authorized_use,
            "production_write_authorized": self.production_write_authorized,
        }

    def sanitized_summary(self) -> dict[str, Any]:
        return {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "attestation_id": self.attestation_id,
            "decision_ref": self.decision_ref,
            "scope_event_ids": list(self.scope_event_ids),
            "explicit_title_bout_count": len(self.title_bouts),
            "all_other_bouts_non_title": self.all_other_bouts_non_title,
            "authorized_use": self.authorized_use,
            "production_write_authorized": self.production_write_authorized,
        }

    def values_for_card(
        self,
        event_id: int,
        observed_bout_ids: Sequence[int],
    ) -> dict[int, dict[str, Any]]:
        """Expand the attested baseline over every observed bout in one card."""

        if event_id not in self.scope_event_ids:
            return {}
        if not observed_bout_ids or any(
            not _positive_int(value) for value in observed_bout_ids
        ):
            raise AdminTitleAttestationError(
                f"Event {event_id} has an invalid or empty observed bout scope."
            )
        if len(set(observed_bout_ids)) != len(observed_bout_ids):
            raise AdminTitleAttestationError(
                f"Event {event_id} has duplicate observed bout IDs."
            )
        explicit = {
            item.bout_id: item for item in self.title_bouts if item.event_id == event_id
        }
        missing = sorted(set(explicit) - set(observed_bout_ids))
        if missing:
            raise AdminTitleAttestationError(
                f"Event {event_id} attests title bouts outside the observed card."
            )
        values = {}
        for bout_id in sorted(observed_bout_ids):
            assignment = explicit.get(bout_id)
            if assignment is None:
                values[bout_id] = {
                    "is_title_fight": False,
                    "is_bmf_title_fight": False,
                    "title_type": "none",
                }
            else:
                values[bout_id] = {
                    "is_title_fight": True,
                    "is_bmf_title_fight": assignment.title_type == "bmf",
                    "title_type": assignment.title_type,
                }
        return values

    def validate_observed_scope(
        self,
        cards: Mapping[int, Sequence[int]],
    ) -> None:
        missing_events = sorted(set(self.scope_event_ids) - set(cards))
        if missing_events:
            raise AdminTitleAttestationError(
                "The selected dry-run omits one or more attested events."
            )
        for event_id in self.scope_event_ids:
            self.values_for_card(event_id, cards[event_id])


def parse_admin_title_attestation(value: Any) -> AdminTitleAttestation:
    if not isinstance(value, Mapping):
        raise AdminTitleAttestationError("Admin TITLE attestation must be an object.")
    if value.get("schema_version") != ATTESTATION_SCHEMA_VERSION:
        raise AdminTitleAttestationError("Unsupported Admin TITLE attestation schema.")
    attested_by = value.get("attested_by")
    attested_at = _utc_timestamp(value.get("attested_at"))
    decision_ref = value.get("decision_ref")
    reason = value.get("reason")
    if not all(_nonempty(item) for item in (attested_by, decision_ref, reason)):
        raise AdminTitleAttestationError(
            "Attester, decision reference and reason are required."
        )
    if attested_at is None:
        raise AdminTitleAttestationError(
            "attested_at must be an explicit UTC timestamp."
        )

    raw_scope = _sequence(value.get("scope_event_ids"))
    if not raw_scope or any(not _positive_int(item) for item in raw_scope):
        raise AdminTitleAttestationError(
            "scope_event_ids must contain positive integers."
        )
    if len(set(raw_scope)) != len(raw_scope):
        raise AdminTitleAttestationError("scope_event_ids contains duplicates.")
    scope = tuple(sorted(raw_scope))

    if value.get("all_other_bouts_non_title") is not True:
        raise AdminTitleAttestationError(
            "A complete baseline must explicitly mark all other bouts non-title."
        )
    if value.get("authorized_use") != DRY_RUN_AUTHORIZED_USE:
        raise AdminTitleAttestationError(
            "Attestation is not restricted to the dry-run."
        )
    if value.get("production_write_authorized") is not False:
        raise AdminTitleAttestationError(
            "Admin TITLE attestation must not authorize a production write."
        )

    raw_title_bouts = value.get("title_bouts")
    if not isinstance(raw_title_bouts, Sequence) or isinstance(
        raw_title_bouts, (str, bytes, bytearray)
    ):
        raise AdminTitleAttestationError("title_bouts must be an array.")

    assignments = []
    seen = set()
    for raw in raw_title_bouts:
        if not isinstance(raw, Mapping):
            raise AdminTitleAttestationError(
                "Every title_bouts entry must be an object."
            )
        event_id = raw.get("event_id")
        bout_id = raw.get("bout_id")
        title_type = raw.get("title_type")
        if not _positive_int(event_id) or not _positive_int(bout_id):
            raise AdminTitleAttestationError(
                "Every title assignment requires positive event_id and bout_id."
            )
        if event_id not in scope:
            raise AdminTitleAttestationError(
                "A title assignment references an event outside the attested scope."
            )
        if title_type not in TITLE_TYPES:
            raise AdminTitleAttestationError(
                "Every explicit title assignment requires a canonical title_type."
            )
        identity = (event_id, bout_id)
        if identity in seen:
            raise AdminTitleAttestationError("Duplicate title-bout assignment.")
        seen.add(identity)
        assignments.append(AdminTitleAssignment(event_id, bout_id, title_type))
    assignments.sort(key=lambda item: (item.event_id, item.bout_id))

    provisional = AdminTitleAttestation(
        attested_by=attested_by.strip(),
        attested_at=attested_at,
        decision_ref=decision_ref.strip(),
        reason=reason.strip(),
        scope_event_ids=scope,
        title_bouts=tuple(assignments),
        all_other_bouts_non_title=True,
        authorized_use=DRY_RUN_AUTHORIZED_USE,
        production_write_authorized=False,
        attestation_id="",
    )
    attestation_id = _hash(provisional.canonical_payload())
    expected_id = value.get("attestation_id")
    if expected_id is not None and expected_id != attestation_id:
        raise AdminTitleAttestationError(
            "Admin TITLE attestation ID does not match payload."
        )
    return replace(provisional, attestation_id=attestation_id)


def load_admin_title_attestation(path: Path) -> AdminTitleAttestation:
    if not path.is_file():
        raise AdminTitleAttestationError("Admin TITLE attestation file does not exist.")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AdminTitleAttestationError(
            f"Admin TITLE attestation could not be read ({type(exc).__name__})."
        ) from exc
    return parse_admin_title_attestation(value)


__all__ = [
    "ATTESTATION_SCHEMA_VERSION",
    "DRY_RUN_AUTHORIZED_USE",
    "AdminTitleAssignment",
    "AdminTitleAttestation",
    "AdminTitleAttestationError",
    "load_admin_title_attestation",
    "parse_admin_title_attestation",
]
