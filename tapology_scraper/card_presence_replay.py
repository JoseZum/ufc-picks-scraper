"""The durable channel from the late-change policy to the canonical writer.

SCR-010 shipped `apply_card_change_policy` as a pure layer: it decides, with
versioned evidence, when a bout that vanished from ESPN has been gone long
enough to be removed for real.  Nothing in production ever called it, so the
`BOUT_ABSENT_FROM_ESPN_PAYLOAD` warning kept pointing at a policy that never
ran, and a bout whose matchup changed stayed on the card forever.

Two things were missing, and both live here:

1. The policy is replay-safe *because* it accumulates evidence across runs —
   three complete ESPN-detail payloads, at least 30 minutes apart.  That
   evidence has to survive between crawls, and nothing persisted it.
2. The policy only normalises its removal observations in memory.  Whoever
   persists has to hand them to the canonical writer, or the confirmed removal
   is decided and then thrown away.

This module is deliberately shaped like `admin_command_replay`: load standing
state, fold it into the observation list, hand back what the caller must
persist.  It never writes canonical collections itself.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from tapology_scraper.card_change_policy import (
    BoutPresenceState,
    CardChangePolicyInputError,
    CardChangePolicyResult,
    CardCoverageObservation,
    apply_card_change_policy,
)
from tapology_scraper.canonical_card_writer import (
    CanonicalCardState,
    rebuild_previous_snapshot,
)

COLLECTION = "card_presence_states"


class CardPresenceReplayError(RuntimeError):
    """Raised when presence evidence cannot be loaded or persisted."""


def _canonical_digest(value: Any) -> str:
    import json

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def restore_source_run(
    snapshot: Optional[Mapping[str, Any]],
) -> Optional[Mapping[str, Any]]:
    """Give a persisted snapshot back the `source_run` the policy demands.

    The writer strips `source_run` on purpose -- it describes the run that
    produced a snapshot, not the card (`canonical_card_writer` line ~461). The
    policy, however, validates `previous_snapshot` against the full contract,
    which requires it. Without this bridge every real call degrades to "policy
    skipped", which is why the policy could only ever run on snapshots it had
    just built itself.

    The rebuilt envelope is descriptive only: no fact is invented, because the
    policy reads bouts/slots/eligibility and never the run descriptor.
    """
    if not isinstance(snapshot, Mapping):
        return snapshot
    if isinstance(snapshot.get("source_run"), Mapping):
        return snapshot
    restored = dict(snapshot)
    revision = restored.get("snapshot_revision")
    restored["source_run"] = {
        "run_id": f"rebuilt_{restored.get('snapshot_id') or 'unknown'}",
        "observed_at": restored.get("generated_at"),
        "sources": [],
        "previous_snapshot_revision": (
            revision - 1 if isinstance(revision, int) and revision > 1 else None
        ),
    }
    return restored


def load_presence_states(db: Optional[Any], event_id: int) -> list[dict]:
    """Every standing presence record for this card, oldest bout first."""
    if db is None:
        return []
    rows = db[COLLECTION].find({"event_id": int(event_id)})
    states = []
    for row in rows:
        state = {k: v for k, v in row.items() if k != "_id"}
        states.append(state)
    return sorted(states, key=lambda item: item.get("bout_id") or 0)


def store_presence_states(
    db: Optional[Any],
    event_id: int,
    states: Sequence[BoutPresenceState | Mapping[str, Any]],
) -> int:
    """Persist this run's evidence, dropping records the policy resolved.

    The policy returns the complete standing set for the event, so anything
    absent from it is a bout that came back or was confirmed removed; either
    way its evidence must go, or a later run would replay a stale miss count.
    """
    if db is None:
        return 0
    documents = [
        item.as_dict() if isinstance(item, BoutPresenceState) else dict(item)
        for item in states
    ]
    keep = {int(item["bout_id"]) for item in documents}
    db[COLLECTION].delete_many(
        {"event_id": int(event_id), "bout_id": {"$nin": sorted(keep)}}
    )
    for document in documents:
        db[COLLECTION].update_one(
            {"event_id": int(event_id), "bout_id": int(document["bout_id"])},
            {"$set": document},
            upsert=True,
        )
    return len(documents)


def build_coverage(
    event_id: int,
    present_bout_ids: Sequence[int],
    *,
    observed_at: str,
    source_event_id: str,
    payload: Any,
    coverage_kind: str,
    source_kind: str = "espn_detail",
) -> CardCoverageObservation:
    """Declare which canonical bouts one ESPN payload actually contained."""
    payload_hash = f"sha256:{_canonical_digest(payload)}"
    coverage_id = (
        f"espn-coverage:{event_id}:{observed_at}:{payload_hash.split(':', 1)[1][:16]}"
    )
    return CardCoverageObservation(
        coverage_id=coverage_id,
        source_kind=source_kind,
        observed_at=observed_at,
        event_id=int(event_id),
        source_event_id=str(source_event_id),
        source_ref=f"espn:scoreboard:{source_event_id}",
        payload_hash=payload_hash,
        coverage_kind=coverage_kind,
        present_bout_ids=tuple(int(item) for item in present_bout_ids),
    )


def with_change_policy(
    source_observations: Sequence[Any],
    state: CanonicalCardState,
    coverage: Optional[CardCoverageObservation | Mapping[str, Any]],
    db: Optional[Any] = None,
) -> tuple[list[Any], Optional[CardChangePolicyResult], list[str]]:
    """Source observations plus any removal the absence policy confirmed.

    Returns the observations to submit, the policy result (so the caller can
    persist evidence only after the write lands) and operator-facing notes.
    Any policy input error degrades to "carry on without the policy": a bad
    coverage declaration must never stop a card from ingesting.
    """
    observations = list(source_observations)
    if coverage is None:
        return observations, None, []

    notes: list[str] = []
    try:
        # Accept the same shapes the policy itself accepts, so a caller that
        # already has a plain mapping does not have to know the dataclass.
        if isinstance(coverage, Mapping):
            coverage = CardCoverageObservation.from_mapping(coverage)
        coverage.validate()
    except CardChangePolicyInputError as error:
        return observations, None, [f"coverage rejected: {error}"]

    previous_states = load_presence_states(db, state.event_id)
    try:
        result = apply_card_change_policy(
            source_observations,
            restore_source_run(rebuild_previous_snapshot(state)),
            coverage=coverage,
            previous_presence_states=previous_states,
        )
    except CardChangePolicyInputError as error:
        return observations, None, [f"change policy skipped: {error}"]

    for finding in result.findings:
        notes.append(
            f"{finding.code} bout={finding.bout_id} action={finding.action}: "
            f"{finding.message}"
        )
    return (
        [*observations, *result.synthetic_observations],
        result,
        notes,
    )


__all__ = [
    "COLLECTION",
    "CardPresenceReplayError",
    "build_coverage",
    "load_presence_states",
    "store_presence_states",
    "with_change_policy",
]
