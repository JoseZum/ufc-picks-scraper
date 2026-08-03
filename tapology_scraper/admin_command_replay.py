"""Replay standing Admin decisions on every canonical reconciliation.

`build_admin_card_observations` emits `admin_override` facts at rank 500, so an
Admin decision beats any ESPN or Tapology signal for the same field. That only
holds while the fact is *present* in the observation set, and the normalizer
rebuilds the snapshot from observations alone.

So a one-off Admin write does not last: the next ESPN pass simply does not
mention the field, evidence is rebuilt without it, and the decision reverts.
The commands the backend persists in `admin_card_commands` are replayed here on
every pass, which is what makes "Admin can never be overwritten" true over time
rather than only immediately after the click.

Admin observations are appended *after* the source observations so that when two
sources resolve the same field, Admin is the one the normalizer sees last.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from tapology_scraper.canonical_card_writer import CanonicalCardState
from tapology_scraper.card_observation_sources import (
    AdminCardCommand,
    ObservationSourceError,
    build_admin_card_observations,
)

COLLECTION = "admin_card_commands"


class AdminCommandReplayError(RuntimeError):
    pass


def load_admin_commands(db: Any, event_id: int) -> list[AdminCardCommand]:
    """Read this card's standing Admin decisions, oldest first."""
    if db is None:
        return []
    documents = db[COLLECTION].find({"event_id": int(event_id)}).sort("observed_at", 1)
    commands: list[AdminCardCommand] = []
    for document in documents:
        try:
            commands.append(
                AdminCardCommand(
                    command_id=str(document["command_id"]),
                    kind=str(document["kind"]),
                    event_id=int(document["event_id"]),
                    observed_at=str(document["observed_at"]),
                    reason=str(document.get("reason") or "admin decision"),
                    bout_id=(
                        int(document["bout_id"]) if document.get("bout_id") else None
                    ),
                    values=dict(document.get("values") or {}),
                )
            )
        except (KeyError, TypeError, ValueError) as error:
            raise AdminCommandReplayError(
                f"Malformed Admin command {document.get('command_id')!r}: {error}"
            ) from error
    return commands


def admin_observations(
    commands: Sequence[AdminCardCommand],
    state: CanonicalCardState,
) -> tuple[list[Any], list[str]]:
    """Convert standing commands into observations.

    A command that no longer makes sense — it names a bout the card dropped, or
    carries values the contract rejects — is skipped and reported rather than
    failing the whole pass. One stale override must not stop a card from
    reconciling.
    """
    observations: list[Any] = []
    skipped: list[str] = []
    known_bouts = {
        bout.get("bout_id") or bout.get("id")
        for bout in (state.bouts or ())
        if isinstance(bout, Mapping)
    }
    for command in commands:
        if command.bout_id is not None and command.bout_id not in known_bouts:
            skipped.append(f"{command.command_id}: bout {command.bout_id} not on card")
            continue
        try:
            batch = build_admin_card_observations(command, state)
        except ObservationSourceError as error:
            skipped.append(f"{command.command_id}: {error}")
            continue
        if batch.blocked:
            skipped.append(
                f"{command.command_id}: blocked "
                f"({[finding.code for finding in batch.findings]})"
            )
            continue
        observations.extend(batch.observations)
    return observations, skipped


def with_admin_overrides(
    source_observations: Sequence[Any],
    state: CanonicalCardState,
    db: Optional[Any] = None,
) -> tuple[list[Any], list[str]]:
    """Source observations plus every standing Admin decision, Admin last."""
    commands = load_admin_commands(db, state.event_id)
    if not commands:
        return list(source_observations), []
    overrides, skipped = admin_observations(commands, state)
    return [*source_observations, *overrides], skipped


__all__ = [
    "COLLECTION",
    "AdminCommandReplayError",
    "admin_observations",
    "load_admin_commands",
    "with_admin_overrides",
]
