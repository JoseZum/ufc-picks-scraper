"""Reaplica las decisiones vigentes de Admin en cada reconciliación.

Un `admin_override` gana a cualquier señal de ESPN, pero solo mientras esté
presente en el set de observaciones, y el normalizador reconstruye todo desde
ahí. Por eso una escritura suelta de Admin no dura: la siguiente pasada de ESPN
no menciona el campo y la decisión se revierte.

Aquí se reproducen en cada pasada los comandos guardados en
`admin_card_commands`, y eso es lo que hace cierto con el tiempo que a Admin no
se le pisa. Se añaden después de las observaciones de origen para que, ante un
empate, el normalizador vea a Admin en último lugar.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

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

    A command that no longer makes sense, it names a bout the card dropped, or
    carries values the contract rejects, is skipped and reported rather than
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
    db: Any | None = None,
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
