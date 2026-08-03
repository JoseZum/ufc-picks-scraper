"""B-010: a Tapology or ESPN run can never restate a title Admin decided.

D-DATA-010 makes Admin the sole authority for the title fields, with **both**
``true`` and ``false`` durable. The `false` half is the one that used to break:
a removed title designation looks identical to a scraper default, so the next
run put the belt back.

These tests exercise the guard the legacy writers now apply, including the
`false` case and the untouched-card case, so the protection cannot silently
regress into "only protects titles that are set".
"""

import pytest

from tapology_scraper.canonical_card_writer import (
    ADMIN_OWNED_BOUT_FIELDS,
    admin_owned_fields,
    strip_admin_owned,
)


def admin_decided(**fields) -> dict:
    """A persisted bout carrying Admin's evidence for the given fields."""
    return {
        "id": 1,
        "card_data_v1": {
            "evidence": {
                field: {
                    "source_kind": "admin_override",
                    "value": value,
                    "actor_id": "admin-1",
                }
                for field, value in fields.items()
            }
        },
    }


SCRAPED = {
    "id": 1,
    "weight_class": "Lightweight",
    "status": "scheduled",
    "is_title_fight": True,
    "is_bmf_title_fight": False,
}


def test_a_bout_admin_never_touched_is_written_in_full():
    """New cards must ingest exactly as before, or this guard breaks the scraper."""
    assert strip_admin_owned(SCRAPED, None) == SCRAPED
    assert strip_admin_owned(SCRAPED, {"id": 1}) == SCRAPED
    assert admin_owned_fields(None) == set()


def test_an_admin_title_is_not_overwritten_by_a_scraper_run():
    bout = admin_decided(is_title_fight=True)

    writable = strip_admin_owned(SCRAPED, bout)

    assert "is_title_fight" not in writable
    assert writable["weight_class"] == "Lightweight", "advisory fields still flow"


def test_a_removed_title_survives_a_scraper_run_that_claims_otherwise():
    """The regression that motivated B-010: `false` is a decision, not a default."""
    bout = admin_decided(is_title_fight=False)

    writable = strip_admin_owned({"is_title_fight": True}, bout)

    assert writable == {}, "Tapology may not put the belt back"


def test_each_admin_field_is_protected_independently():
    bout = admin_decided(is_bmf_title_fight=True)

    writable = strip_admin_owned(SCRAPED, bout)

    assert "is_bmf_title_fight" not in writable
    assert writable["is_title_fight"] is True, "only the claimed field is held back"


def test_every_admin_owned_field_is_actually_enforced():
    """Adding a field to the contract without enforcing it must fail here."""
    bout = admin_decided(**dict.fromkeys(ADMIN_OWNED_BOUT_FIELDS, True))
    update = dict.fromkeys(ADMIN_OWNED_BOUT_FIELDS, "scraped")

    assert strip_admin_owned(update, bout) == {}


def test_evidence_from_a_lower_authority_grants_no_protection():
    """Only `admin_override` counts; an ESPN signal stays advisory (D-DATA-002)."""
    bout = {
        "id": 1,
        "card_data_v1": {
            "evidence": {
                "is_title_fight": {"source_kind": "espn_detail", "value": True}
            }
        },
    }

    assert admin_owned_fields(bout) == set()
    assert strip_admin_owned(SCRAPED, bout) == SCRAPED


def test_a_malformed_sidecar_does_not_crash_ingestion():
    for broken in (
        {"card_data_v1": None},
        {"card_data_v1": {"evidence": None}},
        {"card_data_v1": {"evidence": {"is_title_fight": "not-a-mapping"}}},
        {"card_data_v1": "not-a-mapping"},
    ):
        assert admin_owned_fields(broken) == set()
        assert strip_admin_owned(SCRAPED, broken) == SCRAPED


# ---------------------------------------------------------------------------
# The protection has to survive a full reconciler pass, not just a legacy $set
# ---------------------------------------------------------------------------

import copy  # noqa: E402

from tapology_scraper.canonical_card_writer import (  # noqa: E402
    InMemoryCanonicalCardStore,
    submit_card_observations,
)
from tapology_scraper.card_observation_sources import (  # noqa: E402
    build_espn_card_observations,
)

import json  # noqa: E402
from pathlib import Path  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "card_data_v1"
EVENT_ID = 880101


def _espn_payload() -> dict:
    raw = json.loads(
        (FIXTURES / "numbered_three_sections.json").read_text(encoding="utf-8")
    )
    return copy.deepcopy(raw["events"][0])


def _pass(store, observed_at: str, db=None):
    """One reconciliation pass, exactly as the spider performs it."""
    from tapology_scraper.admin_command_replay import with_admin_overrides

    state = store.load_card(EVENT_ID)
    batch = build_espn_card_observations(
        _espn_payload(), state, observed_at=observed_at
    )
    assert not batch.blocked, [item.code for item in batch.findings]
    observations, _ = with_admin_overrides(batch.observations, state, db)
    return submit_card_observations(store, EVENT_ID, observations, dry_run=False)


class _FakeCommands:
    """The `admin_card_commands` collection the backend writes to."""

    def __init__(self, documents):
        self.documents = list(documents)

    def find(self, query):
        event_id = query.get("event_id")
        matched = [d for d in self.documents if d["event_id"] == event_id]
        return _FakeCursor(matched)


class _FakeCursor:
    def __init__(self, documents):
        self.documents = documents

    def sort(self, key, direction=1):
        self.documents = sorted(self.documents, key=lambda d: d.get(key, ""))
        return self

    def __iter__(self):
        return iter(self.documents)


class _FakeDb:
    def __init__(self, commands):
        self._commands = _FakeCommands(commands)

    def __getitem__(self, name):
        assert name == "admin_card_commands"
        return self._commands


def _title_command(bout_id: int, *, is_title: bool) -> dict:
    return {
        "command_id": f"admincmd-{bout_id}",
        "kind": "title",
        "event_id": EVENT_ID,
        "bout_id": bout_id,
        "observed_at": "2026-08-01T12:30:00Z",
        "reason": "Admin removed the title designation",
        "values": {"is_title_fight": is_title},
    }


def test_a_replayed_admin_command_survives_repeated_espn_passes():
    """B-010 closed on the canonical route.

    The decision lasts because the command is replayed on every pass, not
    because a one-off evidence stamp happened to survive.
    """
    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)

    target_id = sorted(store.bouts)[0]
    db = _FakeDb([_title_command(target_id, is_title=False)])

    # Three further passes: a one-off stamp reverts on the first of these.
    for at in ("2026-08-01T13:00:00Z", "2026-08-01T14:00:00Z", "2026-08-01T15:00:00Z"):
        _pass(store, at, db=db)
        assert store.bouts[target_id]["is_title_fight"] is False, (
            f"an ESPN pass at {at} overwrote Admin's title removal"
        )

    sidecar = store.bouts[target_id]["card_data_v1"]
    assert sidecar["is_title_fight"] is False
    assert sidecar["evidence"]["is_title_fight"]["source_kind"] == "admin_override"


def test_a_replayed_admin_command_can_also_assert_a_title():
    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)
    target_id = sorted(store.bouts)[-1]
    db = _FakeDb([_title_command(target_id, is_title=True)])

    _pass(store, "2026-08-01T13:00:00Z", db=db)

    assert store.bouts[target_id]["is_title_fight"] is True


def test_a_command_naming_a_bout_off_the_card_is_skipped_not_fatal():
    from tapology_scraper.admin_command_replay import with_admin_overrides

    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)
    db = _FakeDb([_title_command(999999, is_title=True)])

    observations, skipped = with_admin_overrides((), store.load_card(EVENT_ID), db)

    assert observations == []
    assert len(skipped) == 1 and "not on card" in skipped[0]


# ---------------------------------------------------------------------------
# B-011: the other Admin decisions must be just as durable as the title one
# ---------------------------------------------------------------------------


def _command(bout_id: int, kind: str, values: dict, at="2026-08-01T12:30:00Z") -> dict:
    return {
        "command_id": f"admincmd-{kind}-{bout_id}",
        "kind": kind,
        "event_id": EVENT_ID,
        "bout_id": bout_id,
        "observed_at": at,
        "reason": f"Admin {kind} decision",
        "values": values,
    }


def test_an_admin_cancellation_is_not_revived_by_later_espn_passes():
    """ESPN keeps listing the bout, so without a command the status reverts."""
    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)
    target_id = sorted(store.bouts)[-1]
    db = _FakeDb([_command(target_id, "bout_lifecycle", {"status": "cancelled"})])

    for at in ("2026-08-01T13:00:00Z", "2026-08-01T14:00:00Z"):
        _pass(store, at, db=db)
        sidecar = store.bouts[target_id]["card_data_v1"]
        assert sidecar["status"] == "cancelled", f"ESPN revived the bout at {at}"


def test_a_structure_override_is_rejected_rather_than_silently_applied():
    """Why `bout_structure` is deliberately NOT wired from the backend yet.

    ESPN emits `card_section` as a fact, not an advisory signal, so an Admin
    override contradicts it on every pass. The plan comes back unsafe with
    quarantines and the replay never converges — which is the boundary refusing
    to leave two authorities disagreeing, and is the correct behaviour. It is
    also why enabling this channel needs a convergence decision first.
    """
    from tapology_scraper.canonical_card_writer import CanonicalCardWriteError

    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)

    target_id = 991200003
    slot_key = next(
        key for key, slot in store.slots.items() if slot.get("bout_id") == target_id
    )
    before = store.slots[slot_key]["card_section"]
    db = _FakeDb([
        _command(target_id, "bout_structure", {"card_section": "early_prelim"})
    ])

    with pytest.raises(CanonicalCardWriteError) as error:
        _pass(store, "2026-08-01T13:00:00Z", db=db)

    assert "converge" in str(error.value)
    assert before == "prelim", "the fixture this documents must not drift"


def test_a_withdrawn_command_stops_being_replayed():
    """Withdrawal removes the override from the observation set.

    It does not undo an effect the boundary has already made terminal — a
    cancelled bout is never revived by a later pass, which is a separate
    invariant the writer holds on its own.
    """
    from tapology_scraper.admin_command_replay import with_admin_overrides

    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)
    target_id = sorted(store.bouts)[-1]
    db = _FakeDb([_command(target_id, "bout_lifecycle", {"status": "cancelled"})])

    with_command, _ = with_admin_overrides((), store.load_card(EVENT_ID), db)
    db._commands.documents.clear()
    without_command, _ = with_admin_overrides((), store.load_card(EVENT_ID), db)

    assert len(with_command) == 1
    assert without_command == [], "a withdrawn command must stop being injected"


def test_a_malformed_command_is_skipped_without_stopping_the_card():
    """One bad override must not block a whole card from reconciling."""
    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)
    target_id = sorted(store.bouts)[-1]

    from tapology_scraper.admin_command_replay import with_admin_overrides

    db = _FakeDb([
        _command(target_id, "title", {}),  # no explicit true/false: rejected
        _command(target_id, "bout_lifecycle", {"status": "cancelled"}),
    ])
    observations, skipped = with_admin_overrides((), store.load_card(EVENT_ID), db)

    assert len(observations) == 1, "the valid command still applies"
    assert len(skipped) == 1 and "title" in skipped[0]


def test_an_admin_result_stays_authoritative_across_repeated_passes():
    """The result channel converges, unlike structure — so it IS wired."""
    store = InMemoryCanonicalCardStore(
        events=[{"id": EVENT_ID, "name": "Seed event", "source": "tapology"}]
    )
    _pass(store, "2026-08-01T12:00:00Z", db=None)

    target_id = 991200003
    winner = store.bouts[target_id]["card_data_v1"]["fighters"][0]["fighter_id"]
    db = _FakeDb([
        _command(
            target_id,
            "result",
            {
                "outcome": "red_win",
                "winner_fighter_id": winner,
                "method_family": "ko_tko",
                "ending_round": 2,
            },
        )
    ])

    for at in ("2026-08-01T13:00:00Z", "2026-08-01T14:00:00Z", "2026-08-01T15:00:00Z"):
        _, receipt = _pass(store, at, db=db)
        result = store.bouts[target_id]["card_data_v1"].get("result") or {}
        assert result.get("outcome") == "red_win", f"result lost at {at}"
        assert result.get("winner_fighter_id") == winner
        assert receipt.verified_converged, f"replay did not converge at {at}"
