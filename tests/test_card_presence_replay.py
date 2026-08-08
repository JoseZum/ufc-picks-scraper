"""Regressions for the wiring between the absence policy and the writer.

SCR-010 already proves the policy decides correctly; these cover the part that
was missing in production: evidence that survives between crawls, removal
observations that actually reach the canonical writer, and the refusals that
keep a bad payload from confirming a removal.
"""

from __future__ import annotations

import pytest

from tapology_scraper.canonical_card_writer import CanonicalCardState
from tapology_scraper.card_presence_replay import (
    COLLECTION,
    build_coverage,
    load_presence_states,
    store_presence_states,
    with_change_policy,
)


class FakeCollection:
    """Just enough Mongo for the two calls this module makes."""

    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]

    def find(self, query):
        event_id = query.get("event_id")
        return [dict(row) for row in self.rows if row.get("event_id") == event_id]

    def delete_many(self, query):
        keep = set(query.get("bout_id", {}).get("$nin", []))
        event_id = query.get("event_id")
        before = len(self.rows)
        self.rows = [
            row
            for row in self.rows
            if row.get("event_id") != event_id or row.get("bout_id") in keep
        ]
        return before - len(self.rows)

    def update_one(self, query, update, upsert=False):
        for row in self.rows:
            if all(row.get(k) == v for k, v in query.items()):
                row.update(update["$set"])
                return
        if upsert:
            self.rows.append(dict(update["$set"]))


class FakeDb:
    def __init__(self, rows=()):
        self.collections = {COLLECTION: FakeCollection(rows)}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


def _state(event_id=900001):
    return CanonicalCardState.build(event_id, {"id": event_id}, [], [])


def _presence_row(event_id, bout_id, misses=1):
    return {
        "event_id": event_id,
        "bout_id": bout_id,
        "disposition": "missing_pending",
        "consecutive_complete_misses": misses,
        "first_qualifying_missing_at": "2026-08-01T00:00:00Z",
        "last_missing_at": "2026-08-01T00:00:00Z",
        "last_coverage_id": "espn-coverage:1",
        "last_payload_hash": "sha256:abc",
        "source_kind": "espn_detail",
    }


def test_presence_states_round_trip_per_event():
    db = FakeDb([_presence_row(900001, 5), _presence_row(900002, 7)])
    loaded = load_presence_states(db, 900001)
    assert [item["bout_id"] for item in loaded] == [5]
    assert "_id" not in loaded[0]


def test_store_drops_states_the_policy_resolved():
    """A bout that came back must not keep its stale miss count."""
    db = FakeDb([_presence_row(900001, 5), _presence_row(900001, 6)])
    stored = store_presence_states(db, 900001, [_presence_row(900001, 5, misses=2)])
    assert stored == 1
    remaining = load_presence_states(db, 900001)
    assert [item["bout_id"] for item in remaining] == [5]
    assert remaining[0]["consecutive_complete_misses"] == 2


def test_store_is_scoped_to_its_event():
    db = FakeDb([_presence_row(900001, 5), _presence_row(900002, 7)])
    store_presence_states(db, 900001, [])
    assert [item["bout_id"] for item in load_presence_states(db, 900002)] == [7]


def test_load_without_db_is_empty():
    assert load_presence_states(None, 900001) == []
    assert store_presence_states(None, 900001, [_presence_row(900001, 5)]) == 0


def test_coverage_declares_present_bouts_and_is_deterministic():
    kwargs = dict(
        observed_at="2026-08-08T00:00:00Z",
        source_event_id="600060621",
        payload={"id": "600060621"},
        coverage_kind="complete",
    )
    first = build_coverage(900001, [3, 1, 2], **kwargs)
    second = build_coverage(900001, [3, 1, 2], **kwargs)
    first.validate()
    assert first.present_bout_ids == (3, 1, 2)
    assert first.coverage_id == second.coverage_id
    assert first.payload_hash == second.payload_hash


def test_different_payload_yields_different_coverage_identity():
    common = dict(
        observed_at="2026-08-08T00:00:00Z",
        source_event_id="600060621",
        coverage_kind="complete",
    )
    a = build_coverage(900001, [1], payload={"id": "a"}, **common)
    b = build_coverage(900001, [1], payload={"id": "b"}, **common)
    assert a.payload_hash != b.payload_hash


def test_without_coverage_observations_pass_through_untouched():
    observations = [{"observation_id": "x"}]
    out, policy, notes = with_change_policy(observations, _state(), None, FakeDb())
    assert out == observations
    assert policy is None and notes == []


def test_invalid_coverage_never_blocks_ingestion():
    """A bad coverage declaration degrades to 'no policy', not to a failed card."""
    broken = build_coverage(
        900001,
        [1],
        observed_at="not-a-timestamp",
        source_event_id="600060621",
        payload={"id": "x"},
        coverage_kind="complete",
    )
    observations = [{"observation_id": "x"}]
    out, policy, notes = with_change_policy(observations, _state(), broken, FakeDb())
    assert out == observations
    assert policy is None
    assert notes and "coverage rejected" in notes[0]


def test_bad_policy_input_degrades_instead_of_raising():
    coverage = build_coverage(
        900001,
        [1],
        observed_at="2026-08-08T00:00:00Z",
        source_event_id="600060621",
        payload={"id": "x"},
        coverage_kind="complete",
    )
    # presence state without a previous snapshot is an input error by contract
    db = FakeDb([_presence_row(900001, 5)])
    out, policy, notes = with_change_policy([], _state(), coverage, db)
    assert policy is None
    assert notes and "change policy skipped" in notes[0]
    assert out == []


def test_persisted_snapshot_is_accepted_by_the_policy():
    """The writer strips `source_run`; the policy requires it.

    Without the bridge every real call degrades to "policy skipped", which is
    exactly why the policy only ever ran on snapshots it had just built itself.
    """
    from tapology_scraper.card_data_contract import validate_card_data_v1
    from tapology_scraper.card_presence_replay import restore_source_run

    import test_card_change_policy as helpers

    persisted = dict(helpers.build_snapshot())
    persisted.pop("source_run")
    assert not validate_card_data_v1(persisted).is_valid

    restored = restore_source_run(persisted)
    assert validate_card_data_v1(restored).is_valid
    # An envelope that already has one is never rewritten.
    original = helpers.build_snapshot()
    assert restore_source_run(original) is original


def test_three_misses_reach_the_writer_as_a_cancelled_bout():
    """End to end: absence -> persisted evidence -> canonical write.

    This is the Miles Johns case. Each pass stores its own evidence, and only
    the third crosses the threshold and cancels the bout on the card.
    """
    from tapology_scraper.canonical_card_writer import (
        InMemoryCanonicalCardStore,
        submit_card_observations,
    )

    import test_card_change_policy as helpers

    event_id = helpers.EVENT_ID
    ghost = 402
    store = InMemoryCanonicalCardStore(
        events=[{"id": event_id, "card_data_snapshot_v1": helpers.build_snapshot()}]
    )
    db = FakeDb()
    seen = []

    runs = [
        ("2026-08-22T09:00:00Z", "a", (401, ghost, 403)),
        (helpers.COVERAGE_1, "first", (401, 403)),
        (helpers.COVERAGE_2, "second", (401, 403)),
        (helpers.COVERAGE_3, "third", (401, 403)),
    ]
    for stamp, suffix, present in runs:
        coverage = helpers.coverage(stamp, suffix, present=present, kind="complete")
        merged, policy, _ = with_change_policy(
            [helpers.refetch(stamp, suffix)], store.load_card(event_id), coverage, db
        )
        submit_card_observations(store, event_id, merged, dry_run=False)
        if policy is not None:
            store_presence_states(db, event_id, policy.presence_states)
        state = next(
            (s for s in load_presence_states(db, event_id) if s["bout_id"] == ghost),
            None,
        )
        seen.append(
            (
                state["consecutive_complete_misses"] if state else 0,
                tuple(o.entity_id for o in policy.synthetic_observations)
                if policy
                else (),
            )
        )

    assert [item[0] for item in seen] == [0, 1, 2, 3]
    # The removal observation is emitted once, only at the threshold.
    assert [item[1] for item in seen] == [(), (), (), (ghost,)]

    final = {b.get("id"): b.get("status") for b in store.load_card(event_id).bouts}
    assert final[ghost] == "cancelled"
    assert final[401] == "scheduled"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
