"""SCR-015/016 proofs for the continuous CardData write boundary.

Every test here drives :mod:`tapology_scraper.canonical_card_writer` with local
fixtures and in-memory doubles.  No test opens a connection, imports a driver
or touches production data.

The invariants proven, in the order the boundary must guarantee them:

* continuous ESPN observations reach storage only through the normalizer and
  the slot reconciler (:class:`TestObservationsTravelThroughTheBoundary`);
* a legacy writer cannot regain canonical authority
  (:class:`TestLegacyWritersCannotRegainAuthority`);
* Admin timing/lifecycle/title/result commands use the same boundary
  (:class:`TestAdminCommandsUseTheSameBoundary`);
* cancelled/postponed/replaced bouts are retained, never deleted
  (:class:`TestTerminalBoutsAreRetained`);
* replacement lineage and ``matchup_revision`` behave as contracted
  (:class:`TestReplacementLineage`);
* an immediate replay is a zero-diff — the B-008 proof
  (:class:`TestConvergence`);
* the legacy ``fighters``/``result`` compatibility fields still populate
  (:class:`TestLegacyCompatibilityProjection`).
"""

import copy
import dataclasses
import json
from pathlib import Path

import pytest

from tapology_scraper.canonical_card_writer import (
    BOUNDARY_VERSION,
    CANONICAL_BOUT_FIELDS,
    CANONICAL_EVENT_FIELDS,
    DOCUMENT_SIDECAR_FIELD,
    SNAPSHOT_SIDECAR_FIELD,
    CanonicalCardState,
    CanonicalCardStateError,
    CanonicalCardWriteError,
    InMemoryCanonicalCardStore,
    LegacyCardWriteViolation,
    MongoCanonicalCardStore,
    apply_canonical_card_write,
    assert_legacy_card_update_allowed,
    plan_canonical_card_write,
    rebuild_previous_snapshot,
    stabilize_snapshot_provenance,
    submit_card_observations,
)
from tapology_scraper.card_observation_sources import (
    AdminCardCommand,
    build_admin_card_observations,
    build_espn_card_observations,
)


FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "card_data_v1"
EVENT_ID = 880101
T1 = "2026-08-01T12:00:00Z"
T2 = "2026-08-01T13:00:00Z"
T3 = "2026-08-02T09:00:00Z"
T4 = "2026-08-02T10:00:00Z"


def espn_event(filename: str) -> dict:
    payload = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    return copy.deepcopy(payload["events"][0])


def new_store(**event_fields) -> InMemoryCanonicalCardStore:
    event = {"id": EVENT_ID, "name": "Seed event", "source": "tapology"}
    event.update(event_fields)
    return InMemoryCanonicalCardStore(events=[event])


def espn_pass(store, filename, observed_at=T1, mutate=None, dry_run=False):
    """Run one full continuous ESPN pass through the boundary."""

    payload = espn_event(filename)
    if mutate is not None:
        mutate(payload)
    batch = build_espn_card_observations(
        payload, store.load_card(EVENT_ID), observed_at=observed_at
    )
    assert not batch.blocked, [item.code for item in batch.findings]
    return submit_card_observations(
        store, EVENT_ID, batch.observations, dry_run=dry_run
    )


def admin_pass(store, *, kind, values, observed_at, bout_id=None, command_id="cmd-1"):
    command = AdminCardCommand(
        command_id=command_id,
        kind=kind,
        event_id=EVENT_ID,
        observed_at=observed_at,
        reason="Operator correction under test.",
        bout_id=bout_id,
        values=values,
    )
    batch = build_admin_card_observations(command, store.load_card(EVENT_ID))
    assert not batch.blocked, [item.code for item in batch.findings]
    return submit_card_observations(
        store, EVENT_ID, batch.observations, dry_run=False
    )


def observation(
    observation_id,
    entity_type,
    entity_id,
    values,
    *,
    source_kind="espn_summary",
    observed_at=T1,
    identity_basis="canonical_id",
    clear_fields=(),
    reason=None,
):
    return {
        "observation_id": observation_id,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": EVENT_ID,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"fixture:{observation_id}",
        "source_event_id": f"source-event-{EVENT_ID}",
        "values": values,
        "clear_fields": list(clear_fields),
        "identity_basis": identity_basis,
        "reason": reason,
        "payload_hash": f"sha256:{observation_id}",
    }


def fighter_pair(red, blue, *, swap=False):
    red_corner, blue_corner = ("blue", "red") if swap else ("red", "blue")
    return [
        {
            "fighter_id": red,
            "display_name": red.replace("_", " ").title(),
            "corner": red_corner,
            "source_ids": {"espn_athlete_id": red},
            "identity_confidence": "exact_source",
        },
        {
            "fighter_id": blue,
            "display_name": blue.replace("_", " ").title(),
            "corner": blue_corner,
            "source_ids": {"espn_athlete_id": blue},
            "identity_confidence": "exact_source",
        },
    ]


def synthetic_card(bouts, *, observed_at=T1):
    """Build a minimal but contract-complete observation set.

    ``bouts`` maps ``bout_id`` to the extra bout values under test, which keeps
    each scenario focused on the one fact it is proving.
    """

    result = [
        observation(
            "event",
            "event",
            EVENT_ID,
            {
                "source_ids": {"espn_event_id": "990000001"},
                "promotion": "UFC",
                "name": "UFC Fixture: Boundary vs. Replay",
                "official_event_date": "2026-09-01",
                "official_date_timezone": "America/New_York",
                "status": "scheduled",
            },
            identity_basis="source_alias",
            observed_at=observed_at,
        )
    ]
    for order, (bout_id, extra) in enumerate(sorted(bouts.items()), start=1):
        values = {
            "source_ids": {"espn_competition_id": str(bout_id)},
            "fighters": fighter_pair(f"ftr_{bout_id}_a", f"ftr_{bout_id}_b"),
            "weight_class": "Lightweight",
            "gender": "male",
            "scheduled_rounds": 3,
            "status": "scheduled",
        }
        values.update(copy.deepcopy(extra))
        result.append(
            observation(
                f"bout-{bout_id}",
                "bout",
                bout_id,
                values,
                observed_at=observed_at,
            )
        )
        if values.get("status") not in {"cancelled", "replaced"}:
            result.append(
                observation(
                    f"slot-{bout_id}",
                    "slot",
                    bout_id,
                    {
                        "is_current": True,
                        "card_section": "main",
                        "order_overall": order,
                        "scheduled_start_time_utc": "2026-09-02T02:00:00Z",
                        "automatic_lock_time_utc": "2026-09-02T02:00:00Z",
                    },
                    observed_at=observed_at,
                )
            )
    return result


class _RecordingCollection:
    """Mongo-shaped double that records writes and refuses every delete."""

    def __init__(self, name, documents, log):
        self.name = name
        self.documents = documents
        self.log = log

    def find_one(self, query, projection=None):
        return next(iter(self.find(query)), None)

    def find(self, query, projection=None):
        return [
            copy.deepcopy(item)
            for item in self.documents
            if all(item.get(key) == value for key, value in query.items())
        ]

    def update_one(self, query, update, upsert=False):
        self.log.append((self.name, "update_one", sorted(update)))
        target = next(
            (
                item
                for item in self.documents
                if all(item.get(key) == value for key, value in query.items())
            ),
            None,
        )
        if target is None:
            if not upsert:
                return
            target = dict(query)
            self.documents.append(target)
        for operator in ("$setOnInsert", "$set"):
            for key, value in (update.get(operator) or {}).items():
                cursor = target
                parts = str(key).split(".")
                for part in parts[:-1]:
                    cursor = cursor.setdefault(part, {})
                cursor[parts[-1]] = copy.deepcopy(value)

    def _forbidden(self, *args, **kwargs):
        raise AssertionError(
            f"The canonical boundary must never delete from {self.name}."
        )

    delete_one = _forbidden
    delete_many = _forbidden


class _RecordingDb:
    def __init__(self, events):
        self.log = []
        self.events = _RecordingCollection("events", list(events), self.log)
        self.bouts = _RecordingCollection("bouts", [], self.log)
        self.event_card_slots = _RecordingCollection(
            "event_card_slots", [], self.log
        )


class TestObservationsTravelThroughTheBoundary:
    def test_continuous_espn_pass_runs_normalizer_and_reconciler(self):
        store = new_store()

        plan, receipt = espn_pass(store, "numbered_three_sections.json")

        assert plan.boundary_version == BOUNDARY_VERSION
        # The normalizer produced the snapshot ...
        assert plan.snapshot["contract_version"] == "card-data/v1"
        assert plan.snapshot["normalizer_version"]
        assert plan.change_set is not None
        # ... and the reconciler produced the slot plan for the same snapshot.
        assert plan.slot_plan is not None
        assert plan.slot_plan.plan_id.startswith(f"slotplan_{EVENT_ID}_")
        assert not plan.blocked
        assert receipt.applied is True

    def test_canonical_structure_lands_in_slots_and_sidecars(self):
        store = new_store()

        espn_pass(store, "numbered_three_sections.json")

        slots = [item for item in store.slots.values() if item["event_id"] == EVENT_ID]
        assert len(slots) == 6
        assert {item["card_section"] for item in slots} == {
            "early_prelim",
            "prelim",
            "main",
        }
        # order_section restarts at 1 inside every section (WP-003).
        by_section = {}
        for slot in slots:
            by_section.setdefault(slot["card_section"], []).append(
                slot["order_section"]
            )
        for orders in by_section.values():
            assert sorted(orders) == list(range(1, len(orders) + 1))
        # Sidecars carry the canonical facts on both documents.
        assert store.events[EVENT_ID][SNAPSHOT_SIDECAR_FIELD]["snapshot_revision"] == 1
        for slot in slots:
            assert DOCUMENT_SIDECAR_FIELD in store.bouts[slot["bout_id"]]

    def test_a_blocked_plan_is_never_applied(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        before = copy.deepcopy(store.bouts)

        # Reusing a canonical bout ID for a different fighter set is refused.
        bout_id = min(store.bouts)
        plan, receipt = submit_card_observations(
            store,
            EVENT_ID,
            synthetic_card(
                {bout_id: {"fighters": fighter_pair("intruder_a", "intruder_b")}},
                observed_at=T2,
            ),
            dry_run=False,
        )

        assert plan.blocked is True
        assert receipt.applied is False
        assert "MATCHUP_ID_REUSE_FORBIDDEN" in {
            item.code for item in plan.quarantines
        }
        assert store.bouts == before

    def test_applying_a_blocked_plan_raises(self):
        store = new_store()
        plan = plan_canonical_card_write(store.load_card(EVENT_ID), [])

        assert plan.blocked is True
        with pytest.raises(CanonicalCardWriteError):
            apply_canonical_card_write(plan, store, dry_run=False)

    def test_a_dry_run_plans_without_writing(self):
        store = new_store()

        plan, receipt = espn_pass(
            store, "numbered_three_sections.json", dry_run=True
        )

        assert plan.operation_count > 0
        assert receipt.dry_run is True and receipt.applied is False
        assert store.bouts == {} and store.slots == {}


class TestLegacyWritersCannotRegainAuthority:
    @pytest.mark.parametrize(
        "collection,update",
        [
            ("events", {"$set": {"status": "completed"}}),
            ("events", {"$set": {"section_lock_times_utc.main": "2026-09-02"}}),
            ("bouts", {"$set": {"is_title_fight": True}}),
            ("bouts", {"$set": {"result": {"winner": "red"}}}),
            ("bouts", {"$setOnInsert": {"card_section": "main"}}),
            ("bouts", {"$unset": {"matchup_revision": ""}}),
            ("event_card_slots", {"$set": {"order_overall": 1}}),
            ("event_card_slots", {"$inc": {"structure_revision": 1}}),
        ],
    )
    def test_a_legacy_update_touching_a_canonical_field_is_refused(
        self, collection, update
    ):
        with pytest.raises(LegacyCardWriteViolation):
            assert_legacy_card_update_allowed(collection, update)

    @pytest.mark.parametrize(
        "collection,update",
        [
            ("events", {"$set": {"poster_image_url": "https://example.test/p.png"}}),
            ("events", {"$set": {"espn_event_id": "990000001"}}),
            ("bouts", {"$set": {"espn_competition_id": "991200001"}}),
            ("bouts", {"$set": {"fighters.red.espn_id": "992200001"}}),
            ("bouts", {"$set": {"espn_match_number": 3}}),
            ("picks", {"$set": {"points": 10}}),
        ],
    )
    def test_auxiliary_enrichment_is_still_allowed(self, collection, update):
        assert assert_legacy_card_update_allowed(collection, update) is None

    def test_title_result_and_structure_are_boundary_owned(self):
        for field in (
            "is_title_fight",
            "is_bmf_title_fight",
            "title_type",
            "result",
            "card_section",
            "matchup_revision",
        ):
            assert field in CANONICAL_BOUT_FIELDS
        for field in ("status", "official_event_date", "month_key", "timing_source"):
            assert field in CANONICAL_EVENT_FIELDS

    def test_an_espn_observation_never_outranks_a_persisted_admin_override(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        bout_id = min(store.bouts)
        admin_pass(
            store,
            kind="bout_structure",
            bout_id=bout_id,
            values={"card_section": "main", "order_overall": 99},
            observed_at=T2,
            command_id="structure-1",
        )
        slot = next(
            item for item in store.slots.values() if item["bout_id"] == bout_id
        )
        assert slot["card_section"] == "main"

        # A later ESPN pass observes the original early_prelim placement again.
        espn_pass(store, "numbered_three_sections.json", observed_at=T3)

        slot = next(
            item for item in store.slots.values() if item["bout_id"] == bout_id
        )
        assert slot["card_section"] == "main"
        assert slot["evidence"]["card_section"]["source_kind"] == "admin_override"


class TestAdminCommandsUseTheSameBoundary:
    def test_admin_event_timing_resolves_date_and_section_locks(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")

        plan, receipt = admin_pass(
            store,
            kind="event_timing",
            values={
                "official_event_date": "2026-08-16",
                # A lock must not fall after its section start (LOCK_AFTER_START).
                "section_lock_times_utc": {"main": "2026-08-16T00:30:00Z"},
            },
            observed_at=T2,
            command_id="timing-1",
        )

        assert receipt.applied and receipt.verified_converged
        event = store.events[EVENT_ID]
        assert event["official_event_date"] == "2026-08-16"
        assert event["month_key"] == "2026-08"
        assert event["timing_source"] == "admin"
        canonical = event[DOCUMENT_SIDECAR_FIELD]
        assert (
            canonical["evidence"]["official_event_date"]["source_kind"]
            == "admin_override"
        )

    def test_admin_event_lifecycle_is_canonical(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        assert store.events[EVENT_ID]["status"] == "scheduled"

        admin_pass(
            store,
            kind="event_lifecycle",
            values={"status": "postponed"},
            observed_at=T2,
            command_id="lifecycle-1",
        )

        assert store.events[EVENT_ID]["status"] == "postponed"

    def test_admin_bout_timing_lands_on_the_canonical_slot(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        bout_id = min(store.bouts)

        admin_pass(
            store,
            kind="bout_timing",
            bout_id=bout_id,
            values={"automatic_lock_time_utc": "2026-08-15T20:15:00Z"},
            observed_at=T2,
            command_id="bout-timing-1",
        )

        slot = next(
            item for item in store.slots.values() if item["bout_id"] == bout_id
        )
        assert slot["automatic_lock_time_utc"] == "2026-08-15T20:15:00Z"
        assert (
            slot["evidence"]["automatic_lock_time_utc"]["source_kind"]
            == "admin_override"
        )

    def test_admin_result_and_clear_result_are_versioned(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        bout_id = min(store.bouts)
        winner = store.bouts[bout_id][DOCUMENT_SIDECAR_FIELD]["fighters"][0][
            "fighter_id"
        ]

        admin_pass(
            store,
            kind="result",
            bout_id=bout_id,
            values={
                "outcome": "red" if winner.endswith("1") else "red",
                "winner_fighter_id": winner,
                "method_family": "submission",
                "method_detail": "Rear-naked choke",
                "ending_round": 2,
                "ending_time_seconds": 143,
            },
            observed_at=T2,
            command_id="result-1",
        )
        first_revision = store.bouts[bout_id]["result_revision"]
        assert store.bouts[bout_id]["result"]["method"] == "SUB"
        assert store.bouts[bout_id]["result"]["time"] == "2:23"

        admin_pass(
            store,
            kind="clear_result",
            bout_id=bout_id,
            values={},
            observed_at=T3,
            command_id="result-clear-1",
        )

        assert store.bouts[bout_id]["result"] is None
        assert store.bouts[bout_id]["result_revision"] >= first_revision

    def test_admin_is_the_sole_title_authority_for_true_and_false(self):
        store = new_store()
        # ESPN explicitly claims a title fight on the first competition.
        espn_pass(
            store,
            "numbered_three_sections.json",
            mutate=lambda payload: payload["competitions"][0].update(
                {"titleFight": True}
            ),
        )
        bout_id = int(espn_event("numbered_three_sections.json")["competitions"][0]["id"])

        # ESPN alone can never resolve TITLE.
        assert store.bouts[bout_id]["is_title_fight"] is None
        assert store.bouts[bout_id]["title_type"] == "unknown"

        # Admin true is durable against a later ESPN pass claiming false.
        admin_pass(
            store,
            kind="title",
            bout_id=bout_id,
            values={"is_title_fight": True, "title_type": "interim"},
            observed_at=T2,
            command_id="title-true",
        )
        assert store.bouts[bout_id]["is_title_fight"] is True
        assert store.bouts[bout_id]["title_type"] == "interim"

        espn_pass(
            store,
            "numbered_three_sections.json",
            observed_at=T3,
            mutate=lambda payload: payload["competitions"][0].update(
                {"titleFight": False}
            ),
        )
        assert store.bouts[bout_id]["is_title_fight"] is True
        assert store.bouts[bout_id]["title_type"] == "interim"

        # Admin false is equally durable against a later ESPN pass claiming true.
        admin_pass(
            store,
            kind="title",
            bout_id=bout_id,
            values={"is_title_fight": False},
            observed_at=T4,
            command_id="title-false",
        )
        assert store.bouts[bout_id]["is_title_fight"] is False
        assert store.bouts[bout_id]["is_bmf_title_fight"] is False
        assert store.bouts[bout_id]["title_type"] == "none"

        espn_pass(
            store,
            "numbered_three_sections.json",
            observed_at="2026-08-03T09:00:00Z",
            mutate=lambda payload: payload["competitions"][0].update(
                {"titleFight": True}
            ),
        )

        assert store.bouts[bout_id]["is_title_fight"] is False
        assert store.bouts[bout_id]["title_type"] == "none"
        evidence = store.bouts[bout_id][DOCUMENT_SIDECAR_FIELD]["evidence"]
        assert evidence["is_title_fight"]["source_kind"] == "admin_override"

    def test_repeated_espn_title_signals_do_not_grow_the_bout_document(self):
        store = new_store()
        bout_id = int(espn_event("numbered_three_sections.json")["competitions"][0]["id"])
        for observed_at in (T1, T2, T3, T4):
            espn_pass(
                store,
                "numbered_three_sections.json",
                observed_at=observed_at,
                mutate=lambda payload: payload["competitions"][0].update(
                    {"titleFight": True}
                ),
            )

        suggestions = store.bouts[bout_id][DOCUMENT_SIDECAR_FIELD]["evidence"][
            "title_suggestions"
        ]

        assert len(suggestions) == 1
        assert suggestions[0]["suggested"] == {"is_title_fight": True}


class TestPrecedenceOrder:
    """Admin override > explicit ESPN metadata > inference > quarantined."""

    def _card(self, source_kind, section, observed_at):
        """A two-bout card whose second bout's section is under contention.

        Bout 9100 anchors the main event so the card stays contract-valid
        (``MAIN_EVENT_MISSING``); only bout 9101's placement varies by source.
        """

        card = [
            observation(
                "event",
                "event",
                EVENT_ID,
                {
                    "source_ids": {"espn_event_id": "990000001"},
                    "promotion": "UFC",
                    "name": "UFC Fixture: Precedence",
                    "official_event_date": "2026-09-01",
                    "official_date_timezone": "America/New_York",
                    "status": "scheduled",
                },
                identity_basis="source_alias",
                observed_at=observed_at,
            )
        ]
        for order, bout_id in enumerate((9100, 9101), start=1):
            card.append(
                observation(
                    f"bout-{bout_id}",
                    "bout",
                    bout_id,
                    {
                        "source_ids": {"espn_competition_id": str(bout_id)},
                        "fighters": fighter_pair(
                            f"ftr_{bout_id}_a", f"ftr_{bout_id}_b"
                        ),
                        "weight_class": "Lightweight",
                        "gender": "male",
                        "scheduled_rounds": 3,
                        "status": "scheduled",
                    },
                    observed_at=observed_at,
                )
            )
            contended = bout_id == 9101
            card.append(
                observation(
                    f"slot-{bout_id}",
                    "slot",
                    bout_id,
                    {
                        "is_current": True,
                        "card_section": section if contended else "main",
                        "order_overall": order,
                        "scheduled_start_time_utc": "2026-09-02T02:00:00Z",
                        "automatic_lock_time_utc": "2026-09-02T02:00:00Z",
                    },
                    source_kind=source_kind if contended else "espn_summary",
                    observed_at=observed_at,
                    # admin_override, time_group_inference and
                    # deterministic_fallback must all justify themselves.
                    reason=(
                        f"{source_kind} placement under test." if contended else None
                    ),
                )
            )
        return card

    def _section(self, store):
        return next(
            item for item in store.slots.values() if item["bout_id"] == 9101
        )["card_section"]

    def test_each_rank_only_loses_to_a_strictly_higher_one(self):
        store = new_store()

        # A quarantined fallback establishes the first value.
        submit_card_observations(
            store,
            EVENT_ID,
            self._card("deterministic_fallback", "early_prelim", T1),
            dry_run=False,
        )
        assert self._section(store) == "early_prelim"

        # Scraper inference outranks the quarantined fallback.
        submit_card_observations(
            store,
            EVENT_ID,
            self._card("time_group_inference", "prelim", T2),
            dry_run=False,
        )
        assert self._section(store) == "prelim"

        # Explicit ESPN metadata outranks inference.
        submit_card_observations(
            store, EVENT_ID, self._card("espn_detail", "main", T3), dry_run=False
        )
        assert self._section(store) == "main"

        # Inference can no longer take the field back from explicit metadata.
        submit_card_observations(
            store,
            EVENT_ID,
            self._card("time_group_inference", "early_prelim", T4),
            dry_run=False,
        )
        assert self._section(store) == "main"

        # Admin outranks everything.
        submit_card_observations(
            store,
            EVENT_ID,
            self._card("admin_override", "prelim", "2026-08-03T09:00:00Z"),
            dry_run=False,
        )
        assert self._section(store) == "prelim"

        # And explicit ESPN metadata cannot take it back from Admin.
        submit_card_observations(
            store,
            EVENT_ID,
            self._card("espn_detail", "main", "2026-08-03T10:00:00Z"),
            dry_run=False,
        )
        assert self._section(store) == "prelim"

    def test_explicit_espn_detail_outranks_summary_in_one_pass(self):
        store = new_store()
        observations = self._card("espn_summary", "prelim", T1)
        detail = copy.deepcopy(observations[-1])
        detail["observation_id"] = "slot-9101-detail"
        detail["source_ref"] = "fixture:slot-9101-detail"
        detail["payload_hash"] = "sha256:slot-9101-detail"
        detail["source_kind"] = "espn_detail"
        detail["values"]["card_section"] = "main"

        submit_card_observations(
            store, EVENT_ID, [*observations, detail], dry_run=False
        )

        assert self._section(store) == "main"


class TestTerminalBoutsAreRetained:
    def test_an_explicit_espn_cancellation_retires_a_still_listed_bout(self):
        store = new_store()
        payload = espn_event("numbered_three_sections.json")
        bout_id = int(payload["competitions"][1]["id"])
        espn_pass(store, "numbered_three_sections.json")

        def cancel_still_listed_bout(next_payload):
            next_payload["competitions"][1]["status"] = {
                "type": {
                    "name": "STATUS_CANCELED",
                    "state": "pre",
                    "completed": False,
                }
            }

        plan, receipt = espn_pass(
            store,
            "numbered_three_sections.json",
            observed_at=T2,
            mutate=cancel_still_listed_bout,
        )

        assert not plan.blocked and receipt.verified_converged
        assert store.bouts[bout_id]["status"] == "cancelled"
        slot = next(slot for slot in store.slots.values() if slot["bout_id"] == bout_id)
        assert slot["is_current"] is False

    @pytest.mark.parametrize("status", ["cancelled", "postponed", "replaced"])
    def test_a_terminal_bout_keeps_its_document_and_lifecycle(self, status):
        store = new_store()
        submit_card_observations(
            store, EVENT_ID, synthetic_card({9201: {}, 9202: {}}), dry_run=False
        )
        assert set(store.bouts) == {9201, 9202}

        submit_card_observations(
            store,
            EVENT_ID,
            synthetic_card({9201: {"status": status}, 9202: {}}, observed_at=T2),
            dry_run=False,
        )

        assert set(store.bouts) == {9201, 9202}
        assert store.bouts[9201]["status"] == status
        assert store.deleted_documents == []
        # The slot survives in every case; only a terminal lifecycle retires it.
        slot = next(
            item for item in store.slots.values() if item["bout_id"] == 9201
        )
        terminal = status in {"cancelled", "replaced"}
        assert slot["is_current"] is not terminal
        assert store.events[EVENT_ID]["total_bouts"] == (1 if terminal else 2)

    def test_a_cancelled_bout_is_not_revived_by_a_later_espn_pass(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        bout_id = min(store.bouts)
        admin_pass(
            store,
            kind="bout_lifecycle",
            bout_id=bout_id,
            values={"status": "cancelled"},
            observed_at=T2,
            command_id="cancel-1",
        )
        assert store.bouts[bout_id]["status"] == "cancelled"

        # ESPN still lists the bout on the card; it must not come back.
        payload = espn_event("numbered_three_sections.json")
        batch = build_espn_card_observations(
            payload, store.load_card(EVENT_ID), observed_at=T3
        )
        submit_card_observations(store, EVENT_ID, batch.observations, dry_run=False)

        assert store.bouts[bout_id]["status"] == "cancelled"
        assert "TERMINAL_BOUT_RELISTED" in {item.code for item in batch.findings}
        assert store.deleted_documents == []

    def test_the_mongo_adapter_never_issues_a_delete(self):
        db = _RecordingDb([{"id": EVENT_ID, "name": "Seed event"}])
        store = MongoCanonicalCardStore(db)
        payload = espn_event("numbered_three_sections.json")
        batch = build_espn_card_observations(
            payload, store.load_card(EVENT_ID), observed_at=T1
        )

        plan, receipt = submit_card_observations(
            store, EVENT_ID, batch.observations, dry_run=False
        )

        assert receipt.applied and receipt.verified_converged
        assert plan.operation_count > 0
        assert db.log, "the adapter must actually have written"
        assert {entry[1] for entry in db.log} == {"update_one"}
        assert len(db.bouts.documents) == 6
        assert len(db.event_card_slots.documents) == 6


class TestReplacementLineage:
    def test_espn_opponent_swap_is_linked_and_retires_the_old_matchup(self):
        """A shared ESPN athlete turns a disappearance into a replacement."""

        store = new_store()
        payload = espn_event("numbered_three_sections.json")
        original = copy.deepcopy(payload["competitions"][2])
        original_id = int(original["id"])
        espn_pass(store, "numbered_three_sections.json")

        replacement_id = 991299999

        def replace_opponent(next_payload):
            replacement = copy.deepcopy(original)
            replacement["id"] = str(replacement_id)
            replacement["competitors"][1]["id"] = "992299999"
            replacement["competitors"][1]["athlete"] = {
                "id": "992299999",
                "displayName": "Fixture Late Replacement",
            }
            next_payload["competitions"][2] = replacement

        plan, receipt = espn_pass(
            store,
            "numbered_three_sections.json",
            observed_at=T2,
            mutate=replace_opponent,
        )

        assert not plan.blocked and receipt.verified_converged
        assert store.bouts[original_id]["status"] == "replaced"
        assert store.bouts[original_id]["replaced_by_bout_id"] == replacement_id
        original_slot = next(
            slot for slot in store.slots.values() if slot["bout_id"] == original_id
        )
        assert original_slot["is_current"] is False
        assert store.bouts[replacement_id]["replaces_bout_id"] == original_id
        assert store.bouts[replacement_id]["matchup_revision"] == 2

    def test_a_fighter_replacement_creates_a_new_target_under_one_lineage(self):
        store = new_store()
        submit_card_observations(
            store, EVENT_ID, synthetic_card({9301: {}, 9302: {}}), dry_run=False
        )
        original_lineage = store.bouts[9301]["lineage_id"]
        assert store.bouts[9301]["matchup_revision"] == 1

        replacement = synthetic_card({9302: {}}, observed_at=T2)
        replacement.append(
            observation(
                "bout-9303",
                "bout",
                9303,
                {
                    "source_ids": {"espn_competition_id": "9303"},
                    "fighters": fighter_pair("ftr_9301_a", "ftr_late_replacement"),
                    "weight_class": "Lightweight",
                    "gender": "male",
                    "scheduled_rounds": 3,
                    "status": "scheduled",
                    "replaces_bout_id": 9301,
                },
                observed_at=T2,
            )
        )
        replacement.append(
            observation(
                "slot-9303",
                "slot",
                9303,
                {
                    "is_current": True,
                    "card_section": "main",
                    "order_overall": 2,
                    "scheduled_start_time_utc": "2026-09-02T02:00:00Z",
                    "automatic_lock_time_utc": "2026-09-02T02:00:00Z",
                },
                observed_at=T2,
            )
        )

        plan, receipt = submit_card_observations(
            store, EVENT_ID, replacement, dry_run=False
        )

        assert not plan.blocked and receipt.applied
        # A new bout target exists ...
        assert 9303 in store.bouts
        assert store.bouts[9303]["replaces_bout_id"] == 9301
        # ... under the same lineage, with a bumped matchup revision ...
        assert store.bouts[9303]["lineage_id"] == original_lineage
        assert store.bouts[9303]["matchup_revision"] == 2
        # ... while the original target is retained, not deleted.
        assert store.bouts[9301]["status"] == "replaced"
        assert store.bouts[9301]["replaced_by_bout_id"] == 9303
        assert store.deleted_documents == []
        retired = next(
            item for item in store.slots.values() if item["bout_id"] == 9301
        )
        assert retired["is_current"] is False

    def test_a_corner_swap_with_the_same_fighter_set_is_not_a_replacement(self):
        store = new_store()
        submit_card_observations(
            store, EVENT_ID, synthetic_card({9401: {}}), dry_run=False
        )
        lineage = store.bouts[9401]["lineage_id"]

        swapped = synthetic_card({9401: {}}, observed_at=T2)
        for item in swapped:
            if item["entity_type"] == "bout":
                item["values"]["fighters"] = fighter_pair(
                    "ftr_9401_a", "ftr_9401_b", swap=True
                )

        plan, receipt = submit_card_observations(
            store, EVENT_ID, swapped, dry_run=False
        )

        assert not plan.blocked and receipt.applied
        assert plan.quarantines == ()
        assert store.bouts[9401]["matchup_revision"] == 1
        assert store.bouts[9401]["lineage_id"] == lineage
        assert store.bouts[9401]["status"] == "scheduled"
        assert store.bouts[9401].get("replaced_by_bout_id") is None
        corners = {
            fighter["fighter_id"]: fighter["corner"]
            for fighter in store.bouts[9401][DOCUMENT_SIDECAR_FIELD]["fighters"]
        }
        assert corners == {"ftr_9401_a": "blue", "ftr_9401_b": "red"}

    def test_reusing_a_bout_id_for_a_different_matchup_is_quarantined(self):
        store = new_store()
        submit_card_observations(
            store, EVENT_ID, synthetic_card({9501: {}}), dry_run=False
        )

        plan, receipt = submit_card_observations(
            store,
            EVENT_ID,
            synthetic_card(
                {9501: {"fighters": fighter_pair("ftr_x", "ftr_y")}},
                observed_at=T2,
            ),
            dry_run=False,
        )

        assert plan.blocked and not receipt.applied
        assert {item.code for item in plan.quarantines} == {
            "MATCHUP_ID_REUSE_FORBIDDEN"
        }
        assert store.bouts[9501]["matchup_revision"] == 1


class TestConvergence:
    """B-008: after the writers run, a replay produces zero operations."""

    @pytest.mark.parametrize(
        "filename",
        [
            "numbered_three_sections.json",
            "fight_night_two_sections.json",
            "title_heavy_missing_explicit_flag.json",
        ],
    )
    def test_an_immediate_replay_is_a_zero_diff(self, filename):
        store = new_store()

        first, receipt = espn_pass(store, filename)
        assert first.operation_count > 0
        assert receipt.verified_converged is True

        payload = espn_event(filename)
        replay = plan_canonical_card_write(
            store.load_card(EVENT_ID),
            build_espn_card_observations(
                payload, store.load_card(EVENT_ID), observed_at=T1
            ).observations,
        )

        assert replay.converged is True
        assert replay.operation_count == 0
        assert replay.event_update == {}
        assert replay.bout_writes == ()
        assert replay.slot_plan.operations == ()

    @pytest.mark.parametrize(
        "filename",
        [
            "numbered_three_sections.json",
            "fight_night_two_sections.json",
            "title_heavy_missing_explicit_flag.json",
        ],
    )
    def test_a_later_pass_of_an_unchanged_card_still_writes_nothing(self, filename):
        """A fresh observed_at is not a card change.

        Every real continuous run carries a new timestamp.  If re-observation
        churned evidence the slot reconciler would refuse the pass with
        ``REVISION_CONTENT_CONFLICT``, so this is the invariant that makes the
        boundary usable more than once.
        """

        store = new_store()
        espn_pass(store, filename, observed_at=T1)

        for observed_at in (T2, T3, T4):
            plan, receipt = espn_pass(store, filename, observed_at=observed_at)
            assert not plan.blocked, [item.code for item in plan.findings]
            assert plan.operation_count == 0
            assert plan.converged is True
            assert receipt.verified_converged is True

    def test_the_persisted_snapshot_is_byte_identical_after_a_replay(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json", observed_at=T1)
        before = json.dumps(
            store.events[EVENT_ID][SNAPSHOT_SIDECAR_FIELD], sort_keys=True, default=str
        )

        espn_pass(store, "numbered_three_sections.json", observed_at=T3)

        after = json.dumps(
            store.events[EVENT_ID][SNAPSHOT_SIDECAR_FIELD], sort_keys=True, default=str
        )
        assert before == after

    def test_convergence_survives_an_interleaved_admin_command(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json", observed_at=T1)
        bout_id = min(store.bouts)
        admin_pass(
            store,
            kind="title",
            bout_id=bout_id,
            values={"is_title_fight": True, "title_type": "undisputed"},
            observed_at=T2,
            command_id="title-1",
        )

        plan, receipt = espn_pass(
            store, "numbered_three_sections.json", observed_at=T3
        )

        assert plan.operation_count == 0
        assert receipt.verified_converged is True
        assert store.bouts[bout_id]["is_title_fight"] is True

    def test_a_real_change_still_produces_operations(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json", observed_at=T1)

        plan, _ = espn_pass(
            store,
            "numbered_three_sections.json",
            observed_at=T2,
            mutate=lambda payload: payload["competitions"][0]["format"][
                "regulation"
            ].update({"periods": 5}),
        )

        assert plan.operation_count > 0
        assert plan.converged is False

    def test_stabilization_keeps_a_strictly_higher_authority(self):
        """Re-confirmation is not news; an authority upgrade is."""

        previous = {
            "event": {
                "event_id": EVENT_ID,
                "status": "scheduled",
                "evidence": {
                    "status": {
                        "observation_id": "espn-1",
                        "source_kind": "espn_summary",
                        "observed_at": T1,
                    }
                },
            }
        }
        upgraded = {
            "event": {
                "event_id": EVENT_ID,
                "status": "scheduled",
                "evidence": {
                    "status": {
                        "observation_id": "admin-1",
                        "source_kind": "admin_override",
                        "observed_at": T2,
                    }
                },
            }
        }
        reconfirmed = copy.deepcopy(previous)
        reconfirmed["event"]["evidence"]["status"]["observed_at"] = T2

        assert (
            stabilize_snapshot_provenance(upgraded, previous)["event"]["evidence"][
                "status"
            ]["source_kind"]
            == "admin_override"
        )
        assert (
            stabilize_snapshot_provenance(reconfirmed, previous)["event"]["evidence"][
                "status"
            ]["observed_at"]
            == T1
        )

    def test_stabilization_does_not_hide_a_changed_value(self):
        previous = {
            "event": {
                "event_id": EVENT_ID,
                "status": "scheduled",
                "evidence": {
                    "status": {
                        "observation_id": "espn-1",
                        "source_kind": "espn_summary",
                        "observed_at": T1,
                    }
                },
            }
        }
        changed = copy.deepcopy(previous)
        changed["event"]["status"] = "completed"
        changed["event"]["evidence"]["status"]["observed_at"] = T2

        stabilized = stabilize_snapshot_provenance(changed, previous)

        assert stabilized["event"]["status"] == "completed"
        assert stabilized["event"]["evidence"]["status"]["observed_at"] == T2


class TestLegacyCompatibilityProjection:
    def test_legacy_fighters_and_result_fields_still_populate(self):
        store = new_store()

        espn_pass(store, "fight_night_two_sections.json")

        for bout in store.bouts.values():
            fighters = bout["fighters"]
            assert set(fighters) == {"red", "blue"}
            for corner, entry in fighters.items():
                assert entry["corner"] == corner
                assert entry["fighter_name"]
                assert entry["fighter_id"]
            result = bout["result"]
            assert result["winner"] in {"red", "blue"}
            assert result["winner_name"]
            assert result["outcome"] in {"red", "blue"}
            assert result["method"] in {"KO/TKO", "SUB", "DEC", "DQ", "OTHER", "NC"}
            assert result["source"] == "card_data_v1"

    def test_legacy_structure_fields_are_derived_from_the_canonical_slot(self):
        store = new_store()

        espn_pass(store, "numbered_three_sections.json")

        for bout in store.bouts.values():
            slot = next(
                item
                for item in store.slots.values()
                if item["bout_id"] == bout["id"]
            )
            assert bout["card_section"] == slot["card_section"]
            assert bout["order_overall"] == slot["order_overall"]
            assert bout["order_section"] == slot["order_section"]
            assert bout["card_order"] == slot["order_section"]
            assert bout["is_main_event"] == (slot["role"] == "main_event")
        main_events = [
            bout for bout in store.bouts.values() if bout["is_main_event"]
        ]
        assert len(main_events) == 1
        assert store.events[EVENT_ID]["main_event_bout_id"] == main_events[0]["id"]

    def test_a_new_canonical_bout_gets_a_legacy_identity_seed(self):
        store = new_store()

        espn_pass(store, "numbered_three_sections.json")

        for bout_id, bout in store.bouts.items():
            assert bout["_id"] == bout_id and bout["id"] == bout_id
            assert bout["event_id"] == EVENT_ID
            assert bout["slug"].startswith("espn-")
            assert bout["espn_competition_id"] == str(bout_id)


class TestCanonicalCardState:
    def test_state_requires_a_positive_event_id_and_identified_bouts(self):
        with pytest.raises(CanonicalCardStateError):
            CanonicalCardState.build(0, None)
        with pytest.raises(CanonicalCardStateError):
            CanonicalCardState.build(EVENT_ID, None, [{"no_id": True}])
        with pytest.raises(CanonicalCardStateError):
            CanonicalCardState.build(EVENT_ID, "not-a-document")

    def test_rebuild_bootstraps_from_backfilled_sidecars(self):
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")
        # Simulate an SCR-013 backfilled card: per-document sidecars, no envelope.
        state = store.load_card(EVENT_ID)
        event = dict(state.event)
        event.pop(SNAPSHOT_SIDECAR_FIELD)

        snapshot = rebuild_previous_snapshot(
            CanonicalCardState.build(EVENT_ID, event, state.bouts, state.slots)
        )

        assert snapshot is not None
        assert snapshot["event"]["event_id"] == EVENT_ID
        assert len(snapshot["bouts"]) == 6
        assert len(snapshot["card_slots"]) == 6

    def test_rebuild_returns_none_without_canonical_sidecars(self):
        state = CanonicalCardState.build(EVENT_ID, {"id": EVENT_ID}, [], [])

        assert rebuild_previous_snapshot(state) is None

    def test_a_drifted_slot_collection_refuses_the_commit(self):
        store = new_store()
        payload = espn_event("numbered_three_sections.json")
        batch = build_espn_card_observations(
            payload, store.load_card(EVENT_ID), observed_at=T1
        )
        plan = plan_canonical_card_write(store.load_card(EVENT_ID), batch.observations)
        store.slots["intruder"] = {
            "_id": "intruder",
            "event_id": EVENT_ID,
            "bout_id": 999999,
        }

        with pytest.raises(CanonicalCardWriteError):
            store.commit_card_write(plan)


class TestReplayRetryAndCorrection:
    """SCR-017 point 3: the properties a continuous writer must actually hold."""

    def test_an_identical_replay_writes_nothing_and_converges(self):
        """Idempotency: re-running the same pass is not a second write."""
        store = new_store()
        first_plan, first_receipt = espn_pass(store, "numbered_three_sections.json")

        second_plan, second_receipt = espn_pass(
            store, "numbered_three_sections.json", observed_at=T2
        )

        assert first_receipt.applied and first_plan.operation_count > 0
        assert second_plan.converged, "an unchanged card must plan to nothing"
        assert second_plan.operation_count == 0
        assert second_receipt.verified_converged

    def test_every_applied_write_proves_convergence_before_returning(self):
        """The replay check is the guard against leaving dual authority behind."""
        store = new_store()

        _, receipt = espn_pass(store, "numbered_three_sections.json")

        assert receipt.applied is True
        assert receipt.verified_converged is True

    def test_a_dry_run_writes_nothing_and_claims_no_verification(self):
        store = new_store()

        plan, receipt = espn_pass(store, "numbered_three_sections.json", dry_run=True)

        assert receipt.dry_run is True
        assert receipt.applied is False
        assert receipt.verified_converged is False, "a dry run has verified nothing"
        assert plan.operation_count > 0, "but it still describes the work"

        # And the store is untouched: a second dry run plans the same work.
        replay, _ = espn_pass(store, "numbered_three_sections.json", dry_run=True)
        assert replay.operation_count == plan.operation_count

    def test_a_blocked_plan_is_never_applied(self):
        """`apply` refuses a blocked plan outright rather than trusting callers."""
        store = new_store()
        plan, _ = espn_pass(store, "numbered_three_sections.json", dry_run=True)
        # A plan without a slot plan is blocked by construction.
        blocked = dataclasses.replace(plan, slot_plan=None)

        with pytest.raises(CanonicalCardWriteError):
            apply_canonical_card_write(blocked, store, dry_run=False)

    def test_a_corrected_result_is_written_and_then_converges(self):
        """A correction is a normal write, not a special path."""
        store = new_store()
        espn_pass(store, "numbered_three_sections.json")

        def wrong_then_right(winner_index):
            def mutate(payload):
                competitions = payload["competitions"]
                for competition in competitions:
                    competitors = competition.get("competitors") or []
                    if len(competitors) < 2:
                        continue
                    for index, competitor in enumerate(competitors):
                        competitor["winner"] = index == winner_index
                    competition["status"] = {
                        "type": {"completed": True, "state": "post", "name": "STATUS_FINAL"}
                    }
                    break
            return mutate

        first, _ = espn_pass(
            store, "numbered_three_sections.json", observed_at=T2,
            mutate=wrong_then_right(0),
        )
        corrected, corrected_receipt = espn_pass(
            store, "numbered_three_sections.json", observed_at=T3,
            mutate=wrong_then_right(1),
        )

        assert first.operation_count > 0
        assert corrected.operation_count > 0, "the correction must actually write"
        assert corrected_receipt.verified_converged

        settled, _ = espn_pass(
            store, "numbered_three_sections.json", observed_at=T4,
            mutate=wrong_then_right(1),
        )
        assert settled.converged, "replaying the correction changes nothing further"
