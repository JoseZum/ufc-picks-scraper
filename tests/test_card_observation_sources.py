"""SCR-015/016 proofs for the continuous observation adapters.

:mod:`tapology_scraper.card_observation_sources` is the only place ESPN
payloads and Admin commands become CardData proposals.  It must stay pure and
must never decide precedence itself — it only assigns the source rank the
normalizer then resolves.

The identity rules proven here are the ones that cannot be expressed as field
precedence: an event is matched by trusted alias (WP-001), a bout by source
competition ID (WP-013), and external TITLE signals stay advisory (WP-007).
"""

import copy
import json
from pathlib import Path

import pytest

from tapology_scraper.canonical_card_writer import CanonicalCardState
from tapology_scraper.card_observation_sources import (
    ADMIN_COMMAND_KINDS,
    EVENT_TIMEZONE,
    AdminCardCommand,
    ObservationSourceError,
    build_admin_card_observations,
    build_espn_card_observations,
    utc_string,
)


FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "card_data_v1"
EVENT_ID = 880201
T1 = "2026-08-01T12:00:00Z"
T2 = "2026-08-01T13:00:00Z"


def espn_event(filename: str = "numbered_three_sections.json") -> dict:
    payload = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    return copy.deepcopy(payload["events"][0])


def state(event=None, bouts=(), slots=()):
    return CanonicalCardState.build(
        EVENT_ID, event if event is not None else {"id": EVENT_ID}, bouts, slots
    )


def by_entity(batch, entity_type):
    return [
        item for item in batch.observations if item["entity_type"] == entity_type
    ]


def admin_command(kind, **overrides):
    payload = {
        "command_id": "cmd-1",
        "kind": kind,
        "event_id": EVENT_ID,
        "observed_at": T1,
        "reason": "Operator correction under test.",
        "values": {},
    }
    payload.update(overrides)
    return AdminCardCommand(**payload)


class TestEspnIdentityRules:
    def test_a_contradicting_alias_is_refused_rather_than_applied(self):
        """WP-001: a conflicting ESPN alias is quarantined, never written."""

        batch = build_espn_card_observations(
            espn_event(),
            state({"id": EVENT_ID, "espn_event_id": "990100999"}),
            observed_at=T1,
        )

        assert batch.blocked is True
        assert batch.observations == ()
        assert {item.code for item in batch.findings} == {
            "ESPN_EVENT_ALIAS_CONFLICT"
        }

    def test_a_matching_alias_is_accepted(self):
        payload = espn_event()

        batch = build_espn_card_observations(
            payload,
            state({"id": EVENT_ID, "espn_event_id": str(payload["id"])}),
            observed_at=T1,
        )

        assert batch.blocked is False
        assert by_entity(batch, "event")[0]["identity_basis"] == "source_alias"

    def test_a_payload_without_an_event_id_is_refused(self):
        payload = espn_event()
        payload.pop("id")

        batch = build_espn_card_observations(payload, state(), observed_at=T1)

        assert batch.blocked is True
        assert {item.code for item in batch.findings} == {"ESPN_EVENT_ID_MISSING"}

    def test_an_empty_card_retains_the_canonical_card(self):
        payload = espn_event()
        payload["competitions"] = []

        batch = build_espn_card_observations(payload, state(), observed_at=T1)

        assert batch.observations == ()
        assert batch.blocked is False
        assert {item.code for item in batch.findings} == {"ESPN_CARD_EMPTY"}

    def test_bouts_are_matched_by_source_competition_id_only(self):
        """WP-013: fighter-name similarity never establishes bout identity."""

        payload = espn_event()
        competition = payload["competitions"][0]
        # A local bout with the same fighters but no alias must not be matched.
        decoy = {
            "id": 4242,
            "event_id": EVENT_ID,
            "fighters": {
                "red": {
                    "fighter_name": competition["competitors"][0]["athlete"][
                        "displayName"
                    ]
                },
                "blue": {
                    "fighter_name": competition["competitors"][1]["athlete"][
                        "displayName"
                    ]
                },
            },
        }

        batch = build_espn_card_observations(
            payload, state(bouts=[decoy]), observed_at=T1
        )

        bout_ids = {item["entity_id"] for item in by_entity(batch, "bout")}
        assert 4242 not in bout_ids
        assert int(competition["id"]) in bout_ids
        for item in by_entity(batch, "bout"):
            assert item["identity_basis"] == "source_competition_id"

    def test_an_aliased_local_bout_keeps_its_canonical_id(self):
        payload = espn_event()
        competition_id = str(payload["competitions"][0]["id"])
        local = {
            "id": 5150,
            "event_id": EVENT_ID,
            "espn_competition_id": competition_id,
        }

        batch = build_espn_card_observations(
            payload, state(bouts=[local]), observed_at=T1
        )

        matched = [
            item for item in by_entity(batch, "bout") if item["entity_id"] == 5150
        ]
        assert len(matched) == 1
        assert matched[0]["values"]["source_ids"] == {
            "espn_competition_id": competition_id
        }

    def test_a_non_numeric_unmatched_competition_is_skipped_with_a_finding(self):
        payload = espn_event()
        payload["competitions"][0]["id"] = "not-a-number"

        batch = build_espn_card_observations(payload, state(), observed_at=T1)

        assert "ESPN_COMPETITION_UNMATCHED" in {
            item.code for item in batch.findings
        }
        assert len(by_entity(batch, "bout")) == 5


class TestEspnSourceRanking:
    def test_scoreboard_only_facts_are_espn_summary(self):
        batch = build_espn_card_observations(espn_event(), state(), observed_at=T1)

        assert {item["source_kind"] for item in by_entity(batch, "bout")} == {
            "espn_summary"
        }

    def test_explicit_competition_detail_upgrades_the_rank(self):
        payload = espn_event()
        competition_id = str(payload["competitions"][0]["id"])

        batch = build_espn_card_observations(
            payload,
            state(),
            observed_at=T1,
            competition_metadata={competition_id: {"card_section": "main"}},
        )

        detailed = next(
            item
            for item in by_entity(batch, "bout")
            if item["entity_id"] == int(competition_id)
        )
        undetailed = next(
            item
            for item in by_entity(batch, "bout")
            if item["entity_id"] != int(competition_id)
        )
        assert detailed["source_kind"] == "espn_detail"
        assert undetailed["source_kind"] == "espn_summary"
        slot = next(
            item
            for item in by_entity(batch, "slot")
            if item["entity_id"] == int(competition_id)
        )
        assert slot["source_kind"] == "espn_detail"
        assert slot["values"]["card_section"] == "main"

    def test_a_result_is_always_submitted_as_explicit_detail(self):
        batch = build_espn_card_observations(
            espn_event("fight_night_two_sections.json"), state(), observed_at=T1
        )

        results = by_entity(batch, "result")
        assert len(results) == 4
        assert {item["source_kind"] for item in results} == {"espn_detail"}
        assert all(
            item["values"]["outcome"] in {"red_win", "blue_win", "draw", "no_contest"}
            for item in results
        )

    def test_no_espn_observation_ever_claims_admin_authority(self):
        batch = build_espn_card_observations(
            espn_event("fight_night_two_sections.json"), state(), observed_at=T1
        )

        assert "admin_override" not in {
            item["source_kind"] for item in batch.observations
        }

    def test_event_timing_derives_the_official_date_in_the_event_zone(self):
        batch = build_espn_card_observations(espn_event(), state(), observed_at=T1)

        event = by_entity(batch, "event")[0]["values"]
        assert event["official_date_timezone"] == EVENT_TIMEZONE
        # 2026-08-15T21:00Z is still 2026-08-15 in America/New_York.
        assert event["official_event_date"] == "2026-08-15"
        assert set(event["section_start_times_utc"]) == {
            "early_prelim",
            "prelim",
            "main",
        }

    def test_an_invalid_observed_at_is_refused(self):
        with pytest.raises(ObservationSourceError):
            build_espn_card_observations(espn_event(), state(), observed_at="soon")
        with pytest.raises(ObservationSourceError):
            build_espn_card_observations("not-a-payload", state(), observed_at=T1)
        with pytest.raises(ObservationSourceError):
            build_espn_card_observations(espn_event(), {"id": 1}, observed_at=T1)


class TestEspnTitleSignalsStayAdvisory:
    def test_a_title_flag_is_emitted_but_never_as_an_override(self):
        payload = espn_event()
        payload["competitions"][0]["titleFight"] = True
        competition_id = int(payload["competitions"][0]["id"])

        batch = build_espn_card_observations(payload, state(), observed_at=T1)

        titled = next(
            item
            for item in by_entity(batch, "bout")
            if item["entity_id"] == competition_id
        )
        assert titled["values"]["is_title_fight"] is True
        assert titled["source_kind"] != "admin_override"
        # ESPN never proposes the Admin-only companions.
        assert "is_bmf_title_fight" not in titled["values"]
        assert "title_type" not in titled["values"]

    def test_an_absent_title_flag_is_not_invented(self):
        payload = espn_event()
        assert "titleFight" not in payload["competitions"][0]

        batch = build_espn_card_observations(payload, state(), observed_at=T1)

        for item in by_entity(batch, "bout"):
            assert "is_title_fight" not in item["values"]


class TestEspnLifecycleSafety:
    @pytest.mark.parametrize("status", ["cancelled", "replaced"])
    def test_a_terminal_bout_is_not_revived_by_a_relisting(self, status):
        payload = espn_event()
        competition_id = str(payload["competitions"][0]["id"])
        persisted = {
            "id": int(competition_id),
            "event_id": EVENT_ID,
            "espn_competition_id": competition_id,
            "status": status,
            "card_data_v1": {"bout_id": int(competition_id), "status": status},
        }

        batch = build_espn_card_observations(
            payload, state(bouts=[persisted]), observed_at=T1
        )

        terminal = next(
            item
            for item in by_entity(batch, "bout")
            if item["entity_id"] == int(competition_id)
        )
        # No lifecycle value is proposed, so the terminal status survives ...
        assert "status" not in terminal["values"]
        # ... and no current slot is proposed for it.
        assert int(competition_id) not in {
            item["entity_id"] for item in by_entity(batch, "slot")
        }
        assert "TERMINAL_BOUT_RELISTED" in {item.code for item in batch.findings}

    def test_a_bout_missing_from_the_payload_is_reported_not_removed(self):
        payload = espn_event()
        dropped = payload["competitions"].pop(0)
        bout_id = int(dropped["id"])
        persisted = {
            "id": bout_id,
            "event_id": EVENT_ID,
            "espn_competition_id": str(bout_id),
        }
        slot = {
            "_id": f"{EVENT_ID}:{bout_id}",
            "event_id": EVENT_ID,
            "bout_id": bout_id,
            "is_current": True,
        }

        batch = build_espn_card_observations(
            payload, state(bouts=[persisted], slots=[slot]), observed_at=T1
        )

        assert "BOUT_ABSENT_FROM_ESPN_PAYLOAD" in {
            item.code for item in batch.findings
        }
        # Nothing proposes a removal; the change policy owns that decision.
        assert bout_id not in {item["entity_id"] for item in batch.observations}

    def test_a_scheduled_bout_is_proposed_as_scheduled(self):
        batch = build_espn_card_observations(espn_event(), state(), observed_at=T1)

        assert {item["values"]["status"] for item in by_entity(batch, "bout")} == {
            "scheduled"
        }


class TestEspnDeterminism:
    def test_the_same_payload_produces_the_same_observations(self):
        first = build_espn_card_observations(espn_event(), state(), observed_at=T1)
        second = build_espn_card_observations(espn_event(), state(), observed_at=T1)

        assert json.dumps(
            [dict(item) for item in first.observations], sort_keys=True, default=str
        ) == json.dumps(
            [dict(item) for item in second.observations], sort_keys=True, default=str
        )

    def test_observations_are_sorted_and_carry_a_payload_hash(self):
        batch = build_espn_card_observations(espn_event(), state(), observed_at=T1)

        ids = [item["observation_id"] for item in batch.observations]
        assert ids == sorted(ids)
        assert len(set(ids)) == len(ids)
        for item in batch.observations:
            assert item["payload_hash"].startswith("sha256:")
            assert item["event_id"] == EVENT_ID


class TestAdminCommandValidation:
    def test_every_supported_kind_is_covered_by_the_contract(self):
        assert ADMIN_COMMAND_KINDS == {
            "event_timing",
            "event_lifecycle",
            "bout_timing",
            "bout_structure",
            "bout_lifecycle",
            "title",
            "result",
            "clear_result",
        }

    def test_an_unsupported_kind_is_refused(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("delete_bout", bout_id=1), state()
            )

    def test_every_admin_command_requires_a_reason(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command(
                    "event_lifecycle", reason=" ", values={"status": "cancelled"}
                ),
                state(),
            )

    def test_a_bout_command_requires_a_bout_id(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("title", values={"is_title_fight": True}), state()
            )

    def test_a_command_must_target_the_loaded_event(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command(
                    "event_lifecycle", event_id=999999, values={"status": "cancelled"}
                ),
                state(),
            )

    def test_an_empty_timing_command_is_refused(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("event_timing", values={}), state()
            )

    def test_a_command_for_an_unpersisted_bout_is_flagged_not_dropped(self):
        batch = build_admin_card_observations(
            admin_command("title", bout_id=4321, values={"is_title_fight": True}),
            state(),
        )

        assert "ADMIN_BOUT_NOT_PERSISTED" in {item.code for item in batch.findings}
        assert len(batch.observations) == 1


class TestAdminTitleAuthority:
    def test_title_requires_an_explicit_boolean(self):
        for values in ({}, {"is_title_fight": None}, {"title_type": "interim"}):
            with pytest.raises(ObservationSourceError):
                build_admin_card_observations(
                    admin_command("title", bout_id=1, values=values), state()
                )

    def test_an_explicit_false_is_a_complete_durable_override(self):
        batch = build_admin_card_observations(
            admin_command("title", bout_id=1, values={"is_title_fight": False}),
            state(),
        )

        observation = batch.observations[0]
        assert observation["source_kind"] == "admin_override"
        assert observation["values"] == {
            "is_title_fight": False,
            "is_bmf_title_fight": False,
            "title_type": "none",
        }
        assert observation["reason"]

    def test_an_explicit_true_resolves_all_three_title_fields(self):
        batch = build_admin_card_observations(
            admin_command(
                "title",
                bout_id=1,
                values={"is_title_fight": True, "title_type": "interim"},
            ),
            state(),
        )

        assert batch.observations[0]["values"] == {
            "is_title_fight": True,
            "is_bmf_title_fight": False,
            "title_type": "interim",
        }

    def test_a_bmf_title_is_normalized_consistently(self):
        batch = build_admin_card_observations(
            admin_command(
                "title",
                bout_id=1,
                values={"is_title_fight": True, "is_bmf_title_fight": True},
            ),
            state(),
        )

        assert batch.observations[0]["values"] == {
            "is_title_fight": True,
            "is_bmf_title_fight": True,
            "title_type": "bmf",
        }

    def test_an_unknown_title_type_degrades_without_losing_the_title(self):
        batch = build_admin_card_observations(
            admin_command(
                "title",
                bout_id=1,
                values={"is_title_fight": True, "title_type": "made-up"},
            ),
            state(),
        )

        assert batch.observations[0]["values"] == {
            "is_title_fight": True,
            "is_bmf_title_fight": False,
            "title_type": "unknown",
        }


class TestAdminCommandsCarryOverrideRank:
    @pytest.mark.parametrize(
        "kind,bout_id,values,entity_type",
        [
            (
                "event_timing",
                None,
                {"official_event_date": "2026-09-01"},
                "event",
            ),
            ("event_lifecycle", None, {"status": "postponed"}, "event"),
            ("bout_lifecycle", 1, {"status": "cancelled"}, "bout"),
            ("title", 1, {"is_title_fight": True}, "bout"),
            (
                "bout_timing",
                1,
                {"automatic_lock_time_utc": "2026-09-01T22:00:00Z"},
                "slot",
            ),
            ("bout_structure", 1, {"card_section": "main"}, "slot"),
            (
                "result",
                1,
                {
                    "outcome": "red",
                    "winner_fighter_id": "ftr_red",
                    "method_family": "decision",
                },
                "result",
            ),
            ("clear_result", 1, {}, "result"),
        ],
    )
    def test_each_command_becomes_one_admin_override_observation(
        self, kind, bout_id, values, entity_type
    ):
        batch = build_admin_card_observations(
            admin_command(kind, bout_id=bout_id, values=values), state()
        )

        assert len(batch.observations) == 1
        observation = batch.observations[0]
        assert observation["source_kind"] == "admin_override"
        assert observation["identity_basis"] == "admin"
        assert observation["entity_type"] == entity_type
        assert observation["event_id"] == EVENT_ID
        assert observation["reason"]
        assert observation["observation_id"].startswith("admin:")

    def test_clear_result_clears_rather_than_writing_an_empty_result(self):
        batch = build_admin_card_observations(
            admin_command("clear_result", bout_id=1), state()
        )

        assert batch.observations[0]["values"] == {}
        assert batch.observations[0]["clear_fields"] == ["result"]

    def test_a_decisive_result_requires_a_winner(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command(
                    "result", bout_id=1, values={"outcome": "red", "method": "KO"}
                ),
                state(),
            )

    def test_a_draw_drops_the_winner_reference(self):
        batch = build_admin_card_observations(
            admin_command(
                "result",
                bout_id=1,
                values={
                    "outcome": "draw",
                    "winner_fighter_id": "ftr_red",
                    "method": "Decision",
                },
            ),
            state(),
        )

        values = batch.observations[0]["values"]
        assert values["outcome"] == "draw"
        assert values["winner_fighter_id"] is None
        assert values["method_family"] == "decision"

    def test_an_unrecognised_outcome_is_refused(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("result", bout_id=1, values={"outcome": "maybe"}),
                state(),
            )

    def test_an_invalid_lifecycle_status_is_refused(self):
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("bout_lifecycle", bout_id=1, values={"status": "meh"}),
                state(),
            )
        with pytest.raises(ObservationSourceError):
            build_admin_card_observations(
                admin_command("event_lifecycle", values={"status": "meh"}), state()
            )


class TestUtcString:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2026-08-01T12:00:00Z", "2026-08-01T12:00:00Z"),
            ("2026-08-01T08:00:00-04:00", "2026-08-01T12:00:00Z"),
            ("", None),
            (None, None),
            ("not-a-time", None),
        ],
    )
    def test_instants_are_canonicalized_or_rejected(self, value, expected):
        assert utc_string(value) == expected


class TestPurity:
    def test_the_adapter_imports_no_driver_and_writes_nothing(self):
        import tapology_scraper.card_observation_sources as module

        source = Path(module.__file__).read_text(encoding="utf-8")

        for forbidden in ("import pymongo", "from pymongo", "import scrapy", "requests"):
            assert forbidden not in source
        for method in ("update_one", "insert_one", "delete_one", "delete_many"):
            assert f".{method}(" not in source
