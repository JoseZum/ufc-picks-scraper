import ast
import copy
import json
from pathlib import Path

import pytest

from tapology_scraper.card_data_contract import validate_card_data_v1
from tapology_scraper.card_data_normalizer import (
    NormalizationInputError,
    normalize_card_data_v1,
)


FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "card_data_v1"
EVENT_ID = 81001
T1 = "2026-08-01T12:00:00Z"
T2 = "2026-08-01T13:00:00Z"
T3 = "2026-08-01T14:00:00Z"
T4 = "2026-08-01T15:00:00Z"
T5 = "2026-08-01T16:00:00Z"


def observation(
    observation_id,
    entity_type,
    entity_id,
    values=None,
    *,
    source_kind="espn_summary",
    observed_at=T1,
    clear_fields=(),
    identity_basis="canonical_id",
    reason=None,
    event_id=EVENT_ID,
):
    return {
        "observation_id": observation_id,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": event_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"fixture:{observation_id}",
        "source_event_id": f"source-event-{event_id}",
        "values": values or {},
        "clear_fields": list(clear_fields),
        "identity_basis": identity_basis,
        "reason": reason,
        "payload_hash": f"sha256:{observation_id}",
    }


def fighters(seed):
    return [
        {
            "fighter_id": f"ftr_{seed}_red",
            "display_name": f"Fixture {seed} Red",
            "corner": "red",
            "source_ids": {"espn_athlete_id": f"{seed}1"},
            "identity_confidence": "exact_source",
        },
        {
            "fighter_id": f"ftr_{seed}_blue",
            "display_name": f"Fixture {seed} Blue",
            "corner": "blue",
            "source_ids": {"espn_athlete_id": f"{seed}2"},
            "identity_confidence": "exact_source",
        },
    ]


def base_observations(*, admin_titles=True):
    values = [
        observation(
            "event-base",
            "event",
            EVENT_ID,
            {
                "source_ids": {"espn_event_id": "600099001"},
                "promotion": "UFC",
                "name": "UFC Fixture Night: Quartz vs. River",
                "official_event_date": "2026-08-15",
                "official_date_timezone": "America/New_York",
                "status": "scheduled",
            },
            identity_basis="source_alias",
        )
    ]
    layout = (
        (201, "main", 1, "2026-08-16T02:00:00Z"),
        (202, "prelim", 2, "2026-08-16T00:00:00Z"),
        (203, "early_prelim", 3, "2026-08-15T22:00:00Z"),
    )
    for bout_id, section, order, start in layout:
        values.append(
            observation(
                f"bout-{bout_id}",
                "bout",
                bout_id,
                {
                    "source_ids": {"espn_competition_id": f"900{bout_id}"},
                    "fighters": fighters(bout_id),
                    "weight_class": "Lightweight",
                    "gender": "male",
                    "scheduled_rounds": 5 if bout_id == 201 else 3,
                    "status": "scheduled",
                },
                source_kind="espn_detail",
                identity_basis="source_competition_id",
            )
        )
        values.append(
            observation(
                f"slot-{bout_id}",
                "slot",
                bout_id,
                {
                    "is_current": True,
                    "card_section": section,
                    "order_overall": order,
                    "order_section": order,
                    "scheduled_start_time_utc": start,
                    "automatic_lock_time_utc": start,
                },
                source_kind="espn_detail",
                identity_basis="source_competition_id",
            )
        )
        if admin_titles:
            values.append(
                observation(
                    f"title-{bout_id}",
                    "bout",
                    bout_id,
                    {"is_title_fight": False},
                    source_kind="admin_override",
                    identity_basis="admin",
                    reason="Admin explicitly marked this as a non-title bout.",
                )
            )
    return values


def bout(snapshot, bout_id=201):
    return next(item for item in snapshot["bouts"] if item["bout_id"] == bout_id)


def slot(snapshot, bout_id=201):
    return next(
        item for item in snapshot["card_slots"] if item["bout_id"] == bout_id
    )


def quarantine_codes(result):
    return {item.code for item in result.quarantines}


def change_types(result):
    return {item["type"] for item in result.change_set["changes"]}


def test_initial_normalization_is_valid_deterministic_and_does_not_mutate_input():
    observations = base_observations()
    before = copy.deepcopy(observations)

    first = normalize_card_data_v1(observations)
    second = normalize_card_data_v1(list(reversed(observations)))

    assert first.as_dict() == second.as_dict()
    assert observations == before
    assert first.snapshot["snapshot_revision"] == 1
    assert first.snapshot["current_eligibility"]["denominator"] == 3
    assert validate_card_data_v1(first.snapshot).is_valid


def test_admin_structure_override_beats_newer_espn_detail():
    observations = base_observations()
    observations.extend(
        [
            observation(
                "admin-section",
                "slot",
                202,
                {"card_section": "main"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Promote this bout to the main card.",
            ),
            observation(
                "espn-section-later",
                "slot",
                202,
                {"card_section": "prelim"},
                source_kind="espn_detail",
                observed_at=T3,
            ),
        ]
    )

    result = normalize_card_data_v1(observations)

    assert slot(result.snapshot, 202)["card_section"] == "main"
    assert slot(result.snapshot, 202)["evidence"]["card_section"][
        "source_kind"
    ] == "admin_override"


def test_lower_rank_refetch_cannot_replace_previous_admin_value():
    initial = normalize_card_data_v1(
        base_observations()
        + [
            observation(
                "admin-weight",
                "bout",
                201,
                {"weight_class": "Catch Weight"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Official catch-weight correction.",
            )
        ]
    )

    updated = normalize_card_data_v1(
        [
            observation(
                "espn-weight-later",
                "bout",
                201,
                {"weight_class": "Lightweight"},
                source_kind="espn_detail",
                observed_at=T4,
            )
        ],
        initial.snapshot,
    )

    assert bout(updated.snapshot)["weight_class"] == "Catch Weight"
    assert updated.snapshot["snapshot_revision"] == initial.snapshot[
        "snapshot_revision"
    ]


def test_older_same_rank_observation_is_quarantined_as_stale():
    initial = normalize_card_data_v1(base_observations())

    updated = normalize_card_data_v1(
        [
            observation(
                "stale-weight",
                "bout",
                201,
                {"weight_class": "Welterweight"},
                source_kind="espn_detail",
                observed_at="2026-08-01T11:00:00Z",
            )
        ],
        initial.snapshot,
    )

    assert bout(updated.snapshot)["weight_class"] == "Lightweight"
    assert "STALE_OBSERVATION" in quarantine_codes(updated)


def test_equal_rank_same_time_conflict_is_quarantined_without_guessing():
    observations = base_observations()
    observations.extend(
        [
            observation(
                "detail-a",
                "bout",
                201,
                {"weight_class": "Welterweight"},
                source_kind="espn_detail",
                observed_at=T2,
            ),
            observation(
                "detail-b",
                "bout",
                201,
                {"weight_class": "Middleweight"},
                source_kind="espn_detail",
                observed_at=T2,
            ),
        ]
    )

    result = normalize_card_data_v1(observations)

    assert bout(result.snapshot)["weight_class"] == "Lightweight"
    assert "SOURCE_VALUE_CONFLICT" in quarantine_codes(result)


def test_date_only_event_identity_never_mutates_the_canonical_event():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "wrong-date-only-event",
                "event",
                EVENT_ID,
                {
                    "name": "UFC Completely Different Event",
                    "official_event_date": "2026-08-15",
                    "source_ids": {"espn_event_id": "600000999"},
                },
                identity_basis="date_only",
                observed_at=T2,
            )
        ],
        initial.snapshot,
    )

    assert result.snapshot["event"]["name"] == initial.snapshot["event"]["name"]
    assert result.snapshot["event"]["source_ids"] == initial.snapshot["event"][
        "source_ids"
    ]
    assert "EVENT_IDENTITY_EVIDENCE_INSUFFICIENT" in quarantine_codes(result)


def test_existing_source_alias_cannot_be_silently_replaced():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "conflicting-alias",
                "event",
                EVENT_ID,
                {
                    "source_ids": {"espn_event_id": "600000999"},
                    "name": "UFC Wrong Event Must Not Leak",
                },
                identity_basis="source_alias",
                observed_at=T2,
            )
        ],
        initial.snapshot,
    )

    assert result.snapshot["event"]["source_ids"]["espn_event_id"] == "600099001"
    assert result.snapshot["event"]["name"] == initial.snapshot["event"]["name"]
    assert "SOURCE_ALIAS_CONFLICT" in quarantine_codes(result)


def test_conflicting_external_aliases_cannot_establish_a_new_identity():
    observations = base_observations()
    observations.append(
        observation(
            "second-new-alias",
            "event",
            EVENT_ID,
            {"source_ids": {"espn_event_id": "600000999"}},
            observed_at=T2,
            identity_basis="source_alias",
        )
    )

    result = normalize_card_data_v1(observations)

    assert "espn_event_id" not in result.snapshot["event"]["source_ids"]
    assert "SOURCE_ALIAS_CONFLICT" in quarantine_codes(result)


def test_external_title_metadata_is_only_a_suggestion():
    observations = base_observations(admin_titles=False)
    observations.append(
        observation(
            "espn-title-signal",
            "bout",
            201,
            {"is_title_fight": True, "title_type": "undisputed"},
            source_kind="espn_detail",
            observed_at=T2,
        )
    )

    result = normalize_card_data_v1(observations)
    target = bout(result.snapshot)

    assert target["is_title_fight"] is None
    assert target["title_type"] == "unknown"
    assert target["evidence"]["title_suggestions"][0]["source_kind"] == "espn_detail"
    assert result.snapshot["quality"]["capabilities"]["TITLE"] == "blocked"


def test_admin_title_true_cannot_be_overridden_by_external_false():
    observations = base_observations(admin_titles=False)
    observations.extend(
        [
            observation(
                "admin-title-on",
                "bout",
                201,
                {"is_title_fight": True, "title_type": "bmf"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin marked the BMF title bout.",
            ),
            observation(
                "tapology-title-off",
                "bout",
                201,
                {"is_title_fight": False},
                source_kind="tapology_explicit",
                observed_at=T4,
            ),
        ]
    )

    target = bout(normalize_card_data_v1(observations).snapshot)

    assert target["is_title_fight"] is True
    assert target["is_bmf_title_fight"] is True
    assert target["title_type"] == "bmf"


def test_admin_title_false_is_durable_against_external_true():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "espn-title-after-admin-off",
                "bout",
                201,
                {"is_title_fight": True, "title_type": "undisputed"},
                source_kind="espn_detail",
                observed_at=T4,
            )
        ],
        initial.snapshot,
    )
    target = bout(result.snapshot)

    assert target["is_title_fight"] is False
    assert target["is_bmf_title_fight"] is False
    assert target["title_type"] == "none"


def test_admin_title_clear_returns_unknown_without_promoting_external_signal():
    initial = normalize_card_data_v1(
        base_observations(admin_titles=False)
        + [
            observation(
                "admin-title-initial",
                "bout",
                201,
                {"is_title_fight": True, "title_type": "undisputed"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin title designation.",
            )
        ]
    )

    result = normalize_card_data_v1(
        [
            observation(
                "external-title-at-clear",
                "bout",
                201,
                {"is_title_fight": True, "title_type": "bmf"},
                source_kind="espn_detail",
                observed_at=T3,
            ),
            observation(
                "admin-title-clear",
                "bout",
                201,
                {},
                source_kind="admin_override",
                observed_at=T4,
                clear_fields=("title",),
                reason="Admin cleared the title override to unknown.",
            ),
        ],
        initial.snapshot,
    )
    target = bout(result.snapshot)

    assert target["is_title_fight"] is None
    assert target["is_bmf_title_fight"] is None
    assert target["title_type"] == "unknown"
    assert "TITLE_CHANGED" in change_types(result)


def test_global_and_section_orders_are_derived_once_and_contiguous():
    observations = base_observations()
    for item in observations:
        if item["entity_type"] == "slot":
            item["values"]["order_section"] = item["values"]["order_overall"]

    snapshot = normalize_card_data_v1(observations).snapshot
    current = sorted(snapshot["card_slots"], key=lambda item: item["order_overall"])

    assert [item["order_overall"] for item in current] == [1, 2, 3]
    assert [item["order_section"] for item in current] == [1, 1, 1]
    assert validate_card_data_v1(snapshot).is_valid


def test_headliner_roles_follow_the_new_canonical_order_not_stale_derived_roles():
    initial = normalize_card_data_v1(base_observations())

    updated = normalize_card_data_v1(
        [
            observation(
                "admin-new-main",
                "slot",
                202,
                {"card_section": "main", "order_overall": 1},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin promoted bout 202 to the main event.",
            ),
            observation(
                "admin-old-main-down",
                "slot",
                201,
                {"order_overall": 2},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin moved the previous main event to co-main.",
            ),
        ],
        initial.snapshot,
    )

    assert slot(updated.snapshot, 202)["role"] == "main_event"
    assert slot(updated.snapshot, 202)["order_overall"] == 1
    assert slot(updated.snapshot, 201)["role"] == "co_main"
    assert updated.snapshot["event"]["main_event_bout_id"] == 202
    assert "HEADLINER_CHANGED" in change_types(updated)


def test_idempotent_refetch_does_not_increment_any_revision():
    observations = base_observations()
    initial = normalize_card_data_v1(observations)

    repeated = normalize_card_data_v1(observations, initial.snapshot)

    assert repeated.snapshot["snapshot_revision"] == 1
    assert repeated.snapshot["event"]["lifecycle_revision"] == 1
    assert repeated.snapshot["event"]["structure_revision"] == 1
    assert repeated.snapshot["event"]["timing_revision"] == 1
    assert repeated.change_set["changes"] == []


def test_timing_change_increments_only_snapshot_and_timing_revision():
    initial = normalize_card_data_v1(base_observations())

    updated = normalize_card_data_v1(
        [
            observation(
                "admin-lock-change",
                "slot",
                203,
                {"automatic_lock_time_utc": "2026-08-15T21:30:00Z"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin adjusted the early-prelim lock.",
            )
        ],
        initial.snapshot,
    )

    assert updated.snapshot["snapshot_revision"] == 2
    assert updated.snapshot["event"]["timing_revision"] == 2
    assert updated.snapshot["event"]["structure_revision"] == 1
    assert updated.snapshot["event"]["lifecycle_revision"] == 1


def test_admin_section_timing_propagates_to_slots_and_beats_later_espn_detail():
    initial = normalize_card_data_v1(base_observations())

    updated = normalize_card_data_v1(
        [
            observation(
                "admin-prelim-lock",
                "event",
                EVENT_ID,
                {
                    "section_lock_times_utc": {
                        "prelim": "2026-08-15T23:30:00Z"
                    }
                },
                source_kind="admin_override",
                observed_at=T2,
                identity_basis="admin",
                reason="Admin corrected the prelim lock.",
            ),
            observation(
                "espn-prelim-lock-later",
                "slot",
                202,
                {"automatic_lock_time_utc": "2026-08-15T23:45:00Z"},
                source_kind="espn_detail",
                observed_at=T4,
                identity_basis="source_competition_id",
            ),
        ],
        initial.snapshot,
    )

    target = slot(updated.snapshot, 202)
    assert target["automatic_lock_time_utc"] == "2026-08-15T23:30:00Z"
    assert target["evidence"]["automatic_lock_time_utc"][
        "source_kind"
    ] == "admin_override"
    assert validate_card_data_v1(updated.snapshot).is_valid


def final_result(method="decision", ending_round=3):
    return {
        "outcome": "red_win",
        "winner_fighter_id": "ftr_201_red",
        "method_family": method,
        "method_detail": "Fixture result",
        "ending_round": ending_round,
        "ending_time_seconds": 300 if method == "decision" else 141,
    }


def test_result_record_correction_and_clear_are_monotonic_and_typed():
    initial = normalize_card_data_v1(base_observations())
    recorded = normalize_card_data_v1(
        [
            observation(
                "result-recorded",
                "result",
                201,
                final_result(),
                source_kind="espn_summary",
                observed_at=T2,
                identity_basis="source_competition_id",
            )
        ],
        initial.snapshot,
    )
    corrected = normalize_card_data_v1(
        [
            observation(
                "result-corrected",
                "result",
                201,
                final_result("submission", 2),
                source_kind="espn_summary",
                observed_at=T3,
                identity_basis="source_competition_id",
            )
        ],
        recorded.snapshot,
    )
    cleared = normalize_card_data_v1(
        [
            observation(
                "result-cleared",
                "result",
                201,
                {},
                source_kind="admin_override",
                observed_at=T4,
                clear_fields=("result",),
                identity_basis="admin",
                reason="Official result was entered on the wrong bout.",
            )
        ],
        corrected.snapshot,
    )

    assert bout(recorded.snapshot)["result_revision"] == 1
    assert bout(recorded.snapshot)["result"]["status"] == "final"
    assert recorded.snapshot["event"]["lifecycle_revision"] == 2
    assert recorded.snapshot["event"]["first_final_result_at"] == T2
    assert "RESULT_RECORDED" in change_types(recorded)
    assert bout(corrected.snapshot)["result_revision"] == 2
    assert bout(corrected.snapshot)["result"]["status"] == "corrected"
    assert "RESULT_CORRECTED" in change_types(corrected)
    assert bout(cleared.snapshot)["result_revision"] == 3
    assert bout(cleared.snapshot)["result"] is None
    assert bout(cleared.snapshot)["status"] == "scheduled"
    assert cleared.snapshot["event"]["lifecycle_revision"] == 3
    assert cleared.snapshot["event"]["first_final_result_at"] is None
    assert "RESULT_CLEARED" in change_types(cleared)


def test_canonical_result_and_admin_clear_own_bout_lifecycle_atomically():
    initial = normalize_card_data_v1(base_observations())
    recorded = normalize_card_data_v1(
        [
            observation(
                "atomic-result",
                "result",
                201,
                final_result(),
                observed_at=T2,
                identity_basis="source_competition_id",
            )
        ],
        initial.snapshot,
    )
    stale_scheduled = normalize_card_data_v1(
        [
            observation(
                "detail-still-scheduled",
                "bout",
                201,
                {"status": "scheduled"},
                source_kind="espn_detail",
                observed_at=T3,
                identity_basis="source_competition_id",
            )
        ],
        recorded.snapshot,
    )
    cleared = normalize_card_data_v1(
        [
            observation(
                "atomic-result-clear",
                "result",
                201,
                {},
                source_kind="admin_override",
                observed_at=T4,
                clear_fields=("result",),
                identity_basis="admin",
                reason="Admin cleared an invalid final result.",
            )
        ],
        stale_scheduled.snapshot,
    )
    external_completed = normalize_card_data_v1(
        [
            observation(
                "external-completed-after-clear",
                "bout",
                201,
                {"status": "completed"},
                source_kind="espn_detail",
                observed_at=T5,
                identity_basis="source_competition_id",
            )
        ],
        cleared.snapshot,
    )

    assert bout(stale_scheduled.snapshot)["status"] == "completed"
    assert bout(stale_scheduled.snapshot)["result"] is not None
    assert bout(external_completed.snapshot)["status"] == "scheduled"
    assert bout(external_completed.snapshot)["result"] is None


def test_cancellation_retains_bout_and_historical_slot_but_removes_eligibility():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "admin-cancel",
                "bout",
                202,
                {"status": "cancelled"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Bout officially cancelled.",
            )
        ],
        initial.snapshot,
    )

    assert bout(result.snapshot, 202)["status"] == "cancelled"
    assert slot(result.snapshot, 202)["is_current"] is False
    assert result.snapshot["current_eligibility"]["denominator"] == 2
    assert "BOUT_CANCELLED" in change_types(result)


def test_source_disappearance_alone_does_not_cancel_or_remove_prior_bouts():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "event-refetch-only",
                "event",
                EVENT_ID,
                {"status": "scheduled"},
                observed_at=T2,
                identity_basis="source_alias",
            )
        ],
        initial.snapshot,
    )

    assert len(result.snapshot["bouts"]) == 3
    assert len([item for item in result.snapshot["card_slots"] if item["is_current"]]) == 3
    assert result.snapshot["snapshot_revision"] == 1


def test_explicit_replacement_uses_new_id_and_retains_lineage():
    initial = normalize_card_data_v1(base_observations())
    new_fighters = fighters(204)

    result = normalize_card_data_v1(
        [
            observation(
                "replacement-bout",
                "bout",
                204,
                {
                    "source_ids": {"espn_competition_id": "900201"},
                    "replaces_bout_id": 201,
                    "fighters": new_fighters,
                    "status": "scheduled",
                    "scheduled_rounds": 5,
                },
                source_kind="espn_detail",
                observed_at=T2,
                identity_basis="source_competition_id",
            ),
            observation(
                "replacement-slot",
                "slot",
                204,
                {
                    "is_current": True,
                    "card_section": "main",
                    "order_overall": 1,
                    "scheduled_start_time_utc": "2026-08-16T02:00:00Z",
                    "automatic_lock_time_utc": "2026-08-16T02:00:00Z",
                },
                source_kind="espn_detail",
                observed_at=T2,
                identity_basis="source_competition_id",
            ),
        ],
        initial.snapshot,
    )
    old = bout(result.snapshot, 201)
    new = bout(result.snapshot, 204)

    assert old["status"] == "replaced"
    assert old["replaced_by_bout_id"] == 204
    assert slot(result.snapshot, 201)["is_current"] is False
    assert new["lineage_id"] == old["lineage_id"]
    assert new["matchup_revision"] == old["matchup_revision"] + 1
    assert "MATCHUP_REPLACED" in change_types(result)


def test_changed_fighter_set_cannot_reuse_existing_bout_id():
    initial = normalize_card_data_v1(base_observations())
    old_fighters = copy.deepcopy(bout(initial.snapshot)["fighters"])

    result = normalize_card_data_v1(
        [
            observation(
                "illegal-id-reuse",
                "bout",
                201,
                {"fighters": fighters(999), "weight_class": "Heavyweight"},
                source_kind="espn_detail",
                observed_at=T2,
                identity_basis="source_competition_id",
            )
        ],
        initial.snapshot,
    )

    assert bout(result.snapshot)["fighters"] == old_fighters
    assert bout(result.snapshot)["weight_class"] == "Lightweight"
    assert "MATCHUP_ID_REUSE_FORBIDDEN" in quarantine_codes(result)


def test_new_blocking_conflict_versions_quality_and_eligibility_changes():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "conflict-after-a",
                "bout",
                201,
                {"weight_class": "Welterweight"},
                source_kind="espn_detail",
                observed_at=T2,
            ),
            observation(
                "conflict-after-b",
                "bout",
                201,
                {"weight_class": "Middleweight"},
                source_kind="espn_detail",
                observed_at=T2,
            ),
        ],
        initial.snapshot,
    )

    assert bout(result.snapshot)["weight_class"] == "Lightweight"
    assert result.snapshot["snapshot_revision"] == 2
    assert result.snapshot["current_eligibility"]["denominator"] == 2
    assert {"ELIGIBILITY_CHANGED", "QUALITY_CHANGED"} <= change_types(result)


def test_names_only_bout_attachment_is_quarantined():
    initial = normalize_card_data_v1(base_observations())

    result = normalize_card_data_v1(
        [
            observation(
                "names-only-result",
                "result",
                201,
                final_result("submission", 2),
                observed_at=T2,
                identity_basis="names_only",
            )
        ],
        initial.snapshot,
    )

    assert bout(result.snapshot)["result"] is None
    assert "BOUT_IDENTITY_EVIDENCE_INSUFFICIENT" in quarantine_codes(result)


def test_scr003_title_fixture_belt_accolades_never_resolve_title():
    fixture = json.loads(
        (FIXTURES / "title_heavy_missing_explicit_flag.json").read_text(
            encoding="utf-8"
        )
    )
    source_event = fixture["events"][0]
    target_id = int(source_event["competitions"][0]["id"])
    observations = base_observations(admin_titles=False)
    observations.append(
        observation(
            "belt-accolade-suggestion",
            "bout",
            201,
            {"is_title_fight": True, "title_type": "unknown"},
            source_kind="espn_summary",
            observed_at=T2,
            identity_basis="source_competition_id",
            reason=None,
        )
    )

    result = normalize_card_data_v1(observations)

    assert target_id > 0
    assert bout(result.snapshot)["is_title_fight"] is None
    assert result.snapshot["quality"]["capabilities"]["TITLE"] == "blocked"


def test_invalid_boundary_inputs_fail_before_normalization():
    missing_reason = observation(
        "bad-admin",
        "bout",
        201,
        {"status": "cancelled"},
        source_kind="admin_override",
    )

    with pytest.raises(NormalizationInputError, match="require a reason"):
        normalize_card_data_v1([missing_reason])
    with pytest.raises(NormalizationInputError, match="Duplicate observation_id"):
        normalize_card_data_v1([base_observations()[0], base_observations()[0]])


def test_module_has_no_database_network_or_writer_imports():
    source = (
        Path(__file__).parents[1]
        / "tapology_scraper"
        / "card_data_normalizer.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not imported & {
        "pymongo",
        "motor.motor_asyncio",
        "requests",
        "scrapy",
        "admin_controller",
    }
    lowered = source.lower()
    for forbidden in (
        "update_one(",
        "insert_one(",
        "delete_one(",
    ):
        assert forbidden not in lowered
