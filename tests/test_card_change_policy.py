import ast
import copy
from pathlib import Path

import pytest

from tapology_scraper.card_change_policy import (
    AUTO_REMOVAL_REQUIRED_MISSES,
    BoutPresenceState,
    CardChangePolicyInputError,
    apply_card_change_policy,
)
from tapology_scraper.card_data_contract import validate_card_data_v1
from tapology_scraper.card_data_normalizer import normalize_card_data_v1
from tapology_scraper.slot_reconciliation import plan_slot_reconciliation


EVENT_ID = 83001
INITIAL_AT = "2026-08-01T12:00:00Z"
COVERAGE_1 = "2026-08-22T10:00:00Z"
COVERAGE_2 = "2026-08-22T10:20:00Z"
COVERAGE_3 = "2026-08-22T10:31:00Z"
LOCK_AT = "2026-08-22T22:00:00Z"


def observation(
    observation_id,
    entity_type,
    entity_id,
    values,
    *,
    source_kind="espn_detail",
    observed_at=INITIAL_AT,
    reason=None,
    identity_basis="source_competition_id",
    clear_fields=(),
):
    return {
        "observation_id": observation_id,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": EVENT_ID,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"fixture:{observation_id}",
        "source_event_id": "600083001",
        "values": values,
        "clear_fields": list(clear_fields),
        "identity_basis": identity_basis,
        "reason": reason,
        "payload_hash": f"sha256:{observation_id}",
    }


def fighter_pair(seed, *, swap=False):
    red = {
        "fighter_id": f"fighter_{seed}_red",
        "display_name": f"Fixture {seed} Red",
        "corner": "red",
        "source_ids": {"espn_athlete_id": f"{seed}1"},
        "identity_confidence": "exact_source",
    }
    blue = {
        "fighter_id": f"fighter_{seed}_blue",
        "display_name": f"Fixture {seed} Blue",
        "corner": "blue",
        "source_ids": {"espn_athlete_id": f"{seed}2"},
        "identity_confidence": "exact_source",
    }
    if not swap:
        return [red, blue]
    red["corner"], blue["corner"] = "blue", "red"
    return [blue, red]


def build_snapshot():
    observations = [
        observation(
            "event",
            "event",
            EVENT_ID,
            {
                "source_ids": {"espn_event_id": "600083001"},
                "promotion": "UFC",
                "name": "UFC Fixture: Cedar vs. Quartz",
                "official_event_date": "2026-08-22",
                "official_date_timezone": "America/New_York",
                "status": "scheduled",
            },
            source_kind="espn_summary",
            identity_basis="source_alias",
        )
    ]
    layout = (
        (401, "main", 1, "2026-08-23T02:00:00Z"),
        (402, "prelim", 2, "2026-08-23T00:00:00Z"),
        (403, "early_prelim", 3, LOCK_AT),
    )
    for bout_id, section, order, start in layout:
        observations.extend(
            [
                observation(
                    f"bout-{bout_id}",
                    "bout",
                    bout_id,
                    {
                        "source_ids": {
                            "espn_competition_id": f"910{bout_id}"
                        },
                        "fighters": fighter_pair(bout_id),
                        "weight_class": "Lightweight",
                        "gender": "male",
                        "scheduled_rounds": 5 if bout_id == 401 else 3,
                        "status": "scheduled",
                    },
                ),
                observation(
                    f"slot-{bout_id}",
                    "slot",
                    bout_id,
                    {
                        "is_current": True,
                        "card_section": section,
                        "order_overall": order,
                        "scheduled_start_time_utc": start,
                        "automatic_lock_time_utc": start,
                    },
                ),
                observation(
                    f"title-{bout_id}",
                    "bout",
                    bout_id,
                    {
                        "is_title_fight": False,
                        "is_bmf_title_fight": False,
                        "title_type": "none",
                    },
                    source_kind="admin_override",
                    reason="Admin explicitly marked a non-title bout.",
                    identity_basis="admin",
                ),
            ]
        )
    return normalize_card_data_v1(observations).snapshot


def refetch(observed_at, suffix):
    return observation(
        f"event-refetch-{suffix}",
        "event",
        EVENT_ID,
        {"status": "scheduled"},
        source_kind="espn_summary",
        observed_at=observed_at,
        identity_basis="source_alias",
    )


def coverage(observed_at, suffix, *, present=(401, 403), kind="complete"):
    return {
        "coverage_id": f"coverage-{suffix}",
        "source_kind": "espn_detail",
        "observed_at": observed_at,
        "event_id": EVENT_ID,
        "source_event_id": "600083001",
        "source_ref": f"fixture:coverage-{suffix}",
        "payload_hash": f"sha256:coverage-{suffix}",
        "coverage_kind": kind,
        "present_bout_ids": list(present),
    }


def bout(snapshot, bout_id=402):
    return next(item for item in snapshot["bouts"] if item["bout_id"] == bout_id)


def slot(snapshot, bout_id=402):
    return next(
        item for item in snapshot["card_slots"] if item["bout_id"] == bout_id
    )


def policy_types(result):
    return {item["type"] for item in result.policy_change_set["changes"]}


def finding_codes(result):
    return {item.code for item in result.findings}


def missing_run(snapshot, states, observed_at, suffix, **coverage_options):
    return apply_card_change_policy(
        [refetch(observed_at, suffix)],
        snapshot,
        coverage=coverage(observed_at, suffix, **coverage_options),
        previous_presence_states=states,
    )


def test_single_complete_absence_is_deterministic_and_preserves_inputs():
    snapshot = build_snapshot()
    source_observations = [refetch(COVERAGE_1, "one")]
    source_coverage = coverage(COVERAGE_1, "one")
    before_snapshot = copy.deepcopy(snapshot)
    before_observations = copy.deepcopy(source_observations)
    before_coverage = copy.deepcopy(source_coverage)

    first = apply_card_change_policy(
        source_observations, snapshot, coverage=source_coverage
    )
    second = apply_card_change_policy(
        list(reversed(source_observations)), snapshot, coverage=source_coverage
    )

    assert first.as_dict() == second.as_dict()
    assert snapshot == before_snapshot
    assert source_observations == before_observations
    assert source_coverage == before_coverage
    assert bout(first.snapshot)["status"] == "scheduled"
    assert slot(first.snapshot)["is_current"] is True
    assert first.snapshot["current_eligibility"]["denominator"] == 3
    assert first.presence_states[0].consecutive_complete_misses == 1
    assert "UNEXPLAINED_REMOVAL" in finding_codes(first)
    assert "BOUT_MISSING_PENDING" in policy_types(first)


def test_partial_payload_never_advances_missing_evidence():
    snapshot = build_snapshot()

    result = missing_run(
        snapshot, (), COVERAGE_1, "partial", kind="partial"
    )

    assert result.presence_states == ()
    assert finding_codes(result) == {"AUTHORITATIVE_CARD_INCOMPLETE"}
    assert bout(result.snapshot)["status"] == "scheduled"
    assert result.snapshot["current_eligibility"]["denominator"] == 3


def test_non_authoritative_complete_payload_cannot_advance_counter():
    snapshot = build_snapshot()
    source_coverage = coverage(COVERAGE_1, "tapology")
    source_coverage["source_kind"] = "tapology_explicit"

    result = apply_card_change_policy(
        [refetch(COVERAGE_1, "tapology")],
        snapshot,
        coverage=source_coverage,
    )

    assert result.presence_states == ()
    assert finding_codes(result) == {"UNEXPLAINED_REMOVAL"}


def test_replaying_same_coverage_is_idempotent_and_does_not_increment():
    snapshot = build_snapshot()
    first = missing_run(snapshot, (), COVERAGE_1, "replay")

    replay = missing_run(
        first.snapshot,
        first.presence_states,
        COVERAGE_1,
        "replay",
    )

    assert replay.presence_states == first.presence_states
    assert bout(replay.snapshot)["status"] == "scheduled"


def test_same_coverage_id_with_different_payload_is_rejected():
    snapshot = build_snapshot()
    first = missing_run(snapshot, (), COVERAGE_1, "collision")
    conflicting = coverage(COVERAGE_1, "collision")
    conflicting["payload_hash"] = "sha256:different"

    with pytest.raises(CardChangePolicyInputError, match="coverage_id"):
        apply_card_change_policy(
            [refetch(COVERAGE_1, "collision")],
            first.snapshot,
            coverage=conflicting,
            previous_presence_states=first.presence_states,
        )


def test_stale_coverage_does_not_advance_counter():
    snapshot = build_snapshot()
    first = missing_run(snapshot, (), COVERAGE_2, "newer")

    stale = missing_run(
        first.snapshot,
        first.presence_states,
        COVERAGE_1,
        "older",
    )

    assert stale.presence_states == first.presence_states
    assert "STALE_COVERAGE_IGNORED" in finding_codes(stale)


def test_presence_restoration_resets_pending_state_without_card_mutation():
    snapshot = build_snapshot()
    missing = missing_run(snapshot, (), COVERAGE_1, "missing")

    restored = missing_run(
        missing.snapshot,
        missing.presence_states,
        COVERAGE_2,
        "restored",
        present=(401, 402, 403),
    )

    assert restored.presence_states == ()
    assert "BOUT_PRESENCE_RESTORED" in policy_types(restored)
    assert restored.snapshot["snapshot_revision"] == snapshot["snapshot_revision"]
    assert restored.snapshot["current_eligibility"]["denominator"] == 3


def test_three_complete_misses_over_thirty_minutes_confirm_removal():
    snapshot = build_snapshot()
    first = missing_run(snapshot, (), COVERAGE_1, "first")
    second = missing_run(
        first.snapshot, first.presence_states, COVERAGE_2, "second"
    )
    third = missing_run(
        second.snapshot, second.presence_states, COVERAGE_3, "third"
    )

    assert AUTO_REMOVAL_REQUIRED_MISSES == 3
    assert bout(third.snapshot)["status"] == "cancelled"
    assert slot(third.snapshot)["is_current"] is False
    assert third.snapshot["current_eligibility"]["denominator"] == 2
    assert validate_card_data_v1(third.snapshot).is_valid
    assert {
        "BOUT_CANCELLED",
        "ELIGIBILITY_CHANGED",
        "REMOVAL_CONFIRMED_BY_ABSENCE_POLICY",
        "SLOT_CURRENT_CHANGED",
    } <= policy_types(third)
    eligibility = third.policy_change_set["eligibility"]
    assert eligibility["before_denominator"] == 3
    assert eligibility["after_denominator"] == 2
    assert eligibility["frozen_snapshots_mutated"] is False
    assert eligibility["removed_targets"] == [
        {"bout_id": 402, "matchup_revision": 1, "cause": "CANCELLED"}
    ]


def test_three_fast_misses_wait_until_minimum_span_is_reached():
    snapshot = build_snapshot()
    moments = (
        "2026-08-22T10:00:00Z",
        "2026-08-22T10:10:00Z",
        "2026-08-22T10:20:00Z",
    )
    states = ()
    current = snapshot
    for index, moment in enumerate(moments, start=1):
        result = missing_run(current, states, moment, f"fast-{index}")
        current = result.snapshot
        states = result.presence_states

    assert states[0].consecutive_complete_misses == 3
    assert states[0].disposition == "missing_pending"
    assert bout(current)["status"] == "scheduled"

    confirmed = missing_run(
        current, states, "2026-08-22T10:31:00Z", "fast-4"
    )
    assert confirmed.presence_states[0].consecutive_complete_misses == 4
    assert bout(confirmed.snapshot)["status"] == "cancelled"


def test_absence_outside_seven_day_window_never_counts():
    snapshot = build_snapshot()

    result = missing_run(
        snapshot, (), "2026-08-14T10:00:00Z", "too-early"
    )

    assert result.presence_states[0].consecutive_complete_misses == 0
    assert result.presence_states[0].first_qualifying_missing_at is None
    assert bout(result.snapshot)["status"] == "scheduled"
    finding = next(item for item in result.findings if item.bout_id == 402)
    assert finding.evidence["window_reason"] == "OUTSIDE_PRE_LOCK_WINDOW"


def test_absence_at_or_after_lock_requires_explicit_status():
    snapshot = build_snapshot()
    result = missing_run(snapshot, (), LOCK_AT, "locked")

    assert result.presence_states[0].consecutive_complete_misses == 0
    assert result.presence_states[0].disposition == "missing_review_required"
    assert bout(result.snapshot)["status"] == "scheduled"
    finding = next(item for item in result.findings if item.bout_id == 402)
    assert finding.evidence["window_reason"] == "CARD_ALREADY_LOCKED"


def test_explicit_source_cancellation_applies_immediately_without_coverage():
    snapshot = build_snapshot()

    result = apply_card_change_policy(
        [
            observation(
                "explicit-cancel",
                "bout",
                402,
                {"status": "cancelled"},
                observed_at=COVERAGE_1,
            )
        ],
        snapshot,
    )

    assert bout(result.snapshot)["status"] == "cancelled"
    assert slot(result.snapshot)["is_current"] is False
    cancelled = next(
        item
        for item in result.policy_change_set["changes"]
        if item["type"] == "BOUT_CANCELLED"
    )
    assert {
        "VOID_DIRECT_TARGETS",
        "KEEP_FROZEN_DENOMINATORS",
    } <= set(cancelled["policy_effects"])


def test_explicit_change_clears_previous_missing_counter_without_coverage():
    snapshot = build_snapshot()
    pending = missing_run(snapshot, (), COVERAGE_1, "pending-before-explicit")

    cancelled = apply_card_change_policy(
        [
            observation(
                "explicit-after-pending",
                "bout",
                402,
                {"status": "cancelled"},
                observed_at=COVERAGE_2,
            )
        ],
        pending.snapshot,
        previous_presence_states=pending.presence_states,
    )

    assert cancelled.presence_states == ()
    assert "MISSING_EVIDENCE_RESOLVED_EXPLICITLY" in policy_types(cancelled)
    assert bout(cancelled.snapshot)["status"] == "cancelled"


def test_explicit_postponement_removes_current_eligibility_and_voids_direct_targets():
    snapshot = build_snapshot()

    result = apply_card_change_policy(
        [
            observation(
                "explicit-postpone",
                "bout",
                402,
                {"status": "postponed"},
                observed_at=COVERAGE_1,
            )
        ],
        snapshot,
    )

    assert bout(result.snapshot)["status"] == "postponed"
    assert result.snapshot["current_eligibility"]["denominator"] == 2
    postponed = next(
        item
        for item in result.policy_change_set["changes"]
        if item["type"] == "BOUT_POSTPONED"
    )
    assert "VOID_DIRECT_TARGETS" in postponed["policy_effects"]


def test_admin_scheduled_override_suppresses_inferred_cancellation():
    initial = build_snapshot()
    admin = normalize_card_data_v1(
        [
            observation(
                "admin-keep-402",
                "bout",
                402,
                {"status": "scheduled"},
                source_kind="admin_override",
                observed_at="2026-08-22T09:00:00Z",
                reason="Admin verified the bout remains scheduled.",
                identity_basis="admin",
            )
        ],
        initial,
    ).snapshot
    first = missing_run(admin, (), COVERAGE_1, "admin-first")
    second = missing_run(
        first.snapshot, first.presence_states, COVERAGE_2, "admin-second"
    )
    third = missing_run(
        second.snapshot, second.presence_states, COVERAGE_3, "admin-third"
    )

    assert bout(third.snapshot)["status"] == "scheduled"
    assert third.presence_states[0].disposition == "missing_review_required"
    assert "AUTO_REMOVAL_SUPPRESSED_BY_HIGHER_AUTHORITY" in finding_codes(third)
    blocked = next(
        item
        for item in third.policy_change_set["changes"]
        if item["type"] == "REMOVAL_CONFIRMATION_BLOCKED_BY_AUTHORITY"
    )
    assert blocked["policy_effects"] == [
        "NO_ELIGIBILITY_CHANGE",
        "RETAIN_CANONICAL_BOUT",
    ]
    assert "VOID_DIRECT_TARGETS" not in blocked["policy_effects"]


def test_explicit_replacement_retires_old_target_and_never_copies_it():
    snapshot = build_snapshot()
    replacement_id = 404

    result = apply_card_change_policy(
        [
            observation(
                "replacement-bout",
                "bout",
                replacement_id,
                {
                    "source_ids": {"espn_competition_id": "910402"},
                    "replaces_bout_id": 402,
                    "fighters": fighter_pair(replacement_id),
                    "weight_class": "Lightweight",
                    "gender": "male",
                    "scheduled_rounds": 3,
                    "status": "scheduled",
                },
                observed_at=COVERAGE_1,
            ),
            observation(
                "replacement-slot",
                "slot",
                replacement_id,
                {
                    "is_current": True,
                    "card_section": "prelim",
                    "order_overall": 2,
                    "scheduled_start_time_utc": "2026-08-23T00:00:00Z",
                    "automatic_lock_time_utc": "2026-08-23T00:00:00Z",
                },
                observed_at=COVERAGE_1,
            ),
            observation(
                "replacement-title",
                "bout",
                replacement_id,
                {
                    "is_title_fight": False,
                    "is_bmf_title_fight": False,
                    "title_type": "none",
                },
                source_kind="admin_override",
                observed_at=COVERAGE_1,
                reason="Admin marked replacement as non-title.",
                identity_basis="admin",
            ),
        ],
        snapshot,
    )

    old = bout(result.snapshot, 402)
    new = bout(result.snapshot, replacement_id)
    assert old["status"] == "replaced"
    assert old["replaced_by_bout_id"] == replacement_id
    assert slot(result.snapshot, 402)["is_current"] is False
    assert new["replaces_bout_id"] == 402
    assert new["matchup_revision"] == old["matchup_revision"] + 1
    replacement = next(
        item
        for item in result.policy_change_set["changes"]
        if item["type"] == "MATCHUP_REPLACED"
    )
    assert "DO_NOT_COPY_PICKS_OR_MISSIONS" in replacement["policy_effects"]
    assert {item["bout_id"] for item in result.policy_change_set["eligibility"]["added_targets"]} == {404}
    assert result.policy_change_set["eligibility"]["removed_targets"] == [
        {"bout_id": 402, "matchup_revision": 1, "cause": "REPLACED"}
    ]


def test_corner_swap_is_not_a_replacement_or_direct_target_void():
    snapshot = build_snapshot()

    result = apply_card_change_policy(
        [
            observation(
                "corner-swap",
                "bout",
                402,
                {"fighters": fighter_pair(402, swap=True)},
                observed_at=COVERAGE_1,
            )
        ],
        snapshot,
    )

    assert bout(result.snapshot)["matchup_revision"] == 1
    assert bout(result.snapshot)["status"] == "scheduled"
    assert "MATCHUP_REPLACED" not in policy_types(result)
    assert all(
        "VOID_DIRECT_TARGETS" not in item.get("policy_effects", [])
        for item in result.policy_change_set["changes"]
    )


def test_cancelled_bout_and_historical_slot_cannot_be_silently_revived():
    snapshot = build_snapshot()
    cancelled = apply_card_change_policy(
        [
            observation(
                "cancel-first",
                "bout",
                402,
                {"status": "cancelled"},
                observed_at=COVERAGE_1,
            )
        ],
        snapshot,
    ).snapshot

    revived = apply_card_change_policy(
        [
            observation(
                "illegal-status-revival",
                "bout",
                402,
                {"status": "scheduled"},
                observed_at=COVERAGE_2,
            ),
            observation(
                "illegal-slot-revival",
                "slot",
                402,
                {"is_current": True},
                observed_at=COVERAGE_2,
            ),
        ],
        cancelled,
    )

    assert bout(revived.snapshot)["status"] == "cancelled"
    assert slot(revived.snapshot)["is_current"] is False
    assert {
        "ILLEGAL_BOUT_LIFECYCLE_TRANSITION",
        "TERMINAL_SLOT_REVIVAL_BLOCKED",
    } <= finding_codes(revived)


def test_illegal_lifecycle_jump_is_blocked_but_other_fields_can_update():
    snapshot = build_snapshot()
    completed = normalize_card_data_v1(
        [
            observation(
                "completed-402",
                "bout",
                402,
                {"status": "completed"},
                observed_at=COVERAGE_1,
            ),
            observation(
                "result-402",
                "result",
                402,
                {
                    "outcome": "red_win",
                    "winner_fighter_id": "fighter_402_red",
                    "method_family": "decision",
                    "method_detail": "Unanimous Decision",
                    "ending_round": 3,
                    "ending_time_seconds": 300,
                    "status": "final",
                },
                observed_at=COVERAGE_1,
            ),
        ],
        snapshot,
    ).snapshot

    result = apply_card_change_policy(
        [
            observation(
                "bad-completed-reopen",
                "bout",
                402,
                {"status": "scheduled", "weight_class": "Welterweight"},
                observed_at=COVERAGE_2,
            )
        ],
        completed,
    )

    assert bout(result.snapshot)["status"] == "completed"
    assert bout(result.snapshot)["weight_class"] == "Welterweight"
    assert "ILLEGAL_BOUT_LIFECYCLE_TRANSITION" in finding_codes(result)


def test_event_date_change_keeps_frozen_monthly_membership():
    snapshot = build_snapshot()

    result = apply_card_change_policy(
        [
            observation(
                "admin-date-change",
                "event",
                EVENT_ID,
                {"official_event_date": "2026-09-05"},
                source_kind="admin_override",
                observed_at=COVERAGE_1,
                reason="Admin corrected the official date.",
                identity_basis="admin",
            )
        ],
        snapshot,
    )

    date_change = next(
        item
        for item in result.policy_change_set["changes"]
        if item["type"] == "EVENT_DATE_CHANGED"
    )
    assert result.snapshot["event"]["month_key"] == "2026-09"
    assert "KEEP_FROZEN_MONTHLY_EVENT_SET" in date_change["policy_effects"]


def test_policy_snapshot_reconciles_slots_without_delete_operations():
    snapshot = build_snapshot()
    current_documents = list(
        plan_slot_reconciliation(snapshot, []).desired_documents
    )
    cancelled = apply_card_change_policy(
        [
            observation(
                "cancel-for-slots",
                "bout",
                402,
                {"status": "cancelled"},
                observed_at=COVERAGE_1,
            )
        ],
        snapshot,
    )

    plan = plan_slot_reconciliation(cancelled.snapshot, current_documents)

    assert plan.safe_to_apply
    assert all(operation.action in {"insert", "update"} for operation in plan.operations)
    target = next(operation for operation in plan.operations if operation.bout_id == 402)
    assert target.after["is_current"] is False


def test_invalid_previous_snapshot_and_presence_state_are_rejected():
    snapshot = build_snapshot()
    invalid_snapshot = copy.deepcopy(snapshot)
    invalid_snapshot["event"]["listed_bout_count"] = 99

    with pytest.raises(CardChangePolicyInputError, match="CardDataContractV1"):
        apply_card_change_policy(
            [refetch(COVERAGE_1, "invalid-snapshot")], invalid_snapshot
        )

    bad_state = BoutPresenceState(
        event_id=EVENT_ID,
        bout_id=402,
        disposition="missing_pending",
        consecutive_complete_misses=-1,
        first_qualifying_missing_at=None,
        last_missing_at=COVERAGE_1,
        last_coverage_id="coverage-bad",
        last_payload_hash="sha256:bad",
        source_kind="espn_detail",
    )
    with pytest.raises(CardChangePolicyInputError, match="non-negative"):
        apply_card_change_policy(
            [refetch(COVERAGE_2, "bad-state")],
            snapshot,
            previous_presence_states=(bad_state,),
        )


def test_coverage_must_match_canonical_espn_event_alias():
    snapshot = build_snapshot()
    wrong_event = coverage(COVERAGE_1, "wrong-event")
    wrong_event["source_event_id"] = "600000999"

    with pytest.raises(CardChangePolicyInputError, match="canonical ESPN"):
        apply_card_change_policy(
            [refetch(COVERAGE_1, "wrong-event")],
            snapshot,
            coverage=wrong_event,
        )


def test_module_has_no_database_network_or_current_writer_imports_or_calls():
    source = (
        Path(__file__).parents[1]
        / "tapology_scraper"
        / "card_change_policy.py"
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
        "bulk_write(",
    ):
        assert forbidden not in lowered
