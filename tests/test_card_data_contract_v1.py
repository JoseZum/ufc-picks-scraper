import copy
import json
from collections import Counter
from pathlib import Path

import pytest

from tapology_scraper.card_data_contract import (
    CardDataValidationError,
    validate_card_data_v1,
    validate_card_data_v1_or_raise,
)
from tapology_scraper.espn_etl import infer_competition_sections


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "espn"
    / "card_data_v1"
    / "numbered_three_sections.json"
)
OBSERVED_AT = "2026-07-31T18:00:00Z"


def build_valid_snapshot() -> dict:
    """Test-only adapter from the SCR-003 ESPN fixture into the V1 shape."""

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    source_event = payload["events"][0]
    competitions = source_event["competitions"]
    sections = infer_competition_sections(competitions)
    section_counts = Counter(sections.values())
    section_indexes = Counter()
    event_id = int(source_event["id"])
    snapshot_revision = 1

    bouts = []
    slots = []
    for index, competition in enumerate(competitions):
        bout_id = int(competition["id"])
        section = sections[str(competition["id"])]
        section_indexes[section] += 1
        order_overall = len(competitions) - index
        order_section = section_counts[section] - section_indexes[section] + 1
        role = (
            "main_event"
            if order_overall == 1
            else "co_main"
            if order_overall == 2
            else "regular"
        )
        fighters = [
            {
                "fighter_id": f"ftr_{competitor['athlete']['id']}",
                "display_name": competitor["athlete"]["displayName"],
                "corner": "red" if competitor["order"] == 1 else "blue",
                "source_ids": {
                    "espn_athlete_id": str(competitor["athlete"]["id"])
                },
                "identity_confidence": "exact_source",
            }
            for competitor in competition["competitors"]
        ]
        bouts.append(
            {
                "bout_id": bout_id,
                "event_id": event_id,
                "source_ids": {"espn_competition_id": str(competition["id"])},
                "lineage_id": f"lineage_{bout_id}",
                "matchup_revision": 1,
                "replaces_bout_id": None,
                "replaced_by_bout_id": None,
                "status": "scheduled",
                "fighters": fighters,
                "weight_class": competition["type"]["text"],
                "gender": "unknown",
                "scheduled_rounds": competition["format"]["regulation"]["periods"],
                "is_title_fight": None,
                "title_type": "unknown",
                "result_revision": 0,
                "result": None,
                "source_updated_at": OBSERVED_AT,
                "canonical_updated_at": OBSERVED_AT,
                "evidence": {},
            }
        )
        slots.append(
            {
                "slot_id": f"{event_id}:{bout_id}",
                "event_id": event_id,
                "bout_id": bout_id,
                "is_current": True,
                "card_section": section,
                "order_overall": order_overall,
                "order_section": order_section,
                "role": role,
                "scheduled_start_time_utc": competition["date"],
                "automatic_lock_time_utc": competition["date"],
                "structure_revision": 1,
                "evidence": {},
            }
        )

    section_times = {
        section: min(
            competition["date"]
            for competition in competitions
            if sections[str(competition["id"])] == section
        )
        for section in sorted(section_counts)
    }
    eligible_targets = [
        {"bout_id": bout["bout_id"], "matchup_revision": 1}
        for bout in sorted(bouts, key=lambda item: item["bout_id"])
    ]
    main_event_bout_id = next(
        slot["bout_id"] for slot in slots if slot["role"] == "main_event"
    )

    return {
        "contract_version": "card-data/v1",
        "normalizer_version": "1.0.0",
        "snapshot_id": f"{event_id}:{snapshot_revision}",
        "snapshot_revision": snapshot_revision,
        "generated_at": OBSERVED_AT,
        "source_run": {
            "run_id": "fixture-run-001",
            "observed_at": OBSERVED_AT,
            "sources": [
                {
                    "source": "espn_summary",
                    "source_event_id": str(source_event["id"]),
                    "observed_at": OBSERVED_AT,
                    "payload_hash": "sha256:fixture-numbered-three-sections",
                }
            ],
            "previous_snapshot_revision": None,
        },
        "event": {
            "event_id": event_id,
            "source_ids": {"espn_event_id": str(source_event["id"])},
            "promotion": "UFC",
            "name": source_event["name"],
            "subtitle": None,
            "official_event_date": "2026-08-15",
            "official_date_timezone": "America/New_York",
            "month_key": "2026-08",
            "status": "scheduled",
            "card_start_time_utc": min(section_times.values()),
            "picks_lock_time_utc": min(section_times.values()),
            "section_start_times_utc": section_times,
            "section_lock_times_utc": dict(section_times),
            "first_final_result_at": None,
            "main_event_bout_id": main_event_bout_id,
            "listed_bout_count": len(slots),
            "mission_eligible_bout_count": len(eligible_targets),
            "lifecycle_revision": 1,
            "structure_revision": 1,
            "timing_revision": 1,
            "created_at": OBSERVED_AT,
            "canonical_updated_at": OBSERVED_AT,
            "evidence": {},
        },
        "bouts": bouts,
        "card_slots": slots,
        "current_eligibility": {
            "eligibility_snapshot_id": f"elig_{event_id}_{snapshot_revision}",
            "kind": "current",
            "event_id": event_id,
            "card_snapshot_revision": snapshot_revision,
            "created_at": OBSERVED_AT,
            "eligible_targets": eligible_targets,
            "excluded_targets": [],
            "denominator": len(eligible_targets),
            "fingerprint": "sha256:fixture-eligibility-v1",
        },
        "quality": {
            "overall": "degraded",
            "capabilities": {
                "EVT": "ready",
                "EVT_DATE": "ready",
                "BOUT": "ready",
                "ELIG": "ready",
                "STRUCT": "ready",
                "TITLE": "blocked",
                "RES": "pending",
            },
            "issues": [
                {
                    "code": "TITLE_STATUS_UNKNOWN",
                    "severity": "warning",
                    "scope_type": "event",
                    "scope_id": str(event_id),
                    "field": "bouts.is_title_fight",
                    "message": "ESPN fixture has no explicit competition title flag.",
                    "blocks_capabilities": ["TITLE"],
                }
            ],
        },
    }


def codes(snapshot: dict) -> set[str]:
    return set(validate_card_data_v1(snapshot).codes)


def set_final_result(snapshot: dict, bout_index: int = -1) -> dict:
    bout = snapshot["bouts"][bout_index]
    red = next(fighter for fighter in bout["fighters"] if fighter["corner"] == "red")
    bout["status"] = "completed"
    bout["result_revision"] = 1
    bout["result"] = {
        "revision": 1,
        "status": "final",
        "outcome": "red_win",
        "winner_fighter_id": red["fighter_id"],
        "method_family": "ko_tko",
        "method_detail": "Fixture TKO",
        "ending_round": 2,
        "ending_time_seconds": 141,
        "recorded_at": "2026-08-16T02:30:00Z",
        "source_updated_at": "2026-08-16T02:31:00Z",
        "corrected_at": None,
        "correction_reason": None,
        "evidence": {},
    }
    return bout


def test_valid_fixture_snapshot_passes_without_mutating_input():
    snapshot = build_valid_snapshot()
    before = copy.deepcopy(snapshot)

    validation = validate_card_data_v1(snapshot)

    assert validation.is_valid
    assert validation.issues == ()
    assert snapshot == before


def test_issue_order_and_serialization_are_deterministic():
    snapshot = build_valid_snapshot()
    snapshot["event"]["month_key"] = "2026-09"
    snapshot["card_slots"][0]["order_overall"] = 1

    first = validate_card_data_v1(snapshot)
    second = validate_card_data_v1(copy.deepcopy(snapshot))

    assert first == second
    assert first.as_dict() == second.as_dict()
    assert json.dumps(first.as_dict(), sort_keys=True) == json.dumps(
        second.as_dict(), sort_keys=True
    )


def test_duplicate_overall_order_is_rejected():
    snapshot = build_valid_snapshot()
    snapshot["card_slots"][0]["order_overall"] = snapshot["card_slots"][1][
        "order_overall"
    ]

    assert "ORDER_DUPLICATE" in codes(snapshot)


def test_section_order_must_be_unique_and_contiguous():
    snapshot = build_valid_snapshot()
    same_section = [
        slot
        for slot in snapshot["card_slots"]
        if slot["card_section"] == "early_prelim"
    ]
    same_section[0]["order_section"] = same_section[1]["order_section"]

    validation_codes = codes(snapshot)
    assert "SECTION_ORDER_DUPLICATE" in validation_codes
    assert "SECTION_ORDER_GAP" in validation_codes


def test_unknown_section_is_quarantined_not_defaulted():
    snapshot = build_valid_snapshot()
    snapshot["card_slots"][0]["card_section"] = "featured_prelim"

    assert "SECTION_UNKNOWN" in codes(snapshot)
    assert snapshot["card_slots"][0]["card_section"] == "featured_prelim"


def test_multiple_main_events_are_rejected():
    snapshot = build_valid_snapshot()
    snapshot["card_slots"][-2]["role"] = "main_event"

    assert "MAIN_EVENT_MULTIPLE" in codes(snapshot)


def test_missing_main_event_is_rejected_for_nonempty_card():
    snapshot = build_valid_snapshot()
    for slot in snapshot["card_slots"]:
        slot["role"] = "regular"
    snapshot["event"]["main_event_bout_id"] = None

    assert "MAIN_EVENT_MISSING" in codes(snapshot)


def test_invalid_co_main_role_is_rejected():
    snapshot = build_valid_snapshot()
    existing_co_main = next(
        slot for slot in snapshot["card_slots"] if slot["role"] == "co_main"
    )
    existing_co_main["role"] = "regular"
    snapshot["card_slots"][0]["role"] = "co_main"

    assert "CO_MAIN_INVALID" in codes(snapshot)


def test_completed_bout_requires_final_result():
    snapshot = build_valid_snapshot()
    snapshot["bouts"][0]["status"] = "completed"

    assert "RESULT_NOT_FINAL" in codes(snapshot)


def test_completed_event_requires_every_current_bout_completed_and_one_final():
    snapshot = build_valid_snapshot()
    snapshot["event"]["status"] = "completed"

    assert "EVENT_COMPLETION_INCOMPLETE" in codes(snapshot)


def test_result_winner_must_match_outcome_corner():
    snapshot = build_valid_snapshot()
    bout = set_final_result(snapshot)
    blue = next(fighter for fighter in bout["fighters"] if fighter["corner"] == "blue")
    bout["result"]["winner_fighter_id"] = blue["fighter_id"]

    assert "RESULT_WINNER_INVALID" in codes(snapshot)


def test_result_revision_must_match_bout_revision():
    snapshot = build_valid_snapshot()
    bout = set_final_result(snapshot)
    bout["result"]["revision"] = 2

    assert "RESULT_REVISION_MISMATCH" in codes(snapshot)


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        ("ending_round", 6, "RESULT_ROUND_INVALID"),
        ("ending_time_seconds", 301, "RESULT_TIME_INVALID"),
        ("method_family", "doctor_stoppage", "RESULT_METHOD_UNKNOWN"),
    ],
)
def test_result_ranges_and_method_enum_are_enforced(field, value, expected_code):
    snapshot = build_valid_snapshot()
    bout = set_final_result(snapshot)
    bout["result"][field] = value

    assert expected_code in codes(snapshot)


def test_title_boolean_and_type_must_be_consistent():
    snapshot = build_valid_snapshot()
    snapshot["bouts"][0]["is_title_fight"] = False
    snapshot["bouts"][0]["title_type"] = "undisputed"

    assert "TITLE_TYPE_CONFLICT" in codes(snapshot)


def test_eligibility_denominator_must_match_unique_targets():
    snapshot = build_valid_snapshot()
    snapshot["current_eligibility"]["denominator"] += 1
    snapshot["event"]["mission_eligible_bout_count"] += 1

    assert "ELIGIBILITY_DENOMINATOR_MISMATCH" in codes(snapshot)


def test_eligibility_target_requires_exact_current_matchup_revision():
    snapshot = build_valid_snapshot()
    snapshot["current_eligibility"]["eligible_targets"][0][
        "matchup_revision"
    ] = 2

    assert "ELIGIBILITY_TARGET_INVALID" in codes(snapshot)


def test_current_eligibility_cannot_masquerade_as_a_frozen_snapshot():
    snapshot = build_valid_snapshot()
    snapshot["current_eligibility"]["kind"] = "mission_assignment"

    assert "ELIGIBILITY_TARGET_INVALID" in codes(snapshot)


def test_postponed_bout_must_be_excluded_from_current_eligible_targets():
    snapshot = build_valid_snapshot()
    snapshot["bouts"][0]["status"] = "postponed"

    assert "ELIGIBILITY_TARGET_INVALID" in codes(snapshot)


def test_month_key_must_match_official_date():
    snapshot = build_valid_snapshot()
    snapshot["event"]["month_key"] = "2026-09"

    assert "MONTH_KEY_MISMATCH" in codes(snapshot)


def test_slot_lock_cannot_be_after_its_start():
    snapshot = build_valid_snapshot()
    snapshot["card_slots"][-1]["automatic_lock_time_utc"] = (
        "2026-08-16T01:01:00Z"
    )

    assert "LOCK_AFTER_START" in codes(snapshot)


def test_event_listed_count_must_match_current_slots():
    snapshot = build_valid_snapshot()
    snapshot["event"]["listed_bout_count"] -= 1

    assert "COUNT_MISMATCH" in codes(snapshot)


def test_slot_relationships_must_reference_the_same_event_and_bout():
    snapshot = build_valid_snapshot()
    snapshot["card_slots"][0]["slot_id"] = "wrong:slot"

    assert "SLOT_BOUT_MISMATCH" in codes(snapshot)


def test_strict_boundary_raises_with_structured_validation():
    snapshot = build_valid_snapshot()
    snapshot["event"]["month_key"] = "2026-09"

    with pytest.raises(CardDataValidationError) as error:
        validate_card_data_v1_or_raise(snapshot)

    assert "MONTH_KEY_MISMATCH" in error.value.validation.codes
    assert "MONTH_KEY_MISMATCH" in str(error.value)


def test_strict_boundary_returns_none_for_valid_snapshot():
    assert validate_card_data_v1_or_raise(build_valid_snapshot()) is None
