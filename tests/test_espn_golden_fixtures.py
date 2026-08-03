import json
from collections import Counter
from pathlib import Path

import pytest

from tapology_scraper.espn_etl import (
    event_status,
    infer_competition_sections,
    map_competitors_to_corners,
    transform_competition_metadata,
    transform_result,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "espn" / "card_data_v1"
MANIFEST = json.loads((FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))


def load_fixture(filename: str) -> dict:
    return json.loads((FIXTURE_DIR / filename).read_text(encoding="utf-8"))


def load_event(filename: str) -> dict:
    payload = load_fixture(filename)
    assert payload["_fixture"]["sanitized"] is True
    assert len(payload["events"]) == 1
    return payload["events"][0]


def scenario(scenario_id: str) -> dict:
    return next(item for item in MANIFEST["scenarios"] if item["id"] == scenario_id)


def competition_by_id(event: dict, competition_id: str) -> dict:
    return next(
        item
        for item in event["competitions"]
        if str(item["id"]) == competition_id
    )


def all_keys(value):
    if isinstance(value, dict):
        for key, nested in value.items():
            yield key
            yield from all_keys(nested)
    elif isinstance(value, list):
        for item in value:
            yield from all_keys(item)


def normalized_result(competition: dict) -> dict:
    result = transform_result(
        competition,
        map_competitors_to_corners(competition),
    )
    assert result is not None
    return {
        "outcome": result["outcome"],
        "method": result["method"],
        "round": result["round"],
    }


def test_manifest_references_every_json_fixture_once():
    referenced = [
        filename
        for item in MANIFEST["scenarios"]
        for filename in item["files"]
    ]
    present = sorted(
        path.name
        for path in FIXTURE_DIR.glob("*.json")
        if path.name != "manifest.json"
    )

    assert MANIFEST["sanitized"] is True
    assert len(referenced) == len(set(referenced))
    assert sorted(referenced) == present


@pytest.mark.parametrize(
    "filename",
    [
        filename
        for item in MANIFEST["scenarios"]
        for filename in item["files"]
    ],
)
def test_fixture_identity_is_synthetic_and_payload_is_reduced(filename):
    payload = load_fixture(filename)
    event = load_event(filename)

    assert str(event["id"]).startswith("9901")
    assert event["name"].startswith("UFC")
    assert "Fixture" in event["name"]
    assert payload["_fixture"]["scenario_id"]

    for competition in event["competitions"]:
        assert str(competition["id"]).startswith("991")
        assert len(competition["competitors"]) == 2
        for competitor in competition["competitors"]:
            assert str(competitor["id"]).startswith("992")
            assert competitor["athlete"]["displayName"].startswith("Fixture")

    forbidden = {"$ref", "guid", "links", "images", "headshot", "odds"}
    assert forbidden.isdisjoint(set(all_keys(payload)))


@pytest.mark.parametrize(
    ("scenario_id", "filename"),
    [
        ("fight_night_two_sections", "fight_night_two_sections.json"),
        ("numbered_three_sections", "numbered_three_sections.json"),
    ],
)
def test_timestamp_groups_infer_expected_sections(scenario_id, filename):
    event = load_event(filename)
    expected = scenario(scenario_id)["expectations"]
    sections = infer_competition_sections(event["competitions"])

    assert len(event["competitions"]) == expected["competition_count"]
    assert len({item["date"] for item in event["competitions"]}) == expected[
        "time_group_count"
    ]
    assert Counter(sections.values()) == expected["inferred_section_counts"]
    assert event_status(event) == expected["event_status"]


def test_title_heavy_fixture_preserves_missing_explicit_title_signal():
    event = load_event("title_heavy_missing_explicit_flag.json")
    expected = scenario("title_heavy_missing_explicit_flag")["expectations"]
    competitions = event["competitions"]

    belt_competitions = [
        competition
        for competition in competitions
        if any(
            accolade.get("type") == "Belt"
            for competitor in competition["competitors"]
            for accolade in competitor["athlete"].get("accolades", [])
        )
    ]

    assert len(competitions) == expected["competition_count"]
    assert len(belt_competitions) == expected["belt_accolade_competition_count"]
    assert sum("titleFight" in item for item in competitions) == expected[
        "explicit_title_fight_field_count"
    ]
    assert sum(bool(item.get("cardSegment")) for item in competitions) == expected[
        "explicit_card_segment_count"
    ]
    assert sum("matchNumber" in item for item in competitions) == expected[
        "explicit_match_number_count"
    ]
    assert {
        transform_competition_metadata(item)["card_section"]
        for item in competitions
    } == {"early_prelim", "prelim", "main"}


def test_card_change_pair_records_disappearance_and_reorder():
    before = load_event("card_change_before.json")
    after = load_event("card_change_after.json")
    expected = scenario("card_change_cancellation_reorder")["expectations"]
    before_order = [str(item["id"]) for item in before["competitions"]]
    after_order = [str(item["id"]) for item in after["competitions"]]

    assert before["id"] == after["id"]
    assert before_order == expected["before_competition_order"]
    assert after_order == expected["after_competition_order"]
    assert sorted(set(before_order) - set(after_order)) == expected[
        "removed_competition_ids"
    ]


def test_replacement_pair_reuses_competition_but_changes_fighter_set():
    before = load_event("replacement_before.json")
    after = load_event("replacement_after.json")
    expected = scenario("fighter_replacement_reused_competition")["expectations"]
    before_competition = competition_by_id(before, expected["competition_id"])
    after_competition = competition_by_id(after, expected["competition_id"])

    before_fighters = sorted(str(item["id"]) for item in before_competition["competitors"])
    after_fighters = sorted(str(item["id"]) for item in after_competition["competitors"])

    assert before_fighters == sorted(expected["before_fighter_ids"])
    assert after_fighters == sorted(expected["after_fighter_ids"])
    assert before_fighters != after_fighters


def test_result_outcomes_match_current_espn_normalization_inputs():
    event = load_event("result_outcomes.json")
    expected = scenario("result_outcomes")["expectations"]["normalized"]

    actual = {
        str(competition["id"]): normalized_result(competition)
        for competition in event["competitions"]
    }

    assert actual == expected


def test_result_correction_pair_has_same_identity_and_changed_result():
    before = load_event("result_correction_before.json")
    after = load_event("result_correction_after.json")
    expected = scenario("result_correction")["expectations"]
    before_competition = competition_by_id(before, expected["competition_id"])
    after_competition = competition_by_id(after, expected["competition_id"])

    assert before["id"] == after["id"]
    assert before_competition["id"] == after_competition["id"]
    assert normalized_result(before_competition) == expected["before"]
    assert normalized_result(after_competition) == expected["after"]
