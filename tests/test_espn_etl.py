import unittest
from datetime import date, datetime

from tapology_scraper.espn_etl import (
    age_on_date,
    build_headshot_url,
    calculate_pick_points,
    build_section_times_utc,
    find_bout_match,
    find_event_match,
    infer_competition_sections,
    infer_card_position,
    map_competitors_to_corners,
    normalize_weight_class,
    parse_record_summary,
    transform_athlete_profile,
    transform_athlete_records,
    transform_competition_metadata,
    transform_event,
    transform_result,
)
from tapology_scraper.spiders.espn import (
    event_has_all_results,
    event_is_near,
    has_fight_result,
    mode_collects_competition_details,
    select_ufc_events,
)


def competitor(
    athlete_id: str,
    name: str,
    order: int,
    winner: bool = False,
    record: str = "10-2-0",
) -> dict:
    return {
        "id": athlete_id,
        "order": order,
        "winner": winner,
        "athlete": {
            "displayName": name,
            "links": [
                {
                    "rel": ["playercard", "desktop", "athlete"],
                    "href": f"https://www.espn.com/mma/fighter/_/id/{athlete_id}/test",
                }
            ],
            "flag": {"alt": "Serbia"},
        },
        "records": [{"name": "overall", "type": "total", "summary": record}],
    }


class EspnEtlTests(unittest.TestCase):
    def test_results_pass_collects_details_for_absence_confirmation(self):
        self.assertTrue(mode_collects_competition_details("results"))
        self.assertTrue(mode_collects_competition_details("general"))
        self.assertFalse(mode_collects_competition_details("photos"))

    def test_infers_three_card_sections_from_espn_start_groups(self):
        competitions = [
            {"id": "early-1", "date": "2026-08-15T21:00Z"},
            {"id": "prelim-1", "date": "2026-08-15T23:00Z"},
            {"id": "main-1", "date": "2026-08-16T01:00Z"},
        ]

        sections = infer_competition_sections(competitions)
        self.assertEqual(
            sections,
            {
                "early-1": "early_prelim",
                "prelim-1": "prelim",
                "main-1": "main",
            },
        )
        self.assertEqual(
            build_section_times_utc(competitions, sections),
            {
                "early_prelim": datetime(2026, 8, 15, 21, 0),
                "prelim": datetime(2026, 8, 15, 23, 0),
                "main": datetime(2026, 8, 16, 1, 0),
            },
        )

    def test_infers_two_card_sections_as_prelims_and_main(self):
        competitions = [
            {"id": "prelim", "date": "2026-08-01T14:00Z"},
            {"id": "main", "date": "2026-08-01T17:00Z"},
        ]
        self.assertEqual(
            infer_competition_sections(competitions),
            {"prelim": "prelim", "main": "main"},
        )

    def test_explicit_espn_segment_wins_over_time_fallback(self):
        competitions = [
            {
                "id": "1",
                "date": "2026-08-15T21:00Z",
                "cardSegment": {"name": "prelims2"},
            },
            {
                "id": "2",
                "date": "2026-08-15T21:00Z",
                "cardSegment": {"name": "main"},
            },
        ]
        self.assertEqual(
            infer_competition_sections(competitions),
            {"1": "early_prelim", "2": "main"},
        )

    def test_historical_scope_includes_only_completed_ufc_events(self):
        payload = {
            "events": [
                {
                    "id": "1",
                    "name": "UFC Freedom 250",
                    "status": {"type": {"completed": True}},
                },
                {
                    "id": "2",
                    "name": "UFC 330",
                    "status": {"type": {"completed": False}},
                },
                {
                    "id": "3",
                    "name": "Dana White's Contender Series",
                    "status": {"type": {"completed": True}},
                },
            ]
        }

        self.assertEqual(
            [event["id"] for event in select_ufc_events(payload, True)],
            ["1"],
        )

    def test_upcoming_card_is_authoritative_only_near_event_date(self):
        self.assertTrue(
            event_is_near(
                {"date": datetime(2026, 8, 1)},
                reference_date=date(2026, 7, 29),
            )
        )
        self.assertFalse(
            event_is_near(
                {"date": datetime(2026, 8, 29)},
                reference_date=date(2026, 7, 29),
            )
        )

    def test_normalizes_duplicated_bout_weight_class(self):
        self.assertEqual(
            normalize_weight_class("Bantamweight Bout Bantamweight Bout"),
            "Bantamweight",
        )
        self.assertEqual(
            normalize_weight_class("Light Heavyweight Bout"),
            "Light Heavyweight",
        )
        self.assertEqual(
            normalize_weight_class("Women's Bantamweight"),
            "Women's Bantamweight",
        )

    def test_event_completes_when_expected_results_are_present(self):
        bouts = [
            {"id": 1, "status": "completed", "result": {"winner": "red"}},
            {"id": 2, "status": "completed", "result": {"winner": "blue"}},
            # A stale local fight removed from the final ESPN card must not
            # keep an otherwise fully resulted card live forever.
            {"id": 3, "status": "scheduled", "result": None},
        ]
        self.assertTrue(event_has_all_results(bouts, expected_total=2))
        self.assertTrue(has_fight_result(bouts[0]))
        self.assertFalse(has_fight_result(bouts[2]))

    def test_event_without_expected_total_requires_every_active_result(self):
        bouts = [
            {"status": "completed", "result": {"winner": "red"}},
            {"status": "scheduled", "result": None},
        ]
        self.assertFalse(event_has_all_results(bouts))
        bouts[1]["status"] = "cancelled"
        self.assertTrue(event_has_all_results(bouts))

    def test_event_date_is_normalized_for_backend_date_schema(self):
        event = transform_event(
            {
                "id": "600059339",
                "name": "UFC Fight Night: Medic vs. Rodriguez",
                "date": "2026-08-01T18:00Z",
                "competitions": [],
            },
            135755,
        )
        self.assertEqual(event["date"], datetime(2026, 8, 1))

    def test_event_uses_earliest_section_for_start_and_locks(self):
        event = transform_event(
            {
                "id": "600059185",
                "name": "UFC 330: Makhachev vs. Machado Garry",
                "date": "2026-08-15T21:00Z",
                "competitions": [
                    {"id": "early", "date": "2026-08-15T21:00Z"},
                    {"id": "prelim", "date": "2026-08-15T23:00Z"},
                    {"id": "main", "date": "2026-08-16T01:00Z"},
                ],
            },
            142341,
        )

        self.assertEqual(event["card_start_time_utc"], datetime(2026, 8, 15, 21))
        self.assertEqual(event["picks_lock_time_utc"], datetime(2026, 8, 15, 21))
        self.assertEqual(event["start_time_et"], "17:00")
        self.assertEqual(event["timing_source"], "espn")

    def test_matches_event_by_date_and_accent_insensitive_name(self):
        espn_event = {
            "id": "600059339",
            "name": "UFC Fight Night: Medić vs. Rodriguez",
            "date": "2026-08-01T14:00Z",
        }
        existing = [
            {
                "_id": 135755,
                "id": 135755,
                "name": "UFC Fight Night UFC Fight Night: Medic vs Rodriguez",
                "date": datetime(2026, 8, 1),
            }
        ]
        self.assertEqual(find_event_match(espn_event, existing)["id"], 135755)

    def test_prefers_unique_canonical_event_over_generated_espn_duplicate(self):
        espn_event = {
            "id": "600060621",
            "name": "UFC Fight Night: Gamrot vs. Salkilld",
            "date": "2026-08-08T12:00Z",
        }
        existing = [
            {
                "id": 143855,
                "source": "tapology",
                "name": "UFC Fight Night",
                "date": datetime(2026, 8, 8),
            },
            {
                "id": 600060621,
                "source": "espn",
                "espn_event_id": "600060621",
                "name": "UFC Fight Night: Gamrot vs. Salkilld",
                "date": datetime(2026, 8, 8, 12),
            },
        ]
        self.assertEqual(find_event_match(espn_event, existing)["id"], 143855)

    def test_matches_bout_regardless_of_existing_corner_order(self):
        competition = {
            "id": "401870843",
            "competitors": [
                competitor("4426312", "Daniel Rodriguez", 2),
                competitor("4685870", "Uroš Medić", 1),
            ],
        }
        existing_bout = {
            "id": 9001,
            "fighters": {
                "red": {"fighter_name": "Daniel Rodriguez"},
                "blue": {"fighter_name": "Uros Medic"},
            },
        }
        self.assertEqual(find_bout_match(competition, [existing_bout])["id"], 9001)
        mapping = map_competitors_to_corners(competition, existing_bout)
        self.assertEqual(mapping["red"]["id"], "4426312")
        self.assertEqual(mapping["blue"]["id"], "4685870")

    def test_transforms_completed_ko_result_to_internal_corner(self):
        competition = {
            "id": "401892276",
            "competitors": [
                competitor("1", "Winner", 1, winner=True),
                competitor("2", "Loser", 2, winner=False),
            ],
            "status": {
                "period": 5,
                "displayClock": "2:41",
                "type": {"completed": True, "state": "post"},
            },
            "details": [
                {"type": {"text": "Results"}},
                {"type": {"text": "Unofficial Winner Kotko"}},
            ],
        }
        mapping = {
            "red": competition["competitors"][1],
            "blue": competition["competitors"][0],
        }
        result = transform_result(competition, mapping)
        self.assertEqual(result["winner"], "blue")
        self.assertEqual(result["outcome"], "blue")
        self.assertEqual(result["winner_name"], "Winner")
        self.assertEqual(result["method"], "KO/TKO")
        self.assertEqual(result["round"], 5)

    def test_transforms_espn_profile_units_and_headshot(self):
        profile = transform_athlete_profile(
            {
                "id": "4685870",
                "displayName": "Uroš Medić",
                "height": 73.0,
                "reach": 71.0,
                "weight": 171.0,
                "age": 33,
                "dateOfBirth": "1993-04-25T07:00Z",
                "citizenship": "Serbia",
                "headshot": {
                    "href": (
                        "https://a.espncdn.com/i/headshots/mma/players/"
                        "full/4685870.png"
                    )
                },
                "association": {"name": "Kings MMA"},
                "stance": {"text": "Southpaw"},
                "weightClass": {"text": "Welterweight"},
            }
        )
        self.assertEqual(profile["height_cm"], 185)
        self.assertEqual(profile["height"]["feet"], 6)
        self.assertEqual(profile["height"]["inches"], 1)
        self.assertEqual(profile["reach_cm"], 180)
        self.assertEqual(profile["date_of_birth"], "1993-04-25")
        self.assertEqual(profile["gym"]["primary"], "Kings MMA")
        self.assertEqual(
            profile["espn_headshot_download_url"],
            build_headshot_url("4685870"),
        )

    def test_transforms_record_and_finish_statistics(self):
        payload = {
            "items": [
                {
                    "name": "overall",
                    "type": "total",
                    "stats": [
                        {"name": "wins", "value": 13},
                        {"name": "losses", "value": 3},
                        {"name": "draws", "value": 0},
                        {"name": "noContests", "value": 0},
                        {"name": "tkos", "value": 11},
                        {"name": "tkoLosses", "value": 1},
                        {"name": "submissions", "value": 2},
                        {"name": "submissionLosses", "value": 2},
                    ],
                }
            ]
        }
        result = transform_athlete_records(payload)
        self.assertEqual(result["current_record"]["wins"], 13)
        self.assertEqual(result["career_stats"]["wins_by_ko_tko"], 11)
        self.assertEqual(result["career_stats"]["wins_by_submission"], 2)

    def test_records_ignore_null_items_from_espn(self):
        result = transform_athlete_records({"items": [None]})
        self.assertEqual(
            result["current_record"],
            {"wins": 0, "losses": 0, "draws": 0, "no_contests": 0},
        )

    def test_pick_scoring_is_accent_insensitive(self):
        pick = {
            "picked_fighter_name": "Uros Medic",
            "picked_method": "TKO",
            "picked_round": 2,
        }
        result = {
            "winner_name": "Uroš Medić",
            "outcome": "red",
            "method": "KO/TKO",
            "round": 2,
        }
        self.assertEqual(calculate_pick_points(pick, result), (3, True))

    def test_record_and_card_position_helpers(self):
        self.assertEqual(
            parse_record_summary("13-3-0"),
            {"wins": 13, "losses": 3, "draws": 0},
        )
        self.assertTrue(infer_card_position(9, 10)["is_main_event"])
        self.assertEqual(infer_card_position(0, 12)["card_section"], "early_prelim")
        self.assertEqual(
            age_on_date(date(1993, 4, 25), date(2026, 8, 1)),
            33,
        )
        metadata = transform_competition_metadata(
            {
                "matchNumber": 14,
                "cardSegment": {
                    "name": "prelims2",
                    "description": "Early Prelims",
                },
                "format": {"regulation": {"periods": 3}},
                "type": {"text": "Women's Bantamweight"},
            }
        )
        self.assertEqual(metadata["card_section"], "early_prelim")
        self.assertEqual(metadata["gender"], "female")
        self.assertEqual(metadata["espn_match_number"], 14)


if __name__ == "__main__":
    unittest.main()
