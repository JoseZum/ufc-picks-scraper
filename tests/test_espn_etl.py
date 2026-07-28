import unittest
from datetime import date, datetime

from tapology_scraper.espn_etl import (
    age_on_date,
    build_headshot_url,
    calculate_pick_points,
    find_bout_match,
    find_event_match,
    infer_card_position,
    map_competitors_to_corners,
    parse_record_summary,
    transform_athlete_profile,
    transform_athlete_records,
    transform_competition_metadata,
    transform_event,
    transform_result,
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
