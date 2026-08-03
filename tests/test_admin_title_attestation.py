import copy
import json
from pathlib import Path

import pytest

from tapology_scraper.admin_title_attestation import (
    AdminTitleAttestationError,
    load_admin_title_attestation,
    parse_admin_title_attestation,
)


def valid_payload():
    return {
        "schema_version": "admin-title-attestation/v1",
        "attested_by": "Jose",
        "attested_at": "2026-08-01T00:00:00Z",
        "decision_ref": "D-DATA-012",
        "reason": "Complete TITLE baseline for a bounded dry-run.",
        "scope_event_ids": [20, 10],
        "title_bouts": [
            {"event_id": 10, "bout_id": 101, "title_type": "bmf"},
            {"event_id": 20, "bout_id": 201, "title_type": "undisputed"},
        ],
        "all_other_bouts_non_title": True,
        "authorized_use": "CARD_DATA_BACKFILL_DRY_RUN_ONLY",
        "production_write_authorized": False,
    }


def test_parse_is_deterministic_and_expands_complete_admin_values():
    payload = valid_payload()

    first = parse_admin_title_attestation(payload)
    second = parse_admin_title_attestation(copy.deepcopy(payload))
    values = first.values_for_card(10, [102, 101])

    assert first == second
    assert first.attestation_id.startswith("sha256:")
    assert first.scope_event_ids == (10, 20)
    assert values == {
        101: {
            "is_title_fight": True,
            "is_bmf_title_fight": True,
            "title_type": "bmf",
        },
        102: {
            "is_title_fight": False,
            "is_bmf_title_fight": False,
            "title_type": "none",
        },
    }
    assert first.values_for_card(999, [1]) == {}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "wrong"),
        ("attested_at", "2026-08-01T00:00:00-06:00"),
        ("authorized_use", "PRODUCTION_WRITE"),
        ("production_write_authorized", True),
        ("all_other_bouts_non_title", False),
        ("title_bouts", "not-an-array"),
    ],
)
def test_rejects_unsafe_or_incomplete_top_level_values(field, value):
    payload = valid_payload()
    payload[field] = value

    with pytest.raises(AdminTitleAttestationError):
        parse_admin_title_attestation(payload)


def test_rejects_duplicate_out_of_scope_and_noncanonical_assignments():
    duplicate = valid_payload()
    duplicate["title_bouts"].append(copy.deepcopy(duplicate["title_bouts"][0]))
    with pytest.raises(AdminTitleAttestationError, match="Duplicate"):
        parse_admin_title_attestation(duplicate)

    outside = valid_payload()
    outside["title_bouts"][0]["event_id"] = 999
    with pytest.raises(AdminTitleAttestationError, match="outside"):
        parse_admin_title_attestation(outside)

    wrong_type = valid_payload()
    wrong_type["title_bouts"][0]["title_type"] = "championship"
    with pytest.raises(AdminTitleAttestationError, match="canonical"):
        parse_admin_title_attestation(wrong_type)


def test_validates_complete_selected_scope_and_observed_bout_ids():
    attestation = parse_admin_title_attestation(valid_payload())

    attestation.validate_observed_scope({10: [101, 102], 20: [201, 202]})

    with pytest.raises(AdminTitleAttestationError, match="omits"):
        attestation.validate_observed_scope({10: [101, 102]})
    with pytest.raises(AdminTitleAttestationError, match="outside"):
        attestation.validate_observed_scope({10: [102], 20: [201, 202]})
    with pytest.raises(AdminTitleAttestationError, match="duplicate"):
        attestation.values_for_card(10, [101, 101])


def test_optional_attestation_id_must_match_the_canonical_payload():
    parsed = parse_admin_title_attestation(valid_payload())
    payload = valid_payload()
    payload["attestation_id"] = parsed.attestation_id
    assert parse_admin_title_attestation(payload) == parsed

    payload["attestation_id"] = "sha256:" + "0" * 64
    with pytest.raises(AdminTitleAttestationError, match="does not match"):
        parse_admin_title_attestation(payload)


def test_loader_reports_sanitized_file_errors(tmp_path):
    malformed = tmp_path / "attestation.json"
    malformed.write_text("{not json", encoding="utf-8")

    with pytest.raises(AdminTitleAttestationError, match="JSONDecodeError"):
        load_admin_title_attestation(malformed)

    with pytest.raises(AdminTitleAttestationError, match="does not exist"):
        load_admin_title_attestation(tmp_path / "missing.json")


def test_source_has_no_database_network_or_write_dependencies():
    import tapology_scraper.admin_title_attestation as module

    source = Path(module.__file__).read_text(encoding="utf-8").lower()
    forbidden = (
        "pymongo",
        "motor",
        "requests",
        "httpx",
        "insert_one",
        "update_one",
        "delete_one",
        "mongodb_uri",
    )

    assert not any(value in source for value in forbidden)
    assert json.loads(json.dumps(valid_payload())) == valid_payload()
