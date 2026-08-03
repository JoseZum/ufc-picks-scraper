import copy
import io
import json
from dataclasses import replace
from pathlib import Path

import pytest

from tapology_scraper.production_card_audit import (
    AUDITED_COLLECTIONS,
    BOUT_PROJECTION,
    EVENT_PROJECTION,
    EXIT_CONFIGURATION_ERROR,
    EXIT_PASS,
    GOLDEN_CARD_SPECS,
    GOLDEN_EVENT_IDS,
    SLOT_PROJECTION,
    GoldenCardSpec,
    LegacyCardDocuments,
    ProductionAuditConfigurationError,
    audit_legacy_card,
    audit_legacy_cards,
    fetch_allowlisted_cards,
    load_mongo_settings,
    main,
    render_json,
    render_markdown,
)


class FakeCursor:
    def __init__(self, documents):
        self.documents = list(documents)
        self.sort_calls = []
        self.max_time_calls = []

    def sort(self, field, direction):
        self.sort_calls.append((field, direction))
        self.documents.sort(key=lambda item: item.get(field, 0))
        return self

    def max_time_ms(self, value):
        self.max_time_calls.append(value)
        return self

    def __iter__(self):
        return iter(self.documents)


class FakeCollection:
    def __init__(self, documents):
        self.documents = list(documents)
        self.calls = []

    def find_one(self, query, projection, **kwargs):
        self.calls.append(("find_one", query, projection, kwargs))
        return next(
            (document for document in self.documents if document.get("id") == query["id"]),
            None,
        )

    def find(self, query, projection):
        self.calls.append(("find", query, projection, {}))
        return FakeCursor(
            document
            for document in self.documents
            if document.get("event_id") == query["event_id"]
        )


class FakeDatabase:
    def __init__(self, events=(), bouts=(), slots=()):
        self.collections = {
            "events": FakeCollection(events),
            "bouts": FakeCollection(bouts),
            "event_card_slots": FakeCollection(slots),
        }
        self.requested_collections = []

    def __getitem__(self, name):
        self.requested_collections.append(name)
        return self.collections[name]


def fighter(fighter_id: str) -> dict:
    return {"fighter_id": fighter_id, "fighter_name": "DO NOT RETAIN THIS NAME"}


def build_clean_scheduled_card() -> LegacyCardDocuments:
    spec = GOLDEN_CARD_SPECS[0]
    event = {
        "id": spec.event_id,
        "name": "UFC Fight Night: Medic vs Rodriguez",
        "source": "tapology",
        "promotion": "UFC",
        "official_event_date": "2026-08-01",
        "official_date_timezone": "America/New_York",
        "month_key": "2026-08",
        "status": "scheduled",
        "total_bouts": 2,
        "listed_bout_count": 2,
        "mission_eligible_bout_count": 2,
        "main_event_bout_id": 101,
        "espn_event_id": spec.expected_espn_event_id,
        "lifecycle_revision": 1,
        "structure_revision": 1,
        "timing_revision": 1,
        "current_eligibility": {
            "eligible_targets": [
                {"bout_id": 101, "matchup_revision": 1},
                {"bout_id": 102, "matchup_revision": 1},
            ],
            "excluded_targets": [],
            "denominator": 2,
            "fingerprint": "sha256:clean-fixture",
        },
        "private_url": "mongodb+srv://must-not-appear",
    }
    bouts = (
        {
            "id": 101,
            "event_id": spec.event_id,
            "status": "scheduled",
            "lineage_id": "lineage-101",
            "matchup_revision": 1,
            "rounds_scheduled": 5,
            "fighters": {"red": fighter("ftr-1"), "blue": fighter("ftr-2")},
            "is_title_fight": False,
            "title_type": "none",
            "is_main_event": True,
            "is_co_main_event": False,
            "card_section": "main",
            "order_overall": 1,
            "order_section": 1,
            "evidence": {"is_title_fight": {"source_kind": "admin_override"}},
        },
        {
            "id": 102,
            "event_id": spec.event_id,
            "status": "scheduled",
            "lineage_id": "lineage-102",
            "matchup_revision": 1,
            "rounds_scheduled": 3,
            "fighters": {"red": fighter("ftr-3"), "blue": fighter("ftr-4")},
            "is_title_fight": False,
            "title_type": "none",
            "is_main_event": False,
            "is_co_main_event": False,
            "card_section": "prelim",
            "order_overall": 2,
            "order_section": 1,
            "evidence": {"is_title_fight": {"source_kind": "admin_override"}},
        },
    )
    slots = (
        {
            "id": f"{spec.event_id}:101",
            "event_id": spec.event_id,
            "bout_id": 101,
            "is_current": True,
            "card_section": "main",
            "order_overall": 1,
            "order_section": 1,
            "role": "main_event",
            "structure_revision": 1,
        },
        {
            "id": f"{spec.event_id}:102",
            "event_id": spec.event_id,
            "bout_id": 102,
            "is_current": True,
            "card_section": "prelim",
            "order_overall": 2,
            "order_section": 1,
            "role": "regular",
            "structure_revision": 1,
        },
    )
    return LegacyCardDocuments(spec=spec, event=event, bouts=bouts, slots=slots)


def issue_codes(card_audit) -> set[str]:
    return {issue.code for issue in card_audit.issues}


def capability(card_audit, capability_id: str):
    return next(
        item for item in card_audit.capabilities if item.capability == capability_id
    )


def test_q014_allowlist_is_exact_and_minimal():
    assert GOLDEN_EVENT_IDS == (135755, 142341, 136871, 142997)
    assert AUDITED_COLLECTIONS == ("events", "bouts", "event_card_slots")
    assert set().union(*(set(spec.scenario_tags) for spec in GOLDEN_CARD_SPECS)) >= {
        "fight_night",
        "numbered",
        "three_section",
        "title_heavy",
        "late_cancellation",
        "positive_identity",
    }


def test_positive_fight_night_mapping_is_separate_from_negative_fixture():
    negative = next(spec for spec in GOLDEN_CARD_SPECS if spec.event_id == 135755)
    positive = next(spec for spec in GOLDEN_CARD_SPECS if spec.event_id == 142997)

    assert negative.expected_espn_event_id == positive.expected_espn_event_id
    assert "positive_identity" not in negative.scenario_tags
    assert "positive_identity" in positive.scenario_tags
    assert positive.expected_sections == ("prelim", "main")


def test_clean_candidate_card_has_ready_facts_and_pending_results_without_mutation():
    card = build_clean_scheduled_card()
    before = copy.deepcopy(card)

    audited = audit_legacy_card(card)

    assert not audited.has_blocking_findings
    assert audited.observed_name_matches
    assert capability(audited, "EVT").state == "ready"
    assert capability(audited, "EVT_DATE").state == "ready"
    assert capability(audited, "BOUT").state == "ready"
    assert capability(audited, "ELIG").state == "ready"
    assert capability(audited, "STRUCT").state == "ready"
    assert capability(audited, "TITLE").state == "ready"
    assert capability(audited, "RES").state == "pending"
    assert card == before


def test_event_name_identity_is_accent_and_punctuation_tolerant_but_not_partial():
    card = build_clean_scheduled_card()
    accented = replace(
        card,
        event={**card.event, "name": "UFC Fight Night: Medi\u0107 vs. Rodriguez"},
    )
    different_headliner = replace(
        card,
        event={**card.event, "name": "UFC Fight Night: Someone vs Else"},
    )

    assert audit_legacy_card(accented).observed_name_matches
    assert not audit_legacy_card(different_headliner).observed_name_matches


def test_title_heavy_card_requires_expected_title_flags_types_and_evidence():
    base = build_clean_scheduled_card()
    spec = GOLDEN_CARD_SPECS[1]
    card = LegacyCardDocuments(
        spec=spec,
        event={**base.event, "id": spec.event_id, "name": spec.expected_name, "espn_event_id": spec.expected_espn_event_id},
        bouts=tuple({**bout, "event_id": spec.event_id, "is_title_fight": False, "title_type": None, "evidence": {}} for bout in base.bouts),
        slots=tuple({**slot, "id": f"{spec.event_id}:{slot['bout_id']}", "event_id": spec.event_id} for slot in base.slots),
    )

    audited = audit_legacy_card(card)

    assert "EXPECTED_TITLE_BOUTS_MISSING" in issue_codes(audited)
    assert "TITLE_EVIDENCE_MISSING" in issue_codes(audited)
    assert capability(audited, "TITLE").state == "blocked"


def test_known_ufc326_cancellation_must_be_retained():
    base = build_clean_scheduled_card()
    spec = GOLDEN_CARD_SPECS[2]
    card = LegacyCardDocuments(
        spec=spec,
        event={**base.event, "id": spec.event_id, "name": spec.expected_name, "status": "completed"},
        bouts=tuple({**bout, "event_id": spec.event_id} for bout in base.bouts),
        slots=tuple({**slot, "id": f"{spec.event_id}:{slot['bout_id']}", "event_id": spec.event_id} for slot in base.slots),
    )

    audited = audit_legacy_card(card)

    assert "KNOWN_CANCELLATION_NOT_RETAINED" in issue_codes(audited)
    assert capability(audited, "BOUT").state == "blocked"


def test_structure_duplicate_gap_and_flattened_disagreement_are_quarantined():
    card = build_clean_scheduled_card()
    slots = list(copy.deepcopy(card.slots))
    slots[1]["order_overall"] = 1
    bouts = list(copy.deepcopy(card.bouts))
    bouts[1]["card_section"] = "main"
    mutated = replace(card, bouts=tuple(bouts), slots=tuple(slots))

    audited = audit_legacy_card(mutated)

    assert {"ORDER_DUPLICATE", "ORDER_GAP", "FLATTENED_STRUCTURE_DISAGREEMENT"} <= issue_codes(audited)
    assert capability(audited, "STRUCT").state == "quarantined"


def test_golden_scenario_rejects_an_unexpected_card_section():
    card = build_clean_scheduled_card()
    extra_bout = {
        **copy.deepcopy(card.bouts[1]),
        "id": 103,
        "fighters": {"red": fighter("ftr-5"), "blue": fighter("ftr-6")},
        "lineage_id": "lineage-103",
        "card_section": "early_prelim",
        "order_overall": 3,
    }
    extra_slot = {
        **copy.deepcopy(card.slots[1]),
        "id": f"{card.spec.event_id}:103",
        "bout_id": 103,
        "card_section": "early_prelim",
        "order_overall": 3,
    }
    event = {
        **card.event,
        "total_bouts": 3,
        "listed_bout_count": 3,
        "mission_eligible_bout_count": 3,
        "current_eligibility": {
            **card.event["current_eligibility"],
            "eligible_targets": [
                *card.event["current_eligibility"]["eligible_targets"],
                {"bout_id": 103, "matchup_revision": 1},
            ],
            "denominator": 3,
        },
    }
    mutated = replace(
        card,
        event=event,
        bouts=(*card.bouts, extra_bout),
        slots=(*card.slots, extra_slot),
    )

    audited = audit_legacy_card(mutated)

    assert "UNEXPECTED_SECTION_PRESENT" in issue_codes(audited)
    assert capability(audited, "STRUCT").state == "blocked"


def test_completed_scope_requires_results_and_result_versions():
    card = build_clean_scheduled_card()
    event = {**card.event, "status": "completed"}
    bouts = list(copy.deepcopy(card.bouts))
    bouts[0]["status"] = "completed"
    bouts[0]["result"] = {
        "winner": "red",
        "outcome": "red",
        "method": "KO/TKO",
        "round": 2,
        "time": "1:42",
    }
    bouts[1]["status"] = "completed"
    completed = replace(card, event=event, bouts=tuple(bouts))

    audited = audit_legacy_card(completed)

    assert "RESULT_NOT_FINAL" in issue_codes(audited)
    assert "RESULT_REVISION_MISSING" in issue_codes(audited)
    assert "RESULT_TIME_NOT_NORMALIZED" in issue_codes(audited)
    assert capability(audited, "RES").state == "blocked"


def test_legacy_decision_method_is_normalized_and_missing_round_is_not_invalid():
    card = build_clean_scheduled_card()
    event = {**card.event, "status": "completed", "first_final_result_at": "2026-08-02T04:00:00Z"}
    bouts = []
    for bout in card.bouts:
        completed = copy.deepcopy(bout)
        completed["status"] = "completed"
        completed["result_revision"] = 1
        completed["result"] = {
            "revision": 1,
            "winner": "red",
            "outcome": "red",
            "method": "DEC",
        }
        bouts.append(completed)

    audited = audit_legacy_card(replace(card, event=event, bouts=tuple(bouts)))

    assert dict(audited.result_method_counts) == {"decision": 2}
    assert "RESULT_ROUND_INVALID" not in issue_codes(audited)


def test_missing_event_quarantines_every_capability():
    spec = GOLDEN_CARD_SPECS[0]
    audited = audit_legacy_card(
        LegacyCardDocuments(spec=spec, event=None, bouts=(), slots=())
    )

    assert not audited.event_found
    assert all(item.state == "quarantined" for item in audited.capabilities)
    assert sum(issue.code == "EVENT_NOT_FOUND" for issue in audited.issues) == 7


def test_report_is_deterministic_and_never_retains_raw_names_urls_or_credentials():
    card = build_clean_scheduled_card()
    first = audit_legacy_cards([card])
    second = audit_legacy_cards([copy.deepcopy(card)])

    first_json = render_json(first)
    assert first_json == render_json(second)
    assert "DO NOT RETAIN THIS NAME" not in first_json
    assert "mongodb+srv" not in first_json
    assert "private_url" not in first_json
    assert json.loads(first_json)["raw_documents_retained"] is False


def test_markdown_contains_aggregate_evidence_and_interpretation_boundary():
    report = audit_legacy_cards([build_clean_scheduled_card()])

    rendered = render_markdown(report)

    assert "# Golden Production Card Audit" in rendered
    assert "Raw documents retained: `NO`" in rendered
    assert "## Capability states" in rendered
    assert "eligible candidates=2" in rendered
    assert "Interpretation boundary" in rendered


def test_fetcher_uses_only_allowlisted_projected_find_operations():
    card = build_clean_scheduled_card()
    database = FakeDatabase([card.event], card.bouts, card.slots)

    loaded = fetch_allowlisted_cards(database, (card.spec,))

    assert len(loaded) == 1
    assert database.requested_collections == list(AUDITED_COLLECTIONS)
    event_call = database.collections["events"].calls[0]
    bout_call = database.collections["bouts"].calls[0]
    slot_call = database.collections["event_card_slots"].calls[0]
    assert event_call == (
        "find_one",
        {"id": card.spec.event_id},
        EVENT_PROJECTION,
        {"max_time_ms": 10_000},
    )
    assert bout_call[:3] == (
        "find",
        {"event_id": card.spec.event_id},
        BOUT_PROJECTION,
    )
    assert slot_call[:3] == (
        "find",
        {"event_id": card.spec.event_id},
        SLOT_PROJECTION,
    )


def test_fetcher_rejects_event_outside_q014_allowlist():
    database = FakeDatabase()
    unapproved = GoldenCardSpec(event_id=999999, expected_name="Not approved", scenario_tags=())

    with pytest.raises(ProductionAuditConfigurationError, match="allowlist"):
        fetch_allowlisted_cards(database, (unapproved,))

    assert database.requested_collections == []


def test_settings_load_from_environment_without_exposing_values(monkeypatch):
    secret = "mongodb+srv://audit-user:super-secret@example.invalid/"
    monkeypatch.setenv("MONGODB_URI", secret)
    monkeypatch.setenv("MONGODB_DB_NAME", "ufc_test")

    uri, database_name = load_mongo_settings()

    assert uri == secret
    assert database_name == "ufc_test"


def test_missing_settings_and_driver_failures_have_sanitized_cli_errors(monkeypatch):
    import tapology_scraper.production_card_audit as audit_module

    monkeypatch.delenv("MONGODB_URI", raising=False)
    missing_stderr = io.StringIO()
    missing_exit = main([], stdout=io.StringIO(), stderr=missing_stderr)
    secret = "mongodb+srv://audit-user:super-secret@example.invalid/"
    monkeypatch.setattr(audit_module, "load_mongo_settings", lambda _path: (secret, "ufc"))

    def fail_safely(_uri, _database_name, _specs):
        raise RuntimeError(f"connection failed for {secret}")

    monkeypatch.setattr(audit_module, "run_production_audit", fail_safely)
    failure_stderr = io.StringIO()
    failure_exit = main([], stdout=io.StringIO(), stderr=failure_stderr)

    assert missing_exit == EXIT_CONFIGURATION_ERROR
    assert "MONGODB_URI is not configured" in missing_stderr.getvalue()
    assert failure_exit == EXIT_CONFIGURATION_ERROR
    assert "RuntimeError" in failure_stderr.getvalue()
    assert secret not in failure_stderr.getvalue()
    assert "super-secret" not in failure_stderr.getvalue()


def test_successful_cli_writes_only_requested_sanitized_report(monkeypatch, tmp_path):
    import tapology_scraper.production_card_audit as audit_module

    report = audit_legacy_cards([build_clean_scheduled_card()])
    monkeypatch.setattr(
        audit_module,
        "load_mongo_settings",
        lambda _path: ("mongodb://not-used", "ufc"),
    )
    monkeypatch.setattr(
        audit_module,
        "run_production_audit",
        lambda _uri, _database_name, _specs: report,
    )
    output_path = tmp_path / "audit.json"

    exit_code = main(
        ["--format", "json", "--output", str(output_path)],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert exit_code == EXIT_PASS
    value = json.loads(output_path.read_text(encoding="utf-8"))
    assert value["contains_credentials"] is False
    assert sorted(path.name for path in tmp_path.iterdir()) == ["audit.json"]


def test_env_file_cannot_be_overwritten_by_report(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("MONGODB_URI=mongodb://secret", encoding="utf-8")
    stderr = io.StringIO()

    exit_code = main(
        ["--env-file", str(env_path), "--output", str(env_path)],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_CONFIGURATION_ERROR
    assert "must not overwrite" in stderr.getvalue()
    assert env_path.read_text(encoding="utf-8") == "MONGODB_URI=mongodb://secret"


def test_source_contains_no_write_methods_or_out_of_scope_collections():
    import tapology_scraper.production_card_audit as audit_module

    source = Path(audit_module.__file__).read_text(encoding="utf-8")
    forbidden = (
        ".insert_one(",
        ".insert_many(",
        ".update_one(",
        ".update_many(",
        ".replace_one(",
        ".delete_one(",
        ".delete_many(",
        ".bulk_write(",
        ".$out",
        ".$merge",
        'database["users"]',
        'database["picks"]',
        'database["missions"]',
    )

    assert not any(value in source for value in forbidden)
