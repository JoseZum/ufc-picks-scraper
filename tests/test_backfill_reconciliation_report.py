import copy
import io
import json
from dataclasses import replace
from pathlib import Path

import pytest

from tapology_scraper.admin_title_attestation import parse_admin_title_attestation
from tapology_scraper.backfill_reconciliation_report import (
    BACKFILL_BOUT_PROJECTION,
    BACKFILL_EVENT_PROJECTION,
    BACKFILL_SLOT_PROJECTION,
    EXIT_BLOCKED,
    EXIT_CONFIGURATION_ERROR,
    EXIT_REVIEWABLE,
    BackfillDryRunConfigurationError,
    build_backfill_dry_run,
    build_card_backfill_projection,
    build_card_dry_run,
    fetch_backfill_cards,
    main,
    render_json,
    render_markdown,
)
from tapology_scraper.production_card_audit import (
    GOLDEN_CARD_SPECS,
    GoldenCardSpec,
    LegacyCardDocuments,
)


FIGHTER_NAME_MARKER = "PRIVATE FIGHTER NAME MUST NEVER APPEAR"


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
            (item for item in self.documents if item.get("id") == query["id"]),
            None,
        )

    def find(self, query, projection):
        self.calls.append(("find", query, projection, {}))
        return FakeCursor(
            item for item in self.documents if item.get("event_id") == query["event_id"]
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


def fighter(fighter_id):
    return {
        "fighter_id": fighter_id,
        "fighter_name": FIGHTER_NAME_MARKER,
    }


def build_clean_card():
    spec = GOLDEN_CARD_SPECS[0]
    event = {
        "id": spec.event_id,
        "name": "UFC Fight Night: Medic vs Rodriguez",
        "source": "espn",
        "promotion": "UFC",
        "official_event_date": "2026-08-22",
        "official_date_timezone": "America/New_York",
        "month_key": "2026-08",
        "status": "scheduled",
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
            "fingerprint": "sha256:legacy",
        },
        "private_uri": "mongodb+srv://secret.invalid/",
    }
    bouts = (
        {
            "id": 101,
            "event_id": spec.event_id,
            "source": "espn",
            "espn_competition_id": "9001",
            "status": "scheduled",
            "lineage_id": "lineage-101",
            "matchup_revision": 1,
            "rounds_scheduled": 5,
            "fighters": {"red": fighter("ftr-1"), "blue": fighter("ftr-2")},
            "is_title_fight": False,
            "title_type": "none",
            "is_main_event": True,
            "card_section": "main",
            "order_overall": 1,
            "order_section": 1,
            "evidence": {
                "is_title_fight": {
                    "source_kind": "admin_override",
                    "observed_at": "2026-07-31T20:00:00Z",
                }
            },
        },
        {
            "id": 102,
            "event_id": spec.event_id,
            "source": "espn",
            "espn_competition_id": "9002",
            "status": "scheduled",
            "lineage_id": "lineage-102",
            "matchup_revision": 1,
            "rounds_scheduled": 3,
            "fighters": {"red": fighter("ftr-3"), "blue": fighter("ftr-4")},
            "is_title_fight": False,
            "title_type": "none",
            "card_section": "prelim",
            "order_overall": 2,
            "order_section": 1,
            "evidence": {
                "is_title_fight": {
                    "source_kind": "admin_override",
                    "observed_at": "2026-07-31T20:00:00Z",
                }
            },
        },
    )
    slots = (
        {
            "_id": f"{spec.event_id}:101",
            "id": f"{spec.event_id}:101",
            "event_id": spec.event_id,
            "bout_id": 101,
            "source": "espn",
            "is_current": True,
            "card_section": "main",
            "order_overall": 1,
            "order_section": 1,
            "role": "main_event",
            "scheduled_start_time_utc": "2026-08-23T02:00:00Z",
            "automatic_lock_time_utc": "2026-08-23T02:00:00Z",
            "structure_revision": 1,
        },
        {
            "_id": f"{spec.event_id}:102",
            "id": f"{spec.event_id}:102",
            "event_id": spec.event_id,
            "bout_id": 102,
            "source": "espn",
            "is_current": True,
            "card_section": "prelim",
            "order_overall": 2,
            "order_section": 1,
            "role": "regular",
            "scheduled_start_time_utc": "2026-08-23T00:00:00Z",
            "automatic_lock_time_utc": "2026-08-23T00:00:00Z",
            "structure_revision": 1,
        },
    )
    return LegacyCardDocuments(spec=spec, event=event, bouts=bouts, slots=slots)


def build_title_attestation(card=None):
    card = card or build_clean_card()
    return parse_admin_title_attestation(
        {
            "schema_version": "admin-title-attestation/v1",
            "attested_by": "Jose",
            "attested_at": "2026-08-01T00:00:00Z",
            "decision_ref": "D-DATA-012",
            "reason": "Complete test TITLE baseline.",
            "scope_event_ids": [card.spec.event_id],
            "title_bouts": [
                {
                    "event_id": card.spec.event_id,
                    "bout_id": 101,
                    "title_type": "bmf",
                }
            ],
            "all_other_bouts_non_title": True,
            "authorized_use": "CARD_DATA_BACKFILL_DRY_RUN_ONLY",
            "production_write_authorized": False,
        }
    )


def finding_codes(card_result):
    return {item.code for item in card_result.findings}


def test_clean_projection_is_reviewable_deterministic_and_input_is_immutable():
    card = build_clean_card()
    before = copy.deepcopy(card)

    first = build_card_dry_run(card)
    second = build_card_dry_run(copy.deepcopy(card))

    assert first == second
    assert first.review_status == "REVIEWABLE"
    assert first.snapshot_id and first.slot_plan_id
    assert dict(first.capability_states)["TITLE"] == "ready"
    assert dict(first.capability_states)["RES"] == "pending"
    assert card == before


def test_report_aggregates_only_collection_field_names_and_counts():
    report = build_backfill_dry_run([build_clean_card()])
    value = report.as_dict()

    assert value["dry_run"] is True
    assert value["write_executed"] is False
    assert value["production_write_authorized"] is False
    assert value["selected_event_ids"] == [build_clean_card().spec.event_id]
    assert value["summary"]["counts"]["event_updates"] == 1
    assert value["summary"]["counts"]["bout_updates"] == 2
    assert value["summary"]["counts"]["slot_updates"] == 2
    assert all("." in field for field in value["summary"]["changed_fields"])


def test_missing_admin_title_evidence_blocks_instead_of_trusting_boolean():
    card = build_clean_card()
    bouts = tuple({**bout, "evidence": {}} for bout in card.bouts)

    result = build_card_dry_run(replace(card, bouts=bouts))

    assert result.review_status == "BLOCKED"
    assert "CAPABILITY_TITLE_NOT_READY" in finding_codes(result)
    assert "TITLE_STATUS_UNKNOWN" in finding_codes(result)


def test_attestation_replaces_unknown_legacy_title_state_for_every_bout():
    card = build_clean_card()
    bouts = tuple({**bout, "evidence": {}} for bout in card.bouts)
    card = replace(card, bouts=bouts)
    attestation = build_title_attestation(card)

    result = build_card_dry_run(card, attestation)

    assert result.review_status == "REVIEWABLE"
    assert dict(result.capability_states)["TITLE"] == "ready"
    assert "TITLE_STATUS_UNKNOWN" not in finding_codes(result)
    assert "ADMIN_TITLE_ATTESTATION_APPLIED" in finding_codes(result)


def test_report_exposes_only_sanitized_attestation_summary():
    card = build_clean_card()
    attestation = build_title_attestation(card)

    report = build_backfill_dry_run([card], attestation)
    value = report.as_dict()["admin_title_attestation"]

    assert value["attestation_id"] == attestation.attestation_id
    assert value["scope_event_ids"] == [card.spec.event_id]
    assert value["explicit_title_bout_count"] == 1
    assert value["applied_bout_count"] == 2
    assert value["production_write_authorized"] is False
    assert "attested_by" not in value
    assert "reason" not in value


def test_attestation_must_match_every_selected_attested_event_and_bout():
    card = build_clean_card()
    attestation = build_title_attestation(card)

    with pytest.raises(BackfillDryRunConfigurationError, match="selected dry-run"):
        build_backfill_dry_run([], attestation)
    with pytest.raises(BackfillDryRunConfigurationError, match="selected dry-run"):
        build_backfill_dry_run(
            [replace(card, bouts=(card.bouts[1],))],
            attestation,
        )


def test_identity_mismatch_blocks_before_snapshot_or_plan_creation():
    card = build_clean_card()
    mismatched = replace(
        card,
        event={**card.event, "name": "A completely different event"},
    )

    result = build_card_dry_run(mismatched)

    assert result.review_status == "BLOCKED"
    assert result.snapshot_id is None
    assert result.slot_plan_id is None
    assert "EVENT_NAME_IDENTITY_MISMATCH" in finding_codes(result)


def test_duplicate_global_order_blocks_without_arbitrary_tie_break():
    card = build_clean_card()
    slots = list(copy.deepcopy(card.slots))
    slots[1]["order_overall"] = 1

    result = build_card_dry_run(replace(card, slots=tuple(slots)))

    assert result.review_status == "BLOCKED"
    assert result.slot_plan_id is None
    assert "ORDER_BACKFILL_AMBIGUOUS" in finding_codes(result)


def test_missing_slot_can_be_proposed_from_flattened_bout_with_warning():
    card = build_clean_card()

    result = build_card_dry_run(replace(card, slots=(card.slots[0],)))

    assert "SLOT_DERIVED_FROM_FLATTENED_BOUT" in finding_codes(result)
    assert dict(result.counts)["slot_inserts"] == 1
    assert result.slot_plan_id is not None


@pytest.mark.parametrize(
    ("legacy_source", "expected_source_kind"),
    [
        ("tapology", "tapology_explicit"),
        ("ufc_official", "deterministic_fallback"),
    ],
)
def test_inserted_slot_replay_preserves_original_evidence_source(
    legacy_source,
    expected_source_kind,
):
    card = build_clean_card()
    bouts = list(copy.deepcopy(card.bouts))
    bouts[1]["source"] = legacy_source
    without_second_slot = replace(
        card,
        bouts=tuple(bouts),
        slots=(card.slots[0],),
    )

    first = build_card_backfill_projection(without_second_slot)
    inserted = next(
        slot for slot in first.slot_plan.desired_documents if slot["bout_id"] == 102
    )
    replay_card = replace(
        without_second_slot,
        slots=tuple(copy.deepcopy(first.slot_plan.desired_documents)),
    )
    replay = build_card_backfill_projection(replay_card)

    assert inserted["evidence"]["card_section"]["source_kind"] == expected_source_kind
    assert replay.result.review_status == "REVIEWABLE"
    assert replay.slot_plan.safe_to_apply is True
    assert replay.slot_plan.operations == ()
    assert replay.slot_plan.conflicts == ()
    assert replay.slot_plan.desired_documents == first.slot_plan.desired_documents


def test_cancelled_bout_is_retained_and_only_updates_slot_current_state():
    card = build_clean_card()
    bouts = list(copy.deepcopy(card.bouts))
    bouts[1]["status"] = "cancelled"

    result = build_card_dry_run(replace(card, bouts=tuple(bouts)))

    assert result.snapshot_id is not None
    assert dict(result.counts)["desired_bouts"] == 2
    assert dict(result.counts)["slot_updates"] >= 1
    assert "slot_deletes" not in dict(result.counts)


def test_completed_event_without_final_results_is_blocked():
    card = build_clean_card()
    event = {**card.event, "status": "completed"}
    bouts = tuple({**bout, "status": "completed"} for bout in card.bouts)

    result = build_card_dry_run(replace(card, event=event, bouts=bouts))

    assert result.review_status == "BLOCKED"
    assert "CAPABILITY_RES_NOT_READY_FOR_COMPLETED_EVENT" in finding_codes(result)


def test_json_and_markdown_are_sanitized_and_include_recovery_gate():
    report = build_backfill_dry_run([build_clean_card()])

    json_text = render_json(report)
    markdown_text = render_markdown(report)
    combined = json_text + markdown_text

    assert FIGHTER_NAME_MARKER not in combined
    assert "mongodb+srv" not in combined
    assert "private_uri" not in combined
    assert "Writes executed: `NO`" in markdown_text
    assert "Production write authorized: `NO`" in markdown_text
    assert "SCR-012" in markdown_text
    assert json.loads(json_text)["raw_documents_retained"] is False


def test_fetcher_uses_only_allowlisted_projected_reads():
    card = build_clean_card()
    database = FakeDatabase([card.event], card.bouts, card.slots)

    loaded = fetch_backfill_cards(database, (card.spec,))

    assert len(loaded) == 1
    assert database.requested_collections == [
        "events",
        "bouts",
        "event_card_slots",
    ]
    assert database.collections["events"].calls[0] == (
        "find_one",
        {"id": card.spec.event_id},
        BACKFILL_EVENT_PROJECTION,
        {"max_time_ms": 10_000},
    )
    assert database.collections["bouts"].calls[0][:3] == (
        "find",
        {"event_id": card.spec.event_id},
        BACKFILL_BOUT_PROJECTION,
    )
    assert database.collections["event_card_slots"].calls[0][:3] == (
        "find",
        {"event_id": card.spec.event_id},
        BACKFILL_SLOT_PROJECTION,
    )


def test_fetcher_rejects_any_event_outside_the_q014_allowlist():
    database = FakeDatabase()
    spec = GoldenCardSpec(999999, "Unapproved", ())

    with pytest.raises(BackfillDryRunConfigurationError, match="allowlist"):
        fetch_backfill_cards(database, (spec,))

    assert database.requested_collections == []


def test_successful_cli_writes_only_the_explicit_report(monkeypatch, tmp_path):
    import tapology_scraper.backfill_reconciliation_report as report_module

    report = build_backfill_dry_run([build_clean_card()])
    monkeypatch.setattr(
        report_module,
        "load_mongo_settings",
        lambda _path: ("mongodb://not-used", "ufc"),
    )
    monkeypatch.setattr(
        report_module,
        "run_backfill_dry_run",
        lambda _uri, _database_name, _specs, _attestation: report,
    )
    output = tmp_path / "dry-run.json"

    exit_code = main(
        ["--format", "json", "--output", str(output)],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert exit_code == EXIT_REVIEWABLE
    assert json.loads(output.read_text(encoding="utf-8"))["write_executed"] is False
    assert sorted(path.name for path in tmp_path.iterdir()) == ["dry-run.json"]


def test_cli_returns_blocked_exit_for_completed_dry_run(monkeypatch):
    import tapology_scraper.backfill_reconciliation_report as report_module

    card = build_clean_card()
    blocked = build_backfill_dry_run(
        [replace(card, event={**card.event, "name": "Wrong card"})]
    )
    monkeypatch.setattr(
        report_module,
        "load_mongo_settings",
        lambda _path: ("mongodb://not-used", "ufc"),
    )
    monkeypatch.setattr(
        report_module,
        "run_backfill_dry_run",
        lambda _uri, _database_name, _specs, _attestation: blocked,
    )

    assert main([], stdout=io.StringIO(), stderr=io.StringIO()) == EXIT_BLOCKED


def test_cli_driver_error_is_redacted(monkeypatch):
    import tapology_scraper.backfill_reconciliation_report as report_module

    secret = "mongodb+srv://user:secret@example.invalid/"
    monkeypatch.setattr(
        report_module,
        "load_mongo_settings",
        lambda _path: (secret, "ufc"),
    )

    def fail(_uri, _database_name, _specs, _attestation):
        raise RuntimeError(f"connection failed at {secret}")

    monkeypatch.setattr(report_module, "run_backfill_dry_run", fail)
    stderr = io.StringIO()

    exit_code = main([], stdout=io.StringIO(), stderr=stderr)

    assert exit_code == EXIT_CONFIGURATION_ERROR
    assert "RuntimeError" in stderr.getvalue()
    assert secret not in stderr.getvalue()
    assert "secret" not in stderr.getvalue().lower()


def test_report_cannot_overwrite_credential_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("MONGODB_URI=mongodb://private", encoding="utf-8")
    stderr = io.StringIO()

    exit_code = main(
        ["--env-file", str(env_file), "--output", str(env_file)],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_CONFIGURATION_ERROR
    assert "must not overwrite" in stderr.getvalue()
    assert env_file.read_text(encoding="utf-8") == "MONGODB_URI=mongodb://private"


def test_cli_loads_attestation_but_keeps_write_authorization_false(
    monkeypatch,
    tmp_path,
):
    import tapology_scraper.backfill_reconciliation_report as report_module

    card = build_clean_card()
    attestation = build_title_attestation(card)
    manifest = tmp_path / "title.json"
    manifest.write_text(
        json.dumps(attestation.canonical_payload()),
        encoding="utf-8",
    )
    output = tmp_path / "report.json"
    captured = {}
    monkeypatch.setattr(
        report_module,
        "load_mongo_settings",
        lambda _path: ("mongodb://not-used", "ufc"),
    )

    def fake_run(_uri, _database_name, _specs, loaded_attestation):
        captured["attestation"] = loaded_attestation
        return build_backfill_dry_run([card], loaded_attestation)

    monkeypatch.setattr(report_module, "run_backfill_dry_run", fake_run)

    exit_code = main(
        [
            "--admin-title-attestation",
            str(manifest),
            "--format",
            "json",
            "--output",
            str(output),
        ],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    value = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == EXIT_REVIEWABLE
    assert captured["attestation"].attestation_id == attestation.attestation_id
    assert value["production_write_authorized"] is False
    assert value["admin_title_attestation"]["production_write_authorized"] is False


def test_report_cannot_overwrite_attestation_input(tmp_path):
    attestation = build_title_attestation()
    manifest = tmp_path / "title.json"
    original = json.dumps(attestation.canonical_payload())
    manifest.write_text(original, encoding="utf-8")
    stderr = io.StringIO()

    exit_code = main(
        [
            "--admin-title-attestation",
            str(manifest),
            "--output",
            str(manifest),
        ],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_CONFIGURATION_ERROR
    assert "must not overwrite" in stderr.getvalue()
    assert manifest.read_text(encoding="utf-8") == original


def test_source_has_no_database_mutation_methods_or_out_of_scope_collections():
    import tapology_scraper.backfill_reconciliation_report as report_module

    source = Path(report_module.__file__).read_text(encoding="utf-8")
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
