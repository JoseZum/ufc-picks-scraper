import copy
import io
import json
from pathlib import Path

import pytest

from tapology_scraper.mission_readiness_audit import (
    AUDIT_SCHEMA_VERSION,
    EXIT_FINDINGS,
    EXIT_INPUT_ERROR,
    EXIT_READY,
    AuditInputError,
    SnapshotInput,
    audit_snapshots,
    load_snapshot_inputs,
    main,
    normalize_required_capabilities,
    render_json,
    render_markdown,
)


OBSERVED_AT = "2026-07-31T18:00:00Z"


def build_valid_empty_snapshot(event_id: int = 990100100) -> dict:
    snapshot_revision = 1
    return {
        "contract_version": "card-data/v1",
        "normalizer_version": "1.0.0",
        "snapshot_id": f"{event_id}:{snapshot_revision}",
        "snapshot_revision": snapshot_revision,
        "generated_at": OBSERVED_AT,
        "source_run": {
            "run_id": f"fixture-run-{event_id}",
            "observed_at": OBSERVED_AT,
            "sources": [
                {
                    "source": "espn_summary",
                    "source_event_id": str(event_id),
                    "observed_at": OBSERVED_AT,
                    "payload_hash": f"sha256:fixture-{event_id}",
                }
            ],
            "previous_snapshot_revision": None,
        },
        "event": {
            "event_id": event_id,
            "source_ids": {"espn_event_id": str(event_id)},
            "promotion": "UFC",
            "name": f"UFC Fixture {event_id}",
            "subtitle": None,
            "official_event_date": "2026-08-15",
            "official_date_timezone": "America/New_York",
            "month_key": "2026-08",
            "status": "scheduled",
            "card_start_time_utc": None,
            "picks_lock_time_utc": None,
            "section_start_times_utc": {},
            "section_lock_times_utc": {},
            "first_final_result_at": None,
            "main_event_bout_id": None,
            "listed_bout_count": 0,
            "mission_eligible_bout_count": 0,
            "lifecycle_revision": 1,
            "structure_revision": 1,
            "timing_revision": 1,
            "created_at": OBSERVED_AT,
            "canonical_updated_at": OBSERVED_AT,
            "evidence": {},
        },
        "bouts": [],
        "card_slots": [],
        "current_eligibility": {
            "eligibility_snapshot_id": f"elig_{event_id}_{snapshot_revision}",
            "kind": "current",
            "event_id": event_id,
            "card_snapshot_revision": snapshot_revision,
            "created_at": OBSERVED_AT,
            "eligible_targets": [],
            "excluded_targets": [],
            "denominator": 0,
            "fingerprint": f"sha256:eligibility-{event_id}",
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
                    "message": "No explicit title evidence is present.",
                    "blocks_capabilities": ["TITLE"],
                }
            ],
        },
    }


def capability(snapshot_audit, capability_id: str):
    return next(
        item for item in snapshot_audit.capabilities if item.capability == capability_id
    )


def write_snapshot(path: Path, snapshot: dict) -> bytes:
    content = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    path.write_bytes(content)
    return content


def test_valid_snapshot_audit_is_pure_and_preserves_nonready_capabilities():
    snapshot = build_valid_empty_snapshot()
    before = copy.deepcopy(snapshot)

    report = audit_snapshots([SnapshotInput("fixture.json", snapshot)])
    audited = report.snapshots[0]

    assert report.passed
    assert report.exit_code == EXIT_READY
    assert audited.contract_valid
    assert audited.required_capabilities_ready
    assert capability(audited, "EVT").effective_state == "ready"
    assert capability(audited, "TITLE").effective_state == "blocked"
    assert capability(audited, "RES").effective_state == "pending"
    assert snapshot == before


def test_contract_issue_quarantines_only_its_blocked_capabilities():
    snapshot = build_valid_empty_snapshot()
    snapshot["event"]["month_key"] = "2026-09"

    report = audit_snapshots(
        [SnapshotInput("invalid.json", snapshot)],
        required_capabilities=("EVT_DATE",),
    )
    audited = report.snapshots[0]

    assert not report.passed
    assert report.exit_code == EXIT_FINDINGS
    assert not audited.contract_valid
    assert not audited.required_capabilities_ready
    assert capability(audited, "EVT_DATE").effective_state == "quarantined"
    assert "MONTH_KEY_MISMATCH" in capability(
        audited, "EVT_DATE"
    ).blocking_issue_codes
    assert capability(audited, "EVT").effective_state == "ready"


def test_declared_blocker_cannot_hide_behind_ready_capability_state():
    snapshot = build_valid_empty_snapshot()
    snapshot["quality"]["capabilities"]["TITLE"] = "ready"

    report = audit_snapshots([SnapshotInput("fixture.json", snapshot)])
    title = capability(report.snapshots[0], "TITLE")

    assert report.passed
    assert title.declared_state == "ready"
    assert title.effective_state == "blocked"
    assert title.blocking_issue_codes == ("TITLE_STATUS_UNKNOWN",)


def test_required_capabilities_are_normalized_in_contract_order():
    assert normalize_required_capabilities(["res,evt", "STRUCT", "evt"]) == (
        "EVT",
        "STRUCT",
        "RES",
    )
    assert normalize_required_capabilities(["ALL"]) == (
        "EVT",
        "EVT_DATE",
        "BOUT",
        "ELIG",
        "STRUCT",
        "TITLE",
        "RES",
    )
    with pytest.raises(AuditInputError, match="Unknown capability"):
        normalize_required_capabilities(["ODDS"])


def test_report_order_and_rendering_are_deterministic():
    later = build_valid_empty_snapshot(990100200)
    earlier = build_valid_empty_snapshot(990100100)
    forward = audit_snapshots(
        [SnapshotInput("later.json", later), SnapshotInput("earlier.json", earlier)]
    )
    reverse = audit_snapshots(
        [SnapshotInput("earlier.json", copy.deepcopy(earlier)), SnapshotInput("later.json", copy.deepcopy(later))]
    )

    assert render_json(forward) == render_json(reverse)
    assert render_markdown(forward) == render_markdown(reverse)
    assert [item.event_id for item in forward.snapshots] == [990100100, 990100200]


def test_json_and_markdown_share_the_same_audit_result():
    report = audit_snapshots(
        [SnapshotInput("fixture.json", build_valid_empty_snapshot())],
        required_capabilities=("EVT", "ELIG"),
    )

    json_report = json.loads(render_json(report))
    markdown_report = render_markdown(report)

    assert json_report["schema_version"] == AUDIT_SCHEMA_VERSION
    assert json_report["passed"] is True
    assert json_report["required_capabilities"] == ["EVT", "ELIG"]
    assert "# Mission Readiness Audit" in markdown_report
    assert "| TITLE | blocked | blocked | TITLE_STATUS_UNKNOWN |" in markdown_report
    assert "Contract issues" in markdown_report


def test_duplicate_snapshot_ids_are_an_audit_finding():
    snapshot = build_valid_empty_snapshot()
    report = audit_snapshots(
        [
            SnapshotInput("first.json", snapshot),
            SnapshotInput("second.json", copy.deepcopy(snapshot)),
        ]
    )

    assert not report.passed
    assert report.exit_code == EXIT_FINDINGS
    assert [issue.code for issue in report.audit_issues] == [
        "DUPLICATE_SNAPSHOT_ID"
    ]


def test_loader_accepts_single_bundle_array_and_sorted_directory(tmp_path):
    first = build_valid_empty_snapshot(990100100)
    second = build_valid_empty_snapshot(990100200)
    single_path = tmp_path / "single.json"
    bundle_path = tmp_path / "bundle.json"
    write_snapshot(single_path, first)
    bundle_path.write_text(
        json.dumps({"snapshots": [first, second]}), encoding="utf-8"
    )

    single = load_snapshot_inputs([str(single_path)])
    bundle = load_snapshot_inputs([str(bundle_path)])
    directory = load_snapshot_inputs([str(tmp_path)])

    assert len(single) == 1
    assert len(bundle) == 2
    assert bundle[0].source.endswith("bundle.json#1")
    assert len(directory) == 3
    assert [Path(item.source.split("#")[0]).name for item in directory] == [
        "bundle.json",
        "bundle.json",
        "single.json",
    ]


def test_loader_accepts_stdin_once():
    stdin = io.StringIO(json.dumps(build_valid_empty_snapshot()))

    loaded = load_snapshot_inputs(["-"], stdin=stdin)

    assert len(loaded) == 1
    assert loaded[0].source == "<stdin>"
    with pytest.raises(AuditInputError, match="only once"):
        load_snapshot_inputs(["-", "-"], stdin=io.StringIO("{}"))


def test_cli_defaults_to_stdout_and_does_not_modify_input_or_create_report(tmp_path):
    input_path = tmp_path / "snapshot.json"
    original = write_snapshot(input_path, build_valid_empty_snapshot())
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = main(
        [str(input_path)],
        stdin=io.StringIO(),
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == EXIT_READY
    assert json.loads(stdout.getvalue())["passed"] is True
    assert stderr.getvalue() == ""
    assert input_path.read_bytes() == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["snapshot.json"]


def test_cli_required_blocked_capability_returns_findings(tmp_path):
    input_path = tmp_path / "snapshot.json"
    write_snapshot(input_path, build_valid_empty_snapshot())
    stdout = io.StringIO()

    exit_code = main(
        [str(input_path), "--require", "TITLE"],
        stdin=io.StringIO(),
        stdout=stdout,
        stderr=io.StringIO(),
    )

    assert exit_code == EXIT_FINDINGS
    report = json.loads(stdout.getvalue())
    assert report["passed"] is False
    assert report["summary"]["not_ready_for_required_count"] == 1


def test_cli_writes_only_an_explicit_report_path(tmp_path):
    input_path = tmp_path / "snapshot.json"
    output_path = tmp_path / "reports" / "readiness.md"
    original = write_snapshot(input_path, build_valid_empty_snapshot())
    stdout = io.StringIO()

    exit_code = main(
        [
            str(input_path),
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
        stdin=io.StringIO(),
        stdout=stdout,
        stderr=io.StringIO(),
    )

    assert exit_code == EXIT_READY
    assert stdout.getvalue() == ""
    assert output_path.read_text(encoding="utf-8").startswith(
        "# Mission Readiness Audit"
    )
    assert input_path.read_bytes() == original


def test_cli_rejects_invalid_json_and_protects_input_paths(tmp_path):
    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text("{not-json", encoding="utf-8")
    stderr = io.StringIO()

    invalid_exit = main(
        [str(invalid_path)],
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=stderr,
    )
    overwrite_exit = main(
        [str(invalid_path), "--output", str(invalid_path)],
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )
    inside_directory_exit = main(
        [str(tmp_path), "--output", str(tmp_path / "report.json")],
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert invalid_exit == EXIT_INPUT_ERROR
    assert "invalid JSON" in stderr.getvalue()
    assert overwrite_exit == EXIT_INPUT_ERROR
    assert inside_directory_exit == EXIT_INPUT_ERROR


def test_audit_module_has_no_database_network_or_scraper_dependency():
    import tapology_scraper.mission_readiness_audit as audit_module

    source = Path(audit_module.__file__).read_text(encoding="utf-8").lower()
    forbidden_imports = (
        "import pymongo",
        "from pymongo",
        "import motor",
        "from motor",
        "import requests",
        "import httpx",
        "import scrapy",
        "mongodb_uri",
    )

    assert not any(fragment in source for fragment in forbidden_imports)
