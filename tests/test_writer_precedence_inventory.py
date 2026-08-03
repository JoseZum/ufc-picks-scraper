import ast
import io
from pathlib import Path

from tapology_scraper.writer_precedence_inventory import (
    BOUNDARY_OWNED_FIELDS,
    BOUNDARY_SCANNED_FILES,
    CAPABILITIES,
    CORE_WRITER_FILES,
    CURRENTLY_UNWRITTEN_V1_FIELDS,
    DISCREPANCIES,
    LEGACY_WRITE_ALLOWANCES,
    MUTATION_FILES,
    PURE_OBSERVATION_MODULES,
    WRITER_PATHS,
    _CanonicalWriteVisitor,
    evidence_line,
    main,
    render_markdown,
    scan_candidate_mutation_files,
    scan_canonical_collection_writes,
    validate_canonical_write_surface,
    validate_inventory,
)


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_WRITER = "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py"
ESPN_SPIDER = "ufc-picks-scraper/tapology_scraper/spiders/espn.py"


def scan_source(source: str, path: str = ESPN_SPIDER):
    visitor = _CanonicalWriteVisitor(path)
    visitor.visit(ast.parse(source))
    return visitor.sites


def test_every_candidate_mutation_file_is_explicitly_classified():
    expected = tuple(sorted(item.path for item in MUTATION_FILES))

    assert scan_candidate_mutation_files(WORKSPACE_ROOT) == expected


def test_inventory_source_files_and_evidence_are_current():
    assert validate_inventory(WORKSPACE_ROOT) == ()

    for discrepancy in DISCREPANCIES:
        for evidence in discrepancy.evidence:
            assert evidence_line(WORKSPACE_ROOT, evidence) >= 1


def test_espn_slot_and_result_discrepancies_now_cite_the_boundary():
    """WP-004/WP-008 must point at the boundary, not at deleted legacy code."""

    cited = {
        discrepancy.discrepancy_id: {
            (evidence.path, evidence.symbol) for evidence in discrepancy.evidence
        }
        for discrepancy in DISCREPANCIES
    }

    assert (ESPN_SPIDER, "_process_card") in cited["WP-004"]
    assert (
        "ufc-picks-scraper/tapology_scraper/slot_reconciliation.py",
        "plan_slot_reconciliation",
    ) in cited["WP-004"]
    assert (ESPN_SPIDER, "_assign_points_for_new_results") in cited["WP-008"]
    assert (CANONICAL_WRITER, "legacy_event_projection") in cited["WP-012"]
    assert (
        "ufc-picks-scraper/tapology_scraper/card_observation_sources.py",
        "build_espn_card_observations",
    ) in cited["WP-005"]


def test_writer_and_discrepancy_ids_are_unique_and_referentially_complete():
    writer_ids = [item.writer_id for item in WRITER_PATHS]
    discrepancy_ids = [item.discrepancy_id for item in DISCREPANCIES]

    assert len(writer_ids) == len(set(writer_ids))
    assert len(discrepancy_ids) == len(set(discrepancy_ids))
    assert {
        "WP-001",
        "WP-002",
        "WP-003",
        "WP-004",
        "WP-005",
        "WP-010",
    } <= set(discrepancy_ids)
    assert set().union(*(set(item.capabilities) for item in WRITER_PATHS)) == set(
        CAPABILITIES
    )


def test_critical_discrepancies_cover_identity_structure_cancellation_and_eligibility():
    critical = {
        item.discrepancy_id: set(item.capabilities)
        for item in DISCREPANCIES
        if item.severity == "critical"
    }

    assert critical["WP-001"] == {"EVT", "BOUT"}
    assert critical["WP-002"] == {"STRUCT"}
    assert critical["WP-003"] == {"STRUCT"}
    assert critical["WP-005"] >= {"BOUT", "ELIG", "STRUCT"}
    assert critical["WP-010"] == {"ELIG"}


def test_title_contract_makes_admin_true_and_false_durable_and_exclusive():
    title_gap = next(
        item for item in DISCREPANCIES if item.discrepancy_id == "WP-007"
    )
    rendered = render_markdown(WORKSPACE_ROOT)

    assert "Admin is the sole canonical owner" in title_gap.v1_requirement
    assert "Both true and false are explicit durable overrides" in title_gap.v1_requirement
    assert "ESPN/Tapology signals are advisory" in title_gap.v1_requirement
    assert "Admin es la única autoridad canónica" in rendered
    assert "incluyendo `false`" in rendered


def test_boundary_owned_fields_match_the_canonical_writer_declaration():
    """The inventory must not drift from the boundary it is policing."""

    from tapology_scraper.canonical_card_writer import (
        CANONICAL_FIELDS_BY_COLLECTION,
    )

    assert set(BOUNDARY_OWNED_FIELDS) == set(CANONICAL_FIELDS_BY_COLLECTION)
    for collection, fields in CANONICAL_FIELDS_BY_COLLECTION.items():
        assert set(BOUNDARY_OWNED_FIELDS[collection]) == set(fields)


def test_every_canonical_collection_write_is_declared_and_non_canonical():
    assert validate_canonical_write_surface(WORKSPACE_ROOT) == ()

    observed = dict(scan_canonical_collection_writes(WORKSPACE_ROOT))
    declared = {item.key for item in LEGACY_WRITE_ALLOWANCES}

    assert set(observed) == declared
    assert CANONICAL_WRITER in BOUNDARY_SCANNED_FILES
    assert ESPN_SPIDER in BOUNDARY_SCANNED_FILES
    # The ESPN spider still writes bouts and events, but only auxiliary fields.
    for key, roots in observed.items():
        if key[0] != ESPN_SPIDER:
            continue
        assert not set(roots) & set(BOUNDARY_OWNED_FIELDS[key[2]])


def test_only_the_duplicate_cleanup_may_delete_and_the_boundary_never_does():
    deletes = {
        key[1]
        for key in dict(scan_canonical_collection_writes(WORKSPACE_ROOT))
        if key[3].startswith("delete")
    }

    assert deletes == {"EspnSpider._remove_safe_espn_duplicate"}
    assert not [
        item
        for item in LEGACY_WRITE_ALLOWANCES
        if item.path == CANONICAL_WRITER and item.operation.startswith("delete")
    ]


def test_a_new_writer_regaining_canonical_authority_is_rejected():
    """A reintroduced insert-only slot writer must fail the inventory."""

    regression = (
        "class EspnSpider:\n"
        "    def _ensure_card_slot(self, bout):\n"
        "        self.db.event_card_slots.update_one(\n"
        '            {"bout_id": bout["id"]},\n'
        '            {"$setOnInsert": {"card_section": "main", "order_overall": 1}},\n'
        "            upsert=True,\n"
        "        )\n"
    )

    sites = scan_source(regression)

    assert len(sites) == 1
    key, roots = sites[0]
    assert key == (
        ESPN_SPIDER,
        "EspnSpider._ensure_card_slot",
        "event_card_slots",
        "update_one",
    )
    # Undeclared symbol -> caught as a new writer; canonical roots -> caught in place.
    assert key not in {item.key for item in LEGACY_WRITE_ALLOWANCES}
    assert set(roots) & set(BOUNDARY_OWNED_FIELDS["event_card_slots"]) == {
        "card_section",
        "order_overall",
    }


def test_auxiliary_dotted_and_fstring_writes_are_not_treated_as_canonical():
    auxiliary = (
        "class EspnSpider:\n"
        "    def _link_competitors(self, bout, corner):\n"
        "        self.db.bouts.update_one(\n"
        '            {"id": bout["id"]},\n'
        '            {"$set": {f"fighters.{corner}.espn_id": 1}},\n'
        "        )\n"
    )

    (_, roots), = scan_source(auxiliary)

    assert roots == ("fighters",)
    assert not set(roots) & set(BOUNDARY_OWNED_FIELDS["bouts"])


def test_observation_adapters_stay_pure():
    for path in PURE_OBSERVATION_MODULES:
        source = (WORKSPACE_ROOT / path).read_text(encoding="utf-8")
        assert ".update_one(" not in source
        assert ".delete_many(" not in source
        assert ".insert_one(" not in source


def test_required_v1_fields_are_absent_from_current_core_writers():
    combined = "\n".join(
        (WORKSPACE_ROOT / path).read_text(encoding="utf-8")
        for path in CORE_WRITER_FILES
    )

    assert not [field for field in CURRENTLY_UNWRITTEN_V1_FIELDS if field in combined]


def test_the_v1_fields_moved_to_the_boundary_rather_than_vanishing():
    """Legacy writers stay out of V1 fields *because* the boundary owns them."""

    boundary = (WORKSPACE_ROOT / CANONICAL_WRITER).read_text(encoding="utf-8")

    assert not [
        field for field in CURRENTLY_UNWRITTEN_V1_FIELDS if field not in boundary
    ]


def test_report_is_deterministic_and_contains_scr008_handoff():
    first = render_markdown(WORKSPACE_ROOT)
    second = render_markdown(WORKSPACE_ROOT)

    assert first == second
    assert "# Writer and precedence discrepancy report — SCR-007" in first
    assert "WP-001" in first
    assert "same-date" in first
    assert "Admin > ESPN explícito > inferencia" in first
    assert "normalize(observations, previous_snapshot)" in first
    assert "MongoDB URI" not in first
    assert "mongodb+srv://" not in first


def test_cli_check_and_render_to_explicit_output(tmp_path):
    check_stdout = io.StringIO()
    check_stderr = io.StringIO()
    check_exit = main(
        ["--workspace-root", str(WORKSPACE_ROOT), "--check"],
        stdout=check_stdout,
        stderr=check_stderr,
    )
    output = tmp_path / "writer-report.md"
    render_exit = main(
        [
            "--workspace-root",
            str(WORKSPACE_ROOT),
            "--output",
            str(output),
        ],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert check_exit == 0
    assert check_stdout.getvalue().strip() == "WRITER_INVENTORY_OK"
    assert check_stderr.getvalue() == ""
    assert render_exit == 0
    assert output.read_text(encoding="utf-8") == render_markdown(WORKSPACE_ROOT)


def test_inventory_module_has_no_database_network_or_mutation_boundary():
    import tapology_scraper.writer_precedence_inventory as inventory_module

    source = Path(inventory_module.__file__).read_text(encoding="utf-8")
    forbidden_imports = (
        "import pymongo",
        "from pymongo",
        "import requests",
        "from requests",
        "import scrapy",
        "from scrapy",
    )
    forbidden_methods = {
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "delete_one",
        "delete_many",
        "bulk_write",
    }
    executable_mutations = [
        node.func.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in forbidden_methods
    ]

    assert not any(value in source for value in forbidden_imports)
    assert executable_mutations == []
