"""Static writer/precedence inventory for SCR-007.

The inventory reads local source files only.  It never imports application
writers, opens MongoDB, performs network access, or mutates product data.  Its
purpose is to keep the SCR-008 normalizer handoff tied to evidence that still
exists in the scraper and backend source trees.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, TextIO


CAPABILITIES = ("EVT", "EVT_DATE", "BOUT", "ELIG", "STRUCT", "TITLE", "RES")
MUTATION_FILE_CATEGORIES = (
    "canonical_boundary",
    "boundary_routed_writer",
    "core_card_writer",
    "migration_orchestrator",
    "auxiliary_enrichment",
    "generic_mutation_interface",
    "non_card_mutation",
)
SEVERITIES = ("critical", "high", "medium", "low")

MUTATION_METHODS = (
    "insert_one",
    "insert_many",
    "update_one",
    "update_many",
    "replace_one",
    "delete_one",
    "delete_many",
    "bulk_write",
    "find_one_and_update",
    "find_one_and_replace",
)
MUTATION_PATTERN = re.compile(
    r"\.(?:" + "|".join(MUTATION_METHODS) + r")\s*\("
)
CARD_TOKEN_PATTERN = re.compile(r"\b(?:events|bouts|event_card_slots)\b")

#: Collections whose canonical facts belong exclusively to the SCR-015/016
#: continuous boundary.
CANONICAL_COLLECTIONS = ("bouts", "event_card_slots", "events")

#: Field roots the canonical boundary owns, declared here independently of
#: :mod:`tapology_scraper.canonical_card_writer` so this inventory stays a
#: purely static reader.  ``test_writer_precedence_inventory`` cross-checks the
#: two declarations, so drift between them fails the suite instead of silently
#: widening what a legacy writer may touch.
BOUNDARY_OWNED_FIELDS = {
    "events": frozenset(
        {
            "card_data_snapshot_v1",
            "card_data_v1",
            "status",
            "official_event_date",
            "official_date_timezone",
            "month_key",
            "card_start_time_utc",
            "picks_lock_time_utc",
            "section_start_times_utc",
            "section_lock_times_utc",
            "first_final_result_at",
            "main_event_bout_id",
            "total_bouts",
            "listed_bout_count",
            "mission_eligible_bout_count",
            "lifecycle_revision",
            "structure_revision",
            "timing_revision",
            "current_eligibility",
            "timing_source",
        }
    ),
    "bouts": frozenset(
        {
            "card_data_v1",
            "status",
            "result",
            "is_title_fight",
            "is_bmf_title_fight",
            "title_type",
            "lineage_id",
            "matchup_revision",
            "replaces_bout_id",
            "replaced_by_bout_id",
            "result_revision",
            "card_section",
            "card_order",
            "order_overall",
            "order_section",
            "is_main_event",
            "is_co_main",
            "is_co_main_event",
            "scheduled_start_time_utc",
            "automatic_lock_time_utc",
        }
    ),
    "event_card_slots": frozenset(
        {
            "slot_id",
            "is_current",
            "card_section",
            "order_overall",
            "order_section",
            "role",
            "is_main_event",
            "is_co_main",
            "is_co_main_event",
            "scheduled_start_time_utc",
            "automatic_lock_time_utc",
            "structure_revision",
            "evidence",
            "card_snapshot_revision",
            "canonical_slot_version",
            "source",
        }
    ),
}

#: Modules that convert a source payload into typed observations.  They must
#: stay pure: an observation adapter that starts writing documents has bypassed
#: the boundary by definition.
PURE_OBSERVATION_MODULES = (
    "ufc-picks-scraper/tapology_scraper/card_observation_sources.py",
)


@dataclass(frozen=True)
class MutationFile:
    path: str
    category: str
    role: str


@dataclass(frozen=True)
class WriterPath:
    writer_id: str
    label: str
    layer: str
    source_kind: str
    paths: tuple[str, ...]
    symbols: tuple[str, ...]
    collections: tuple[str, ...]
    operations: tuple[str, ...]
    capabilities: tuple[str, ...]
    field_families: tuple[str, ...]
    current_precedence: str
    idempotency: str
    contract_status: str


@dataclass(frozen=True)
class LegacyWriteAllowance:
    """One reviewed mutation call site on a canonical CardData collection.

    The inventory pins the *exact* set of call sites that files classified
    ``canonical_boundary`` or ``boundary_routed_writer`` are allowed to
    execute.  A newly added writer function in one of those files does not
    appear here, so it fails validation instead of silently regaining
    canonical authority.
    """

    path: str
    symbol: str
    collection: str
    operation: str
    field_roots: tuple[str, ...]
    note: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.path, self.symbol, self.collection, self.operation)


@dataclass(frozen=True)
class SourceEvidence:
    path: str
    symbol: str
    needle: str
    note: str


@dataclass(frozen=True)
class Discrepancy:
    discrepancy_id: str
    severity: str
    title: str
    capabilities: tuple[str, ...]
    writer_ids: tuple[str, ...]
    observed: str
    risk: str
    v1_requirement: str
    scr008_action: str
    evidence: tuple[SourceEvidence, ...]


MUTATION_FILES = (
    MutationFile(
        "ufc-picks-scraper/legacy/backfill_2026.py",
        "migration_orchestrator",
        "One-time event alias/identity restoration around the ESPN writer.",
    ),
    MutationFile(
        "ufc-picks-scraper/legacy/backfill_2026_timing.py",
        "migration_orchestrator",
        "Runs the ESPN timing writer and records a migration marker.",
    ),
    MutationFile(
        "ufc-picks-scraper/legacy/fix_nationality_data.py",
        "auxiliary_enrichment",
        "Repairs embedded fighter nationality only.",
    ),
    MutationFile(
        "ufc-picks-scraper/legacy/fix_ranking_data.py",
        "auxiliary_enrichment",
        "Repairs embedded fighter ranking only.",
    ),
    MutationFile(
        "ufc-picks-scraper/ingest.py",
        "core_card_writer",
        "Legacy JSONL Tapology event/bout ingest, cleanup and deletion path.",
    ),
    MutationFile(
        "ufc-picks-scraper/legacy/scheduler.py",
        "migration_orchestrator",
        "Legacy scrape-window marker and Tapology ingest orchestrator.",
    ),
    MutationFile(
        "ufc-picks-scraper/legacy/sync_image_keys.py",
        "auxiliary_enrichment",
        "Synchronizes embedded fighter image keys only.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/pipelines.py",
        "core_card_writer",
        "Direct asynchronous Tapology event/bout upsert pipeline.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
        "canonical_boundary",
        (
            "SCR-015/016 continuous CardData boundary; the only path allowed to "
            "resolve and persist canonical event/bout/slot facts."
        ),
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/production_backfill_package.py",
        "migration_orchestrator",
        "Exact-run CardData sidecar/slot backfill with transaction and drift guards.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "boundary_routed_writer",
        (
            "ESPN observation source; CardData facts travel through the canonical "
            "boundary and only auxiliary/alias fields are written directly."
        ),
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/spiders/event_images.py",
        "auxiliary_enrichment",
        "Owns event poster/hero image fields, outside CardData mission facts.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/spiders/fighter_images.py",
        "auxiliary_enrichment",
        "Owns embedded fighter image fields.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/spiders/ufc_fighters.py",
        "auxiliary_enrichment",
        "Updates embedded fighter profile facts in bouts.",
    ),
    MutationFile(
        "ufc-picks-scraper/tapology_scraper/spiders/ufc_images.py",
        "auxiliary_enrichment",
        "Legacy embedded fighter image updater.",
    ),
    MutationFile(
        "ufc-picks-backend/app/controllers/admin_controller.py",
        "core_card_writer",
        "Active Admin timing/result/lifecycle/title/slot mutation endpoints.",
    ),
    MutationFile(
        "ufc-picks-backend/app/repositories/bout_repository.py",
        "generic_mutation_interface",
        "Dormant generic bout create/update/delete capability without provenance.",
    ),
    MutationFile(
        "ufc-picks-backend/app/repositories/event_repository.py",
        "generic_mutation_interface",
        "Dormant generic event/slot create/update/delete capability without provenance.",
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/bout_evaluation.py",
        "non_card_mutation",
        "Reads canonical bout results but mutates only mission assignments and runs.",
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/card_control.py",
        "non_card_mutation",
        (
            "Admin close/reopen/VOID of the mission window. Reads `events` to "
            "verify the card exists but mutates only mission card controls and "
            "mission assignments — never a CardData field."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/card_streak.py",
        "non_card_mutation",
        (
            "Reads bout lifecycle and picks to settle STREAK-001, but mutates only "
            "the mission streak collections, the XP ledger and celebrations."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/progression.py",
        "non_card_mutation",
        (
            "Aggregates the XP ledger and mutates only the progression cache and "
            "the celebration queue."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/services/admin_card_commands.py",
        "boundary_routed_writer",
        (
            "Persists an Admin decision as a durable CardData command in "
            "`admin_card_commands`. Writes no CardData collection itself: the "
            "command is replayed as an `admin_override` observation on every "
            "reconciliation, which is what makes the decision outlast an ESPN "
            "pass (D-DATA-010)."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/services/canonical_authority.py",
        "boundary_routed_writer",
        (
            "Stamps the Admin authority evidence that makes a title decision "
            "durable (D-DATA-010), and is the read every lower-authority writer "
            "performs before touching a canonical field."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/card_finalization.py",
        "non_card_mutation",
        "Reads a frozen card digest but mutates only mission finalization runs.",
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/monthly_progress.py",
        "non_card_mutation",
        "Reads events but mutates only the mission monthly progress collection.",
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/read_models.py",
        "non_card_mutation",
        "Reads card facts but mutates only mission offer sets.",
    ),
    MutationFile(
        "ufc-picks-backend/app/modules/missions/application/selection.py",
        "non_card_mutation",
        (
            "Reads canonical event/bout facts while its transaction mutates only "
            "mission assignments, command receipts and user picks."
        ),
    ),
    MutationFile(
        "ufc-picks-backend/app/services/pick_service.py",
        "non_card_mutation",
        "Reads card data but mutations target picks, not CardData collections.",
    ),
    MutationFile(
        "ufc-picks-backend/app/services/points_service.py",
        "non_card_mutation",
        "Reads bouts but mutations target picks/users, not CardData facts.",
    ),
)


WRITER_PATHS = (
    WriterPath(
        "W-CANONICAL-BOUNDARY",
        "Continuous CardData normalizer/reconciler boundary",
        "scraper",
        "canonical_boundary",
        ("ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",),
        (
            "plan_canonical_card_write",
            "submit_card_observations",
            "MongoCanonicalCardStore.commit_card_write",
        ),
        ("events", "bouts", "event_card_slots"),
        ("update_one", "setOnInsert/upsert"),
        CAPABILITIES,
        (
            "canonical snapshot sidecars",
            "reconciled event-card slots",
            "derived legacy compatibility projection",
        ),
        (
            "Precedence is resolved by CardDataNormalizerV1 from source rank and "
            "persisted field evidence, so Admin overrides and TITLE authority "
            "survive every later ESPN/Tapology observation."
        ),
        (
            "Digest-guarded event-scoped commit; an immediate replay of the same "
            "observations produces zero operations and zero field diffs."
        ),
        "canonical",
    ),
    WriterPath(
        "W-CARDDATA-BACKFILL",
        "Guarded CardData V1 production backfill",
        "scraper",
        "admin_attested_canonical_projection",
        ("ufc-picks-scraper/tapology_scraper/production_backfill_package.py",),
        (
            "prepare_backfill_run",
            "MongoCardDataBackfillAdapter.execute",
        ),
        ("events", "bouts", "event_card_slots"),
        ("update_one", "insert_one", "transaction"),
        CAPABILITIES,
        (
            "card_data_v1 compatibility sidecars",
            "canonical event-card slots",
            "Admin TITLE baseline",
        ),
        (
            "Exact content-derived run/plan authorization plus in-transaction "
            "preimage digests; Admin TITLE is injected before projection."
        ),
        (
            "Dry-run by default; exact run authorization, all-scope preflight, one "
            "transaction, post-commit verification and converged replay."
        ),
        "compatible_guarded",
    ),
    WriterPath(
        "W-TAPOLOGY-PIPELINE",
        "Tapology Scrapy pipeline",
        "scraper",
        "tapology_legacy",
        ("ufc-picks-scraper/tapology_scraper/pipelines.py",),
        ("MongoDBPipeline.process_event", "MongoDBPipeline.process_bout"),
        ("events", "bouts"),
        ("update_one/upsert",),
        ("EVT", "EVT_DATE", "BOUT", "STRUCT", "TITLE"),
        ("event identity/date", "bout identity/fighters", "flattened structure/title"),
        "Unconditional $set for emitted non-null fields; no field-level source guard.",
        "Retry-safe by key but not convergent with the canonical slot model.",
        "incompatible",
    ),
    WriterPath(
        "W-TAPOLOGY-INGEST",
        "Legacy JSONL ingest and cleanup",
        "scraper",
        "tapology_legacy",
        ("ufc-picks-scraper/ingest.py",),
        (
            "process_event",
            "process_bout",
            "cleanup_pipeline_duplicates",
            "cleanup_deleted_bouts",
            "main",
        ),
        ("events", "bouts"),
        ("insert_one", "update_one", "delete_one", "delete_many"),
        ("EVT", "EVT_DATE", "BOUT", "STRUCT", "TITLE", "RES"),
        ("event/bout snapshots", "flattened structure", "cancellation/deletion"),
        "Last writer wins while scheduled; completed bouts are partially protected by result presence.",
        "Upserts are keyed, but cleanup is destructive and source-completeness is implicit.",
        "incompatible",
    ),
    WriterPath(
        "W-ESPN-EVENT",
        "ESPN event matching and summary upsert",
        "scraper",
        "espn_summary",
        (
            "ufc-picks-scraper/tapology_scraper/espn_etl.py",
            "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        ),
        ("find_event_match", "ESPNSpider._upsert_event"),
        ("events",),
        ("update_one/upsert",),
        ("EVT",),
        ("source alias", "legacy display/image fields"),
        (
            "SCR-015: every canonical event field is filtered out of the ongoing "
            "update; only the source alias and legacy display fields remain."
        ),
        (
            "Keyed upsert; canonical identity/timing now converge through the "
            "boundary in the same pass."
        ),
        "boundary_routed",
    ),
    WriterPath(
        "W-ESPN-STRUCTURE",
        "ESPN card summary and slot insertion",
        "scraper",
        "espn_summary_or_inference",
        (
            "ufc-picks-scraper/tapology_scraper/espn_etl.py",
            "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        ),
        (
            "build_espn_card_observations",
            "ESPNSpider._process_card",
            "ESPNSpider._attach_espn_aliases",
        ),
        ("bouts",),
        ("update_one",),
        ("BOUT",),
        ("source competition alias",),
        (
            "SCR-015: section, order, headline roles and title travel as typed "
            "observations; name matching only establishes the alias."
        ),
        (
            "Slots are reconciled by plan_slot_reconciliation and converge after "
            "reorder, replacement and removal."
        ),
        "boundary_routed",
    ),
    WriterPath(
        "W-ESPN-DETAIL",
        "ESPN competition detail writer",
        "scraper",
        "espn_detail",
        (
            "ufc-picks-scraper/tapology_scraper/espn_etl.py",
            "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        ),
        ("transform_competition_metadata", "ESPNSpider.parse_competition_metadata"),
        ("bouts",),
        ("update_one",),
        ("BOUT",),
        ("espn match number", "espn card segment"),
        (
            "SCR-015: explicit detail is submitted as an espn_detail observation "
            "that outranks scoreboard inference but never an Admin override."
        ),
        (
            "Both order fields are derived once after final section grouping, so "
            "a global match number can no longer become a section-local order."
        ),
        "boundary_routed",
    ),
    WriterPath(
        "W-ESPN-RESULT",
        "ESPN result and event completion writer",
        "scraper",
        "espn_summary",
        ("ufc-picks-scraper/tapology_scraper/spiders/espn.py",),
        (
            "ESPNSpider._submit_card_observations",
            "ESPNSpider._assign_points_for_new_results",
            "ESPNSpider._reconcile_fully_resulted_events",
        ),
        ("picks", "users"),
        ("update_one",),
        ("RES",),
        ("pick scoring", "user totals"),
        (
            "SCR-015: results and lifecycle are canonical; this path only rescores "
            "picks for bouts whose canonical result actually changed."
        ),
        (
            "A corrected source result now raises result_revision and converges "
            "instead of being skipped because a result already exists."
        ),
        "boundary_routed",
    ),
    WriterPath(
        "W-ESPN-DUPLICATE",
        "ESPN duplicate cleanup",
        "scraper",
        "derived",
        ("ufc-picks-scraper/tapology_scraper/spiders/espn.py",),
        ("ESPNSpider._remove_safe_espn_duplicate",),
        ("events", "bouts", "event_card_slots"),
        ("delete_one", "delete_many"),
        ("EVT", "BOUT", "STRUCT"),
        ("duplicate identity", "dependent slots/bouts"),
        "Deletes only source=espn duplicates and refuses when picks exist.",
        "Safe retry after deletion, but no retained lineage/tombstone.",
        "partial",
    ),
    WriterPath(
        "W-ESPN-FIGHTER",
        "ESPN fighter identity/profile enrichment",
        "scraper",
        "espn_detail",
        ("ufc-picks-scraper/tapology_scraper/spiders/espn.py",),
        (
            "ESPNSpider._link_competitors",
            "ESPNSpider._update_embedded_fighter_fields",
            "ESPNSpider._update_scheduled_fighter_ages",
        ),
        ("bouts",),
        ("update_one", "update_many"),
        ("BOUT",),
        ("fighter aliases/names", "profile snapshot", "age at fight"),
        "ESPN IDs enrich matched corners; name matching establishes the initial link.",
        "Keyed enrichment, but matchup identity/revision is not versioned.",
        "partial",
    ),
    WriterPath(
        "W-ADMIN-TIMING",
        "Admin event/bout timing",
        "backend",
        "admin_override",
        ("ufc-picks-backend/app/controllers/admin_controller.py",),
        ("update_event_timing", "update_bout_timing"),
        ("events", "bouts"),
        ("update_one", "update_many"),
        ("EVT_DATE", "STRUCT"),
        ("card/section locks", "legacy per-bout timing"),
        "Event timing_source=admin is protected by ESPN event/detail paths, not by Tapology ingest.",
        "Repeatable values, but no evidence object or timing revision.",
        "partial",
    ),
    WriterPath(
        "W-ADMIN-RESULT",
        "Admin result and lifecycle controls",
        "backend",
        "admin_override",
        ("ufc-picks-backend/app/controllers/admin_controller.py",),
        (
            "update_bout_result",
            "delete_bout_result",
            "complete_event",
            "_complete_event_when_all_results_exist",
        ),
        ("events", "bouts"),
        ("update_one", "update_many"),
        ("EVT", "BOUT", "RES"),
        ("result", "bout/event lifecycle"),
        "Admin can overwrite/delete results, but no persistent precedence or correction metadata is stored.",
        "Commands are not ledgered/versioned; retries can recalculate side effects.",
        "incompatible",
    ),
    WriterPath(
        "W-ADMIN-CARD",
        "Admin bout/title/slot editor",
        "backend",
        "admin_override",
        ("ufc-picks-backend/app/controllers/admin_controller.py",),
        ("update_bout_details",),
        ("bouts", "event_card_slots"),
        ("update_one",),
        ("BOUT", "STRUCT", "TITLE"),
        ("rounds/weight/title", "section/order/headline roles"),
        "Admin is the intended TITLE authority, but title and structure are stored without durable source/evidence/revision markers.",
        "Partial updates are keyed but can be overwritten and leave flattened fields divergent.",
        "incompatible",
    ),
    WriterPath(
        "W-ADMIN-REMOVAL",
        "Admin cancellation and hard deletion",
        "backend",
        "admin_override",
        ("ufc-picks-backend/app/controllers/admin_controller.py",),
        ("cancel_bout", "delete_bout"),
        ("events", "bouts", "event_card_slots"),
        ("update_one", "delete_one"),
        ("EVT", "BOUT", "ELIG", "STRUCT", "RES"),
        ("cancellation", "hard deletion", "event count"),
        "Admin can cancel or physically delete; both paths also delete picks.",
        "Cancellation is retryable; hard deletion erases mission target history.",
        "incompatible",
    ),
    WriterPath(
        "W-GENERIC-REPOSITORY",
        "Generic backend repositories",
        "backend",
        "untyped_caller",
        (
            "ufc-picks-backend/app/repositories/event_repository.py",
            "ufc-picks-backend/app/repositories/bout_repository.py",
        ),
        ("EventRepository.update", "BoutRepository.update"),
        ("events", "bouts", "event_card_slots"),
        ("insert", "find_one_and_update", "delete"),
        CAPABILITIES,
        ("arbitrary caller-provided updates",),
        "No provenance or field allowlist; currently no active mutation call sites were found.",
        "Potentially idempotent by key, but caller semantics are unconstrained.",
        "dormant_risk",
    ),
    WriterPath(
        "W-MIGRATION-ORCHESTRATORS",
        "One-time backfill and scheduler paths",
        "scraper",
        "migration_or_legacy",
        (
            "ufc-picks-scraper/legacy/backfill_2026.py",
            "ufc-picks-scraper/legacy/backfill_2026_timing.py",
            "ufc-picks-scraper/legacy/scheduler.py",
        ),
        ("run_backfill", "backfill_2026_timing.main", "scheduler module loop"),
        ("events",),
        ("update_one", "subprocess writer invocation"),
        ("EVT", "EVT_DATE"),
        ("one-off alias", "timing orchestration", "legacy scrape markers"),
        "Bypasses a unified observation boundary and invokes existing writers directly.",
        "Migration markers help once-only execution; card effects inherit invoked writer behavior.",
        "partial",
    ),
)


DISCREPANCIES = (
    Discrepancy(
        "WP-001",
        "critical",
        "A same-date singleton can be accepted as the wrong event identity",
        ("EVT", "BOUT"),
        ("W-ESPN-EVENT",),
        "When no ESPN alias matches and exactly one local event shares the date, the matcher returns it without a name threshold.",
        "The wrong ESPN event/competitions can become permanently attached to a Tapology event, as observed for Q-014 event 135755.",
        "Identity reconciliation must require a trusted alias or sufficient independent evidence and quarantine contradictions.",
        "Introduce an EventObservation matcher that scores alias/date/name, rejects a conflicting existing alias, and emits an explicit quarantine instead of mutating.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/espn_etl.py",
                "find_event_match",
                "if len(date_matches) == 1:\n        return date_matches[0]",
                "Date-only singleton shortcut.",
            ),
            SourceEvidence(
                "mission-planning/GOLDEN_PRODUCTION_CARD_AUDIT.md",
                "card 135755",
                "EXPECTED_ESPN_EVENT_ALIAS_MISSING",
                "Production audit detects the approved alias contradiction.",
            ),
        ),
    ),
    Discrepancy(
        "WP-002",
        "critical",
        "Admin structure overrides are not protected from ESPN detail",
        ("STRUCT",),
        ("W-ADMIN-CARD", "W-ESPN-DETAIL"),
        "Admin updates slot fields but does not set source/evidence; an ESPN-created slot remains source=espn and is therefore eligible for later ESPN overwrite.",
        "The documented Admin > ESPN precedence is not durable.",
        "Every resolved field must retain winning provenance and lower-precedence observations must not overwrite it.",
        "Store field-level Admin evidence/revision through the normalizer and make reconciliation compare observations rather than mutate the slot directly.",
        (
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "update_bout_details",
                '{"$set": slot_update}',
                "Admin slot update carries no source/evidence marker.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
                "parse_competition_metadata",
                "self._submit_card_observations(int(event_id))",
                (
                    "SCR-015: ESPN detail no longer reads the slot source; it enters "
                    "the boundary as an observation and loses to an Admin override."
                ),
            ),
        ),
    ),
    Discrepancy(
        "WP-003",
        "critical",
        "ESPN detail writes global match number into section-local order",
        ("STRUCT",),
        ("W-ESPN-DETAIL",),
        "order_overall and order_section both receive espn_match_number.",
        "Every prelim/early-prelim section violates the V1 contiguous 1..N invariant.",
        "order_section must restart at 1 inside each section and be derived after final section grouping.",
        "Normalize all current slots together, sort within resolved sections, and derive both order fields once.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/card_data_normalizer.py",
                "_normalize_slots",
                'slot["order_section"] = section_counts[section]',
                (
                    "SCR-015: order_section restarts at 1 per section and is derived "
                    "after final grouping, never copied from a global match number."
                ),
            ),
            SourceEvidence(
                "mission-planning/GOLDEN_PRODUCTION_CARD_AUDIT.md",
                "Capability states",
                "SECTION_ORDER_GAP",
                "All audited cards expose the resulting invariant failure.",
            ),
        ),
    ),
    Discrepancy(
        "WP-004",
        "high",
        "Slots are insert-only and cannot converge after card changes",
        ("STRUCT", "ELIG"),
        ("W-ESPN-STRUCTURE", "W-ESPN-DETAIL"),
        "The summary path returns when any slot exists and otherwise uses $setOnInsert; detail updates only ESPN-source slots and never reconciles missing/orphan/current state.",
        "Reorders, replacements and removed bouts leave stale or contradictory placement.",
        "A snapshot reconciliation must upsert desired current slots and retire prior placements idempotently.",
        "Make SCR-008 produce desired slots plus change sets; SCR-009 will apply them with current/revision semantics.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
                "_process_card",
                "self._submit_card_observations(event_id)",
                (
                    "SCR-015: the insert-only _ensure_card_slot path is gone; the "
                    "summary card is submitted to the boundary instead."
                ),
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/slot_reconciliation.py",
                "plan_slot_reconciliation",
                'operations.append(_operation("update", current, desired, changed))',
                (
                    "Existing slots are now reconciled against the desired snapshot "
                    "instead of being skipped."
                ),
            ),
        ),
    ),
    Discrepancy(
        "WP-005",
        "critical",
        "Cancellation/removal semantics conflict and may erase targets/picks",
        ("BOUT", "ELIG", "STRUCT", "RES"),
        (
            "W-TAPOLOGY-PIPELINE",
            "W-TAPOLOGY-INGEST",
            "W-ESPN-STRUCTURE",
            "W-ADMIN-REMOVAL",
        ),
        "Tapology skips/deletes cancelled or missing bouts, ESPN marks them cancelled, Admin cancellation deletes picks, and Admin hard deletion removes bout plus slot.",
        "Mission targets, denominators and audit history can disappear or disagree by execution path.",
        "Cancelled/replaced bouts remain as non-current lineage members; affected targeted missions VOID without deleting historical picks/assignments.",
        "Convert every source outcome to cancellation/removal observations; let the normalizer produce tombstones and frozen eligibility effects.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/pipelines.py",
                "MongoDBPipeline.process_bout",
                'if item.get("cancelled") or item.get("status") == "cancelled":',
                "Pipeline discards the observation.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/ingest.py",
                "process_bout",
                'bouts_col.delete_one({"_id": bout_id})',
                "Legacy ingest physically deletes a cancelled bout.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/card_observation_sources.py",
                "build_espn_card_observations",
                "if previous_status in TERMINAL_BOUT_STATUSES and not completed:",
                (
                    "SCR-015: a cancelled/replaced bout is retained with its "
                    "lifecycle state and is never revived or deleted by a refetch."
                ),
            ),
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "cancel_bout",
                'delete_result = await db["picks"].delete_many({"bout_id": bout_id})',
                "Admin cancellation deletes canonical picks.",
            ),
        ),
    ),
    Discrepancy(
        "WP-006",
        "high",
        "Structure has two mutable authorities",
        ("STRUCT",),
        ("W-TAPOLOGY-INGEST", "W-ESPN-STRUCTURE", "W-ESPN-DETAIL", "W-ADMIN-CARD"),
        "card_section/order/headline fields exist on bouts and event_card_slots; writers update them independently.",
        "Consumers can see different cards depending on which collection they read.",
        "The current slot snapshot is canonical; flattened bout fields are a compatibility projection from the same resolved snapshot.",
        "Return one normalized CardData result and derive both slot documents and temporary flattened fields from it.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
                "legacy_bout_projection",
                '"card_section": canonical_slot.get("card_section"),',
                (
                    "SCR-015: flattened bout structure is now derived from the "
                    "canonical slot instead of being an independent authority."
                ),
            ),
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "update_bout_details",
                'slot_update["card_section"] = body.card_section',
                "Admin mutates slot structure only.",
            ),
        ),
    ),
    Discrepancy(
        "WP-007",
        "high",
        "Title facts are incomplete and unproven",
        ("TITLE",),
        ("W-TAPOLOGY-PIPELINE", "W-ESPN-STRUCTURE", "W-ADMIN-CARD"),
        "ESPN titleFight is persisted only when creating a new ESPN bout, while Tapology writes a default boolean on every emitted bout. Admin can set or remove title state, but its true/false choice has no durable authority marker.",
        "A later scraper run can reactivate a title fight that Admin removed or remove one that Admin marked, because explicit false is indistinguishable from an untrusted/default false.",
        "Admin is the sole canonical owner of is_title_fight, is_bmf_title_fight and title_type. Both true and false are explicit durable overrides; ESPN/Tapology signals are advisory and never overwrite Admin.",
        "Model nullable Admin TITLE authority separately from external suggestions, preserve explicit false, increment the title/structure revision on change, and resolve only the Admin value into canonical TITLE fields.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/espn_etl.py",
                "transform_new_bout",
                '"is_title_fight": bool(competition.get("titleFight")),',
                "Title evidence is only used on new ESPN bout creation.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/pipelines.py",
                "MongoDBPipeline.process_bout",
                '"is_title_fight": item.get("is_title_fight", False),',
                "Tapology writes a default title boolean without respecting Admin authority.",
            ),
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "update_bout_details",
                'bout_update["is_title_fight"] = body.is_title_fight',
                "Admin stores its choice only as a boolean, without a durable override marker.",
            ),
        ),
    ),
    Discrepancy(
        "WP-008",
        "high",
        "Result correction precedence is implicit and unversioned",
        ("RES", "EVT"),
        ("W-ESPN-RESULT", "W-ADMIN-RESULT"),
        "ESPN refuses to touch any existing result; Admin overwrites or deletes it, but neither path records result_revision, evidence or correction lineage.",
        "Retries/corrections cannot be reconciled deterministically and downstream XP compensation has no stable source revision.",
        "Results require final/corrected status, source evidence, monotonic revision and correction timestamps.",
        "Normalize result observations separately and emit a semantic revision only when the canonical result changes.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
                "_assign_points_for_new_results",
                'if "result" not in write.changed_fields:',
                (
                    "SCR-015: an existing result no longer blocks ESPN; rescoring "
                    "follows the canonical result change instead of first-write-wins."
                ),
            ),
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "update_bout_result",
                '"result": result_data,',
                "Admin replaces the result without version metadata.",
            ),
        ),
    ),
    Discrepancy(
        "WP-009",
        "high",
        "Admin timing precedence is narrow and date semantics remain legacy",
        ("EVT_DATE", "STRUCT"),
        ("W-TAPOLOGY-INGEST", "W-ESPN-EVENT", "W-ESPN-DETAIL", "W-ADMIN-TIMING"),
        "timing_source=admin protects selected ESPN writes, but Tapology ingest has no equivalent guard; both ESPN and Admin persist timezone=ET and omit frozen official date/month authority.",
        "Monthly inclusion and lock times can drift by writer order or ambiguous timezone labels.",
        "Admin > ESPN explicit > inference must apply per field with IANA timezone, official_event_date and month_key.",
        "Represent timing observations independently, resolve per field, derive month_key from the frozen official date and keep the winning evidence.",
        (
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "build_event_timing_updates",
                'updates["timing_source"] = "admin"',
                "Only event timing receives an Admin marker.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/espn_etl.py",
                "transform_event",
                '"timezone": "ET",',
                "Legacy non-IANA timezone is persisted.",
            ),
        ),
    ),
    Discrepancy(
        "WP-010",
        "critical",
        "No writer freezes mission eligibility",
        ("ELIG",),
        ("W-TAPOLOGY-INGEST", "W-ESPN-STRUCTURE", "W-ADMIN-REMOVAL"),
        "No active core writer persists current_eligibility, eligible/excluded targets, denominator, fingerprint or the revisions used to derive them.",
        "Offers and streak denominators cannot remain stable across reorder, cancellation or replacement.",
        "Every canonical snapshot needs a deterministic eligibility projection and frozen assignment-time snapshot.",
        "Have SCR-008 compute eligibility from normalized current bouts/slots; SCR-009/010 persist and transition it atomically with structure changes.",
        (
            SourceEvidence(
                "mission-planning/GOLDEN_PRODUCTION_CARD_AUDIT.md",
                "production cards",
                "ELIGIBILITY_SNAPSHOT_MISSING",
                "All three audited production cards lack the snapshot.",
            ),
        ),
    ),
    Discrepancy(
        "WP-011",
        "high",
        "Semantic revisions and field evidence are absent",
        ("EVT", "BOUT", "STRUCT", "TITLE", "RES"),
        tuple(
            path.writer_id
            for path in WRITER_PATHS
            if path.writer_id != "W-GENERIC-REPOSITORY"
        ),
        "Current writers mostly retain timestamps and coarse source labels, not lifecycle/structure/timing/matchup/result revisions or field evidence.",
        "A retry cannot be distinguished from a semantic change and lower-confidence data can silently replace authority.",
        "Resolved facts retain evidence and revisions increase only when their semantic family changes.",
        "Build a pure diff/revision allocator around the normalized snapshot before any writer migration.",
        (
            SourceEvidence(
                "mission-planning/GOLDEN_PRODUCTION_CARD_AUDIT.md",
                "production cards",
                "EVENT_REVISIONS_MISSING",
                "Production audit confirms event revisions are absent.",
            ),
            SourceEvidence(
                "mission-planning/GOLDEN_PRODUCTION_CARD_AUDIT.md",
                "production cards",
                "MATCHUP_REVISION_MISSING",
                "Production audit confirms matchup revisions are absent.",
            ),
        ),
    ),
    Discrepancy(
        "WP-012",
        "high",
        "total_bouts and completion use incompatible populations",
        ("EVT", "ELIG", "RES"),
        ("W-TAPOLOGY-INGEST", "W-ESPN-STRUCTURE", "W-ADMIN-RESULT", "W-ADMIN-REMOVAL"),
        "ESPN writes len(competitions), Tapology writes scraped count, Admin hard deletion writes remaining documents, while Admin completion excludes cancelled bouts but compares against event total_bouts.",
        "Cards with cancellations can fail to close or close under different denominators depending on the path.",
        "listed_bout_count, mission_eligible_bout_count and completion population must be separate explicit facts.",
        "Derive each count from the same normalized snapshot and remove total_bouts from mission/completion authority.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
                "legacy_event_projection",
                '"total_bouts": len(slots),',
                (
                    "SCR-015: the count is derived from current canonical slots, not "
                    "from the length of a source list."
                ),
            ),
            SourceEvidence(
                "ufc-picks-backend/app/controllers/admin_controller.py",
                "_complete_event_when_all_results_exist",
                'active_bouts = [\n        bout for bout in bouts if bout.get("status") != "cancelled"',
                "Completion removes cancelled bouts before comparing to event total.",
            ),
        ),
    ),
    Discrepancy(
        "WP-013",
        "high",
        "Replacement and identity lineage are not modeled",
        ("BOUT", "ELIG", "STRUCT"),
        ("W-TAPOLOGY-INGEST", "W-ESPN-EVENT", "W-ESPN-FIGHTER"),
        "ESPN creates competition IDs directly and otherwise matches bouts by fighter names; Tapology uses its own bout ID. No path creates lineage_id, matchup_revision or replaces/replaced_by links.",
        "A fighter replacement can overwrite/mismatch an existing target or appear as an unrelated deletion/insertion.",
        "Changed fighter set creates a new target under stable lineage; corner-only swaps preserve identity.",
        "Make identity resolution an explicit pure step that returns matched/new/replacement/quarantined before data projection.",
        (
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/espn_etl.py",
                "find_bout_match",
                "return candidate if score >= 0.78 else None",
                "Fighter-name similarity is the fallback identity rule.",
            ),
            SourceEvidence(
                "ufc-picks-scraper/tapology_scraper/espn_etl.py",
                "transform_new_bout",
                '"id": competition_id,',
                "Unmatched ESPN competition ID becomes the canonical ID.",
            ),
        ),
    ),
    Discrepancy(
        "WP-014",
        "medium",
        "Generic repositories can bypass all precedence rules",
        CAPABILITIES,
        ("W-GENERIC-REPOSITORY",),
        "Repository update methods accept arbitrary dictionaries and append only last_updated; no active production call sites were found.",
        "A future caller could accidentally reintroduce an untracked writer after SCR-008.",
        "CardData writes must enter through the normalizer/reconciliation command boundary.",
        "Deprecate generic CardData mutation methods or constrain them to already-normalized projections with explicit provenance.",
        (
            SourceEvidence(
                "ufc-picks-backend/app/repositories/event_repository.py",
                "EventRepository.update",
                'updates["last_updated"] = datetime.utcnow()',
                "Arbitrary event updates receive only a timestamp.",
            ),
            SourceEvidence(
                "ufc-picks-backend/app/repositories/bout_repository.py",
                "BoutRepository.update",
                'updates["last_updated"] = datetime.utcnow()',
                "Arbitrary bout updates receive only a timestamp.",
            ),
        ),
    ),
)


LEGACY_WRITE_ALLOWANCES = (
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
        "MongoCanonicalCardStore.commit_card_write",
        "events",
        "update_one",
        ("<canonical projection>",),
        "The boundary itself persists the resolved event projection.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
        "MongoCanonicalCardStore.commit_card_write",
        "bouts",
        "update_one",
        ("<canonical projection>",),
        "The boundary seeds and updates bouts from the resolved snapshot.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/canonical_card_writer.py",
        "MongoCanonicalCardStore.commit_card_write",
        "event_card_slots",
        "update_one",
        ("<reconciled slot document>",),
        "Slots are upserted from plan_slot_reconciliation; never deleted.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._attach_espn_aliases",
        "bouts",
        "update_one",
        ("espn_competition_id",),
        "Establishes the source alias only; every fact then travels as an observation.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._upsert_event",
        "events",
        "update_one",
        ("espn_event_id", "espn_url", "espn_last_updated", "<legacy display fields>"),
        (
            "Canonical event fields are filtered out through CANONICAL_EVENT_FIELDS; "
            "only the alias and legacy display/image fields remain."
        ),
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._link_competitors",
        "bouts",
        "update_one",
        ("espn_competition_id", "espn_last_updated", "fighters"),
        "Embedded fighter profile enrichment around the canonical fighter reference.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider.parse_competition_metadata",
        "bouts",
        "update_one",
        ("espn_match_number", "espn_card_segment", "espn_last_updated"),
        "Auxiliary ESPN bookkeeping; section/timing/rounds go through the boundary.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._update_scheduled_fighter_ages",
        "bouts",
        "update_one",
        ("fighters",),
        "Writes fighters.<corner>.age_at_fight_years only.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._remove_safe_espn_duplicate",
        "events",
        "delete_one",
        (),
        (
            "Removes a duplicate source=espn event document, never a canonical card; "
            "refuses when picks exist."
        ),
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._remove_safe_espn_duplicate",
        "bouts",
        "delete_many",
        (),
        "Removes bouts of the duplicate source=espn event only.",
    ),
    LegacyWriteAllowance(
        "ufc-picks-scraper/tapology_scraper/spiders/espn.py",
        "EspnSpider._remove_safe_espn_duplicate",
        "event_card_slots",
        "delete_many",
        (),
        "Removes slots of the duplicate source=espn event only.",
    ),
)


#: V1 fields no writer produces yet. `result_revision` and `title_type` left
#: this list once the canonical result projection and the Admin title authority
#: module began governing them; the check exists to notice exactly that.
CURRENTLY_UNWRITTEN_V1_FIELDS = (
    "official_event_date",
    "official_date_timezone",
    "month_key",
    "current_eligibility",
    "lineage_id",
    "matchup_revision",
    "structure_revision",
)

CORE_WRITER_FILES = tuple(
    item.path
    for item in MUTATION_FILES
    if item.category in {"core_card_writer", "boundary_routed_writer"}
)


def scan_candidate_mutation_files(workspace_root: Path) -> tuple[str, ...]:
    """Find files that combine mutation calls with card-data collection tokens."""

    roots = (
        workspace_root / "ufc-picks-scraper",
        workspace_root / "ufc-picks-backend" / "app",
    )
    candidates = []
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*.py"):
            if any(part in {"tests", "__pycache__", ".venv"} for part in path.parts):
                continue
            if path.name == Path(__file__).name:
                continue
            source = path.read_text(encoding="utf-8")
            if MUTATION_PATTERN.search(source) and CARD_TOKEN_PATTERN.search(source):
                candidates.append(path.relative_to(workspace_root).as_posix())
    return tuple(sorted(candidates))


BOUNDARY_FILE_CATEGORIES = ("canonical_boundary", "boundary_routed_writer")

BOUNDARY_SCANNED_FILES = tuple(
    item.path
    for item in MUTATION_FILES
    if item.category in set(BOUNDARY_FILE_CATEGORIES)
)


def _qualified_name(stack: Sequence[str]) -> str:
    return ".".join(stack) if stack else "<module>"


def _literal_field_roots(node: ast.AST) -> tuple[str, ...]:
    """Collect statically visible field roots inside an update payload.

    Only string literals are considered.  An f-string such as
    ``f"fighters.{corner}.espn_id"`` still contributes its literal ``fighters``
    root, which is exactly the granularity the boundary owns.
    """

    roots: set[str] = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Constant) or not isinstance(child.value, str):
            continue
        text = child.value
        if text.startswith("$"):
            continue
        root = text.split(".", 1)[0].strip()
        if root:
            roots.add(root)
    return tuple(sorted(roots))


class _CanonicalWriteVisitor(ast.NodeVisitor):
    """Find every mutation call aimed at a canonical CardData collection."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.stack: list[str] = []
        self.sites: list[tuple[tuple[str, str, str, str], tuple[str, ...]]] = []

    def _visit_scope(self, node: ast.AST) -> None:
        self.stack.append(getattr(node, "name", "<anonymous>"))
        self.generic_visit(node)
        self.stack.pop()

    visit_ClassDef = _visit_scope
    visit_FunctionDef = _visit_scope
    visit_AsyncFunctionDef = _visit_scope

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr in MUTATION_METHODS
            and isinstance(func.value, ast.Attribute)
            and func.value.attr in CANONICAL_COLLECTIONS
        ):
            payloads = list(node.args[1:2]) + [
                keyword.value
                for keyword in node.keywords
                if keyword.arg in {"update", "replacement"}
            ]
            roots: set[str] = set()
            for payload in payloads:
                roots.update(_literal_field_roots(payload))
            self.sites.append(
                (
                    (
                        self.path,
                        _qualified_name(self.stack),
                        func.value.attr,
                        func.attr,
                    ),
                    tuple(sorted(roots)),
                )
            )
        self.generic_visit(node)


def scan_canonical_collection_writes(
    workspace_root: Path,
) -> tuple[tuple[tuple[str, str, str, str], tuple[str, ...]], ...]:
    """Return every canonical-collection mutation site in boundary files.

    Sites are deduplicated by ``(path, symbol, collection, operation)`` and the
    literal field roots of every call sharing that key are merged, so two
    ``update_one`` calls in one function are one reviewable declaration.
    """

    merged: dict[tuple[str, str, str, str], set[str]] = {}
    for path in BOUNDARY_SCANNED_FILES:
        source_path = workspace_root / path
        if not source_path.is_file():
            continue
        visitor = _CanonicalWriteVisitor(path)
        visitor.visit(ast.parse(source_path.read_text(encoding="utf-8")))
        for key, roots in visitor.sites:
            merged.setdefault(key, set()).update(roots)
    return tuple(
        (key, tuple(sorted(merged[key]))) for key in sorted(merged)
    )


def validate_canonical_write_surface(workspace_root: Path) -> tuple[str, ...]:
    """Check that only declared, non-canonical legacy writes remain.

    Two independent failures are reported:

    * an undeclared (or vanished) canonical-collection call site, which is how
      a new writer would silently regain authority; and
    * a declared boundary-routed call site whose literal payload names a field
      the canonical boundary owns, which is how an existing writer would
      regain authority in place.
    """

    errors: list[str] = []
    declared = {item.key: item for item in LEGACY_WRITE_ALLOWANCES}
    observed = dict(scan_canonical_collection_writes(workspace_root))

    undeclared = sorted(set(observed) - set(declared))
    for key in undeclared:
        errors.append(
            "Undeclared canonical-collection write: "
            f"{key[0]}::{key[1]} -> {key[3]}({key[2]})"
        )
    vanished = sorted(set(declared) - set(observed))
    for key in vanished:
        errors.append(
            "Declared canonical-collection write no longer exists: "
            f"{key[0]}::{key[1]} -> {key[3]}({key[2]})"
        )

    category_by_path = {item.path: item.category for item in MUTATION_FILES}
    for key, roots in sorted(observed.items()):
        if category_by_path.get(key[0]) == "canonical_boundary":
            continue
        owned = BOUNDARY_OWNED_FIELDS.get(key[2], frozenset())
        offending = sorted(set(roots) & owned)
        if offending:
            errors.append(
                f"{key[0]}::{key[1]} writes boundary-owned {key[2]} fields "
                f"{offending}; those facts must travel as observations."
            )

    for key in sorted(observed):
        if (
            category_by_path.get(key[0]) == "canonical_boundary"
            and key[3].startswith("delete")
        ):
            errors.append(
                f"The canonical boundary must never delete: {key[0]}::{key[1]}."
            )

    for path in PURE_OBSERVATION_MODULES:
        source_path = workspace_root / path
        if not source_path.is_file():
            errors.append(f"Declared observation module is missing: {path}")
            continue
        if MUTATION_PATTERN.search(source_path.read_text(encoding="utf-8")):
            errors.append(
                f"Observation adapter {path} performs a mutation; it must stay pure."
            )
    return tuple(errors)


def evidence_line(workspace_root: Path, evidence: SourceEvidence) -> int:
    source_path = workspace_root / evidence.path
    source = source_path.read_text(encoding="utf-8")
    search_start = 0
    if source_path.suffix == ".py":
        symbol_name = evidence.symbol.rsplit(".", 1)[-1]
        symbol_offsets = [
            offset
            for declaration in (
                f"def {symbol_name}(",
                f"async def {symbol_name}(",
                f"class {symbol_name}",
            )
            if (offset := source.find(declaration)) >= 0
        ]
        if symbol_offsets:
            search_start = min(symbol_offsets)
    offset = source.find(evidence.needle, search_start)
    if offset < 0:
        raise ValueError(f"Evidence for {evidence.symbol} is stale in {evidence.path}.")
    return source.count("\n", 0, offset) + 1


def validate_inventory(workspace_root: Path) -> tuple[str, ...]:
    errors = []
    classified_paths = tuple(sorted(item.path for item in MUTATION_FILES))
    scanned_paths = scan_candidate_mutation_files(workspace_root)
    if scanned_paths != classified_paths:
        missing = sorted(set(scanned_paths) - set(classified_paths))
        stale = sorted(set(classified_paths) - set(scanned_paths))
        if missing:
            errors.append(f"Unclassified candidate mutation files: {missing}")
        if stale:
            errors.append(f"Classified files no longer detected: {stale}")

    writer_ids = [item.writer_id for item in WRITER_PATHS]
    if len(writer_ids) != len(set(writer_ids)):
        errors.append("Writer IDs must be unique.")
    discrepancy_ids = [item.discrepancy_id for item in DISCREPANCIES]
    if len(discrepancy_ids) != len(set(discrepancy_ids)):
        errors.append("Discrepancy IDs must be unique.")

    known_writers = set(writer_ids)
    for writer in WRITER_PATHS:
        if not set(writer.capabilities) <= set(CAPABILITIES):
            errors.append(f"{writer.writer_id} has unknown capabilities.")
        for path in writer.paths:
            if not (workspace_root / path).is_file():
                errors.append(f"{writer.writer_id} source file is missing: {path}")

    for discrepancy in DISCREPANCIES:
        if discrepancy.severity not in SEVERITIES:
            errors.append(f"{discrepancy.discrepancy_id} has invalid severity.")
        unknown = set(discrepancy.writer_ids) - known_writers
        if unknown:
            errors.append(
                f"{discrepancy.discrepancy_id} references unknown writers: {sorted(unknown)}"
            )
        if not set(discrepancy.capabilities) <= set(CAPABILITIES):
            errors.append(f"{discrepancy.discrepancy_id} has unknown capabilities.")
        for evidence in discrepancy.evidence:
            try:
                evidence_line(workspace_root, evidence)
            except (OSError, UnicodeError, ValueError) as error:
                errors.append(f"{discrepancy.discrepancy_id}: {error}")

    core_text = "\n".join(
        (workspace_root / path).read_text(encoding="utf-8")
        for path in CORE_WRITER_FILES
    )
    unexpectedly_written = [
        field for field in CURRENTLY_UNWRITTEN_V1_FIELDS if field in core_text
    ]
    if unexpectedly_written:
        errors.append(
            "Inventory assumption is stale; V1 fields now appear in core writers: "
            f"{unexpectedly_written}"
        )
    errors.extend(validate_canonical_write_surface(workspace_root))
    return tuple(errors)


def _escape(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _evidence_links(workspace_root: Path, evidence: Sequence[SourceEvidence]) -> str:
    links = []
    for item in evidence:
        line = evidence_line(workspace_root, item)
        label = f"{item.path}:{line}"
        links.append(f"[{label}](../{item.path}#L{line})")
    return ", ".join(links)


def render_markdown(workspace_root: Path) -> str:
    errors = validate_inventory(workspace_root)
    if errors:
        raise ValueError("; ".join(errors))

    severity_counts = {
        severity: sum(item.severity == severity for item in DISCREPANCIES)
        for severity in SEVERITIES
    }
    category_counts = {
        category: sum(item.category == category for item in MUTATION_FILES)
        for category in MUTATION_FILE_CATEGORIES
    }
    lines = [
        "# Writer and precedence discrepancy report — SCR-007",
        "",
        "## Resultado ejecutivo",
        "",
        "Este inventario es estático y local: inspecciona código fuente, no importa los writers, no abre MongoDB, no usa red y no modifica datos. Su objetivo es fijar el punto de partida que SCR-008 debe normalizar.",
        "",
        f"- Superficie candidata clasificada: **{len(MUTATION_FILES)} archivos**.",
        f"- Rutas lógicas de escritura: **{len(WRITER_PATHS)}**.",
        f"- Discrepancias: **{len(DISCREPANCIES)}** — critical={severity_counts['critical']}, high={severity_counts['high']}, medium={severity_counts['medium']}, low={severity_counts['low']}.",
        "- Causa raíz de Q-014: el fallback de identidad acepta un único evento de la misma fecha sin validar el nombre (`WP-001`).",
        "- Precedencia actual: solo el timing Admin está protegido parcialmente; estructura, título, lifecycle, resultado y cancelación dependen del último writer o de guardas incompatibles.",
        "- Decisión TITLE: Admin es la única autoridad canónica; `true` y `false` son overrides explícitos y persistentes. ESPN/Tapology solo pueden sugerir.",
        "- Campos V1 sin writer actual: `official_event_date`, `official_date_timezone`, `month_key`, `current_eligibility`, `lineage_id`, `matchup_revision`, `structure_revision`, `result_revision`, `title_type`.",
        "",
        "## Precedencia efectiva vs. contrato",
        "",
        "| Familia | Precedencia efectiva actual | Contrato V1 | Estado |",
        "|---|---|---|---|",
        "| Event identity | Alias ESPN si ya existe; si no, fecha y a veces nombre; un único match de fecha gana solo | Alias confiable + evidencia independiente; contradicción se cuarentena | `BLOCKED` |",
        "| Event timing/month | Admin vence a ESPN en rutas puntuales; Tapology puede volver a escribir; timezone `ET` | Admin > ESPN explícito > inferencia; IANA + official date/month congelados | `BLOCKED` |",
        "| Bout identity/replacement | ESPN ID nuevo o fuzzy match por nombres; Tapology ID separado | Lineage estable + matchup revision + aliases | `BLOCKED` |",
        "| Structure | Mezcla de bout y slot; summary/detail/Admin escriben por separado | Slot snapshot único; flattening derivado | `QUARANTINED` |",
        "| Title | Booleano Tapology/ESPN-nuevo/Admin sin autoridad durable; gana el último writer | Admin exclusivo; true/false/type persistentes; fuentes externas solo sugieren | `BLOCKED` |",
        "| Result | ESPN first-write-wins; Admin overwrite/delete | Revisión monotónica + corrección + evidencia | `DEGRADED` |",
        "| Cancellation | Skip/delete/mark-cancelled/hard-delete según ruta | Retain target/lineage; current=false; VOID explícito | `BLOCKED` |",
        "| Eligibility | No existe writer | Snapshot determinístico con denominator/fingerprint | `BLOCKED` |",
        "",
        "## Rutas lógicas de escritura",
        "",
        "| ID | Ruta | Layer/source | Colecciones | Capacidades | Precedencia actual | Idempotencia | Estado V1 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for writer in WRITER_PATHS:
        lines.append(
            "| "
            + " | ".join(
                (
                    f"`{writer.writer_id}`",
                    _escape(writer.label),
                    _escape(f"{writer.layer} / {writer.source_kind}"),
                    _escape(", ".join(writer.collections)),
                    _escape(", ".join(writer.capabilities)),
                    _escape(writer.current_precedence),
                    _escape(writer.idempotency),
                    f"`{writer.contract_status}`",
                )
            )
            + " |"
        )

    lines.extend(["", "## Discrepancias accionables", ""])
    for discrepancy in DISCREPANCIES:
        lines.extend(
            [
                f"### {discrepancy.discrepancy_id} — {discrepancy.title}",
                "",
                f"- Severidad: `{discrepancy.severity}`",
                f"- Capacidades: `{', '.join(discrepancy.capabilities)}`",
                f"- Writers: `{', '.join(discrepancy.writer_ids)}`",
                f"- Evidencia: {_evidence_links(workspace_root, discrepancy.evidence)}",
                f"- Observado: {discrepancy.observed}",
                f"- Riesgo: {discrepancy.risk}",
                f"- Requisito V1: {discrepancy.v1_requirement}",
                f"- Acción SCR-008: {discrepancy.scr008_action}",
                "",
            ]
        )

    lines.extend(
        [
            "## Cobertura de archivos mutantes",
            "",
            "La detección conservadora exige clasificar cualquier archivo Python que combine una llamada mutante con referencias a `events`, `bouts` o `event_card_slots`. Algunos son falsos positivos intencionales porque leen CardData pero solo escriben picks/users.",
            "",
            "| Archivo | Clasificación | Rol |",
            "|---|---|---|",
        ]
    )
    for item in MUTATION_FILES:
        lines.append(
            f"| `{_escape(item.path)}` | `{item.category}` | {_escape(item.role)} |"
        )

    lines.extend(
        [
            "",
            "Conteos por clasificación: "
            + ", ".join(
                f"`{category}`={category_counts[category]}"
                for category in MUTATION_FILE_CATEGORIES
            )
            + ".",
            "",
            "## Contrato de entrada para SCR-008",
            "",
            "SCR-008 debe implementar un normalizador puro; todavía no debe migrar writers ni escribir producción. La secuencia recomendada es:",
            "",
            "1. Definir observaciones discriminadas para `admin_override`, `espn_detail`, `espn_summary`, `tapology_explicit`, `time_group_inference` y fallback cuarentenado.",
            "2. Resolver identidad de event/bout antes de proyectar campos; ningún match por fecha única puede mutar sin evidencia de nombre/alias.",
            "3. Resolver precedencia por campo y conservar evidencia del ganador y de contradicciones relevantes; TITLE usa exclusivamente el override Admin, incluyendo `false`, mientras ESPN/Tapology quedan como sugerencias.",
            "4. Producir un único `CardDataContractV1` con event, bouts, current slots, results y eligibility; derivar el flattening legacy desde ese resultado.",
            "5. Calcular diffs semánticos determinísticos y revisiones por familia, sin timestamps dentro del core puro.",
            "6. Exponer `normalize(observations, previous_snapshot) -> normalized_snapshot + change_set + quarantines`.",
            "7. Probar los fixtures SCR-003 y regresiones directas para WP-001..WP-014; ninguna ruta de Mongo participa en esos tests.",
            "",
            "## Fuera de alcance",
            "",
            "- No se modificó ningún writer actual.",
            "- No se consultó ni escribió MongoDB.",
            "- No se ejecutó scraping ni backfill.",
            "- La reconciliación persistente de slots corresponde a SCR-009; cancelación/reemplazo a SCR-010; cualquier backfill requiere SCR-011/012.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and render the local SCR-007 writer inventory."
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Workspace containing ufc-picks-scraper and ufc-picks-backend.",
    )
    parser.add_argument("--output", type=Path, help="Optional Markdown output path.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate source coverage/evidence without rendering Markdown.",
    )
    return parser


def main(
    argv: Optional[Sequence[str]] = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    args = build_parser().parse_args(argv)
    workspace_root = args.workspace_root.resolve()
    errors = validate_inventory(workspace_root)
    if errors:
        for error in errors:
            print(f"INVENTORY_ERROR: {error}", file=stderr)
        return 1
    if args.check:
        print("WRITER_INVENTORY_OK", file=stdout)
        return 0

    rendered = render_markdown(workspace_root)
    if args.output:
        output = args.output.resolve()
        if output.name.lower() == ".env":
            print(
                "INVENTORY_ERROR: output must not overwrite an environment file.",
                file=stderr,
            )
            return 2
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, file=stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
