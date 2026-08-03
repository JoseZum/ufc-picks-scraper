import ast
import copy
from pathlib import Path

import pytest

from tapology_scraper.card_data_normalizer import normalize_card_data_v1
from tapology_scraper.slot_reconciliation import (
    SLOT_STORAGE_VERSION,
    SlotCommitReceipt,
    SlotReconciliationApplyError,
    SlotReconciliationInputError,
    UnsafeSlotReconciliationPlan,
    apply_slot_reconciliation,
    legacy_bout_slot_projection,
    plan_slot_reconciliation,
    slot_collection_digest,
    slot_storage_fingerprint,
)


EVENT_ID = 82001
T1 = "2026-08-01T12:00:00Z"
T2 = "2026-08-01T13:00:00Z"


def observation(
    observation_id,
    entity_type,
    entity_id,
    values,
    *,
    source_kind="espn_detail",
    observed_at=T1,
    reason=None,
    identity_basis="source_competition_id",
):
    return {
        "observation_id": observation_id,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "event_id": EVENT_ID,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_ref": f"fixture:{observation_id}",
        "source_event_id": "fixture-source-event-82001",
        "values": values,
        "identity_basis": identity_basis,
        "reason": reason,
        "payload_hash": f"sha256:{observation_id}",
    }


def fighter_pair(seed):
    return [
        {
            "fighter_id": f"fighter_{seed}_red",
            "display_name": f"Fixture {seed} Red",
            "corner": "red",
            "source_ids": {"espn_athlete_id": f"{seed}1"},
            "identity_confidence": "exact_source",
        },
        {
            "fighter_id": f"fighter_{seed}_blue",
            "display_name": f"Fixture {seed} Blue",
            "corner": "blue",
            "source_ids": {"espn_athlete_id": f"{seed}2"},
            "identity_confidence": "exact_source",
        },
    ]


def build_snapshot():
    observations = [
        observation(
            "event",
            "event",
            EVENT_ID,
            {
                "source_ids": {"espn_event_id": "600082001"},
                "promotion": "UFC",
                "name": "UFC Fixture: Granite vs. Harbor",
                "official_event_date": "2026-08-22",
                "official_date_timezone": "America/New_York",
                "status": "scheduled",
            },
            source_kind="espn_summary",
            identity_basis="source_alias",
        )
    ]
    layout = (
        (301, "main", 1, "2026-08-23T02:00:00Z"),
        (302, "prelim", 2, "2026-08-23T00:00:00Z"),
        (303, "early_prelim", 3, "2026-08-22T22:00:00Z"),
    )
    for bout_id, section, order, start in layout:
        observations.extend(
            [
                observation(
                    f"bout-{bout_id}",
                    "bout",
                    bout_id,
                    {
                        "source_ids": {
                            "espn_competition_id": f"900{bout_id}"
                        },
                        "fighters": fighter_pair(bout_id),
                        "weight_class": "Lightweight",
                        "gender": "male",
                        "scheduled_rounds": 5 if bout_id == 301 else 3,
                        "status": "scheduled",
                    },
                ),
                observation(
                    f"slot-{bout_id}",
                    "slot",
                    bout_id,
                    {
                        "is_current": True,
                        "card_section": section,
                        "order_overall": order,
                        "scheduled_start_time_utc": start,
                        "automatic_lock_time_utc": start,
                    },
                ),
                observation(
                    f"title-{bout_id}",
                    "bout",
                    bout_id,
                    {"is_title_fight": False},
                    source_kind="admin_override",
                    reason="Admin explicitly marked a non-title bout.",
                    identity_basis="admin",
                ),
            ]
        )
    return normalize_card_data_v1(observations).snapshot


def slot(snapshot, bout_id):
    return next(
        item for item in snapshot["card_slots"] if item["bout_id"] == bout_id
    )


def legacy_document(canonical_slot, **updates):
    role = canonical_slot["role"]
    document = {
        "_id": canonical_slot["slot_id"],
        "id": canonical_slot["slot_id"],
        "event_id": canonical_slot["event_id"],
        "bout_id": canonical_slot["bout_id"],
        "card_section": canonical_slot["card_section"],
        "order_overall": canonical_slot["order_overall"],
        "order_section": canonical_slot["order_section"],
        "is_main_event": role == "main_event",
        "is_co_main": role == "co_main",
        "source": "espn",
    }
    document.update(updates)
    return document


class FakeAtomicSlotStore:
    def __init__(self, documents):
        self.documents = [copy.deepcopy(document) for document in documents]
        self.commit_count = 0

    def commit_slot_plan(self, plan):
        current = [
            document
            for document in self.documents
            if document.get("event_id") == plan.event_id
        ]
        if slot_collection_digest(current) != plan.current_digest:
            raise SlotReconciliationApplyError(
                "Optimistic event digest changed before commit."
            )

        staged = copy.deepcopy(self.documents)
        by_id = {
            str(document.get("_id") or document.get("slot_id") or document.get("id")): document
            for document in staged
            if document.get("event_id") == plan.event_id
        }
        for operation in plan.operations:
            current_document = by_id.get(operation.slot_id)
            if operation.action == "insert":
                if current_document is not None:
                    raise SlotReconciliationApplyError(
                        f"Insert race for {operation.slot_id}."
                    )
                inserted = copy.deepcopy(dict(operation.after))
                staged.append(inserted)
                by_id[operation.slot_id] = inserted
                continue
            if current_document is None:
                raise SlotReconciliationApplyError(
                    f"Update target disappeared for {operation.slot_id}."
                )
            if slot_storage_fingerprint(current_document) != (
                operation.expected_fingerprint
            ):
                raise SlotReconciliationApplyError(
                    f"Slot CAS changed for {operation.slot_id}."
                )
            current_document.update(copy.deepcopy(dict(operation.after)))

        self.documents = staged
        self.commit_count += 1
        final_documents = tuple(
            copy.deepcopy(document)
            for document in self.documents
            if document.get("event_id") == plan.event_id
        )
        return SlotCommitReceipt(
            plan_id=plan.plan_id,
            previous_digest=plan.current_digest,
            applied_operation_ids=tuple(
                operation.operation_id for operation in plan.operations
            ),
            final_documents=final_documents,
        )


def canonical_documents(snapshot):
    return list(plan_slot_reconciliation(snapshot, []).desired_documents)


def conflict_codes(plan):
    return {conflict.code for conflict in plan.conflicts}


def test_plan_is_deterministic_and_does_not_mutate_inputs():
    snapshot = build_snapshot()
    current = [
        legacy_document(slot(snapshot, 301)),
        legacy_document(slot(snapshot, 302)),
    ]
    before_snapshot = copy.deepcopy(snapshot)
    before_current = copy.deepcopy(current)

    first = plan_slot_reconciliation(snapshot, current)
    second = plan_slot_reconciliation(snapshot, list(reversed(current)))

    assert first.as_dict() == second.as_dict()
    assert snapshot == before_snapshot
    assert current == before_current
    assert first.safe_to_apply
    assert {operation.action for operation in first.operations} == {
        "insert",
        "update",
    }


def test_legacy_and_missing_slots_converge_then_second_plan_is_noop():
    snapshot = build_snapshot()
    current = [
        legacy_document(
            slot(snapshot, 301), legacy_annotation="preserve this field"
        )
    ]
    store = FakeAtomicSlotStore(current)
    plan = plan_slot_reconciliation(snapshot, current)

    applied = apply_slot_reconciliation(
        plan,
        store,
        dry_run=False,
        expected_plan_id=plan.plan_id,
    )
    repeated = plan_slot_reconciliation(snapshot, store.documents)

    assert plan.summary() == {
        "desired_count": 3,
        "insert_count": 2,
        "update_count": 1,
        "unchanged_count": 0,
        "conflict_count": 0,
    }
    assert applied.verified_converged
    assert store.commit_count == 1
    assert store.documents[0]["legacy_annotation"] == "preserve this field"
    assert repeated.converged
    assert repeated.operations == ()
    assert repeated.summary()["unchanged_count"] == 3


def test_dry_run_never_calls_the_storage_port():
    snapshot = build_snapshot()
    store = FakeAtomicSlotStore([])
    plan = plan_slot_reconciliation(snapshot, [])

    result = apply_slot_reconciliation(plan, store, dry_run=True)

    assert result.dry_run
    assert result.attempted_operation_count == 3
    assert result.applied_operation_ids == ()
    assert store.commit_count == 0
    assert store.documents == []


def test_apply_requires_the_exact_reviewed_plan_id():
    plan = plan_slot_reconciliation(build_snapshot(), [])

    with pytest.raises(SlotReconciliationApplyError, match="expected_plan_id"):
        apply_slot_reconciliation(
            plan,
            FakeAtomicSlotStore([]),
            dry_run=False,
            expected_plan_id="different-plan",
        )


def test_reorder_updates_existing_canonical_slots_and_headliner_flags():
    initial = build_snapshot()
    current = canonical_documents(initial)
    updated = normalize_card_data_v1(
        [
            observation(
                "new-main",
                "slot",
                302,
                {"card_section": "main", "order_overall": 1},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin promoted bout 302.",
                identity_basis="admin",
            ),
            observation(
                "old-main-down",
                "slot",
                301,
                {"order_overall": 2},
                source_kind="admin_override",
                observed_at=T2,
                reason="Admin moved bout 301 to co-main.",
                identity_basis="admin",
            ),
        ],
        initial,
    ).snapshot

    plan = plan_slot_reconciliation(updated, current)
    by_bout = {operation.bout_id: operation for operation in plan.operations}

    assert plan.safe_to_apply
    assert set(by_bout) == {301, 302, 303}
    assert by_bout[302].after["is_main_event"] is True
    assert by_bout[302].after["is_co_main"] is False
    assert by_bout[301].after["is_main_event"] is False
    assert by_bout[301].after["is_co_main_event"] is True
    assert by_bout[303].changed_fields == (
        "card_snapshot_revision",
        "reconciliation_fingerprint",
    )


def test_cancelled_desired_slot_is_retained_and_marked_historical_not_deleted():
    initial = build_snapshot()
    current = canonical_documents(initial)
    cancelled = normalize_card_data_v1(
        [
            observation(
                "cancel-302",
                "bout",
                302,
                {"status": "cancelled"},
                source_kind="admin_override",
                observed_at=T2,
                reason="Bout officially cancelled.",
                identity_basis="admin",
            )
        ],
        initial,
    ).snapshot

    plan = plan_slot_reconciliation(cancelled, current)
    target = next(operation for operation in plan.operations if operation.bout_id == 302)

    assert target.action == "update"
    assert target.after["is_current"] is False
    assert target.after["slot_id"] == f"{EVENT_ID}:302"
    assert {operation.action for operation in plan.operations} <= {"insert", "update"}


def test_persisted_slot_absent_from_snapshot_blocks_instead_of_deleting():
    snapshot = build_snapshot()
    current = canonical_documents(snapshot)
    current.append(
        legacy_document(
            {
                "slot_id": f"{EVENT_ID}:999",
                "event_id": EVENT_ID,
                "bout_id": 999,
                "card_section": "prelim",
                "order_overall": 4,
                "order_section": 2,
                "role": "regular",
            }
        )
    )

    plan = plan_slot_reconciliation(snapshot, current)

    assert not plan.safe_to_apply
    assert "UNEXPECTED_PERSISTED_SLOT" in conflict_codes(plan)
    assert all(operation.action != "delete" for operation in plan.operations)
    with pytest.raises(UnsafeSlotReconciliationPlan):
        apply_slot_reconciliation(
            plan,
            FakeAtomicSlotStore(current),
            dry_run=False,
            expected_plan_id=plan.plan_id,
        )


def test_duplicate_persisted_bout_slots_block_the_plan():
    snapshot = build_snapshot()
    first = legacy_document(slot(snapshot, 301))
    duplicate = copy.deepcopy(first)
    duplicate["_id"] = f"{EVENT_ID}:duplicate-301"
    duplicate["id"] = f"{EVENT_ID}:duplicate-301"

    plan = plan_slot_reconciliation(snapshot, [first, duplicate])

    assert not plan.safe_to_apply
    assert "DUPLICATE_PERSISTED_BOUT_SLOT" in conflict_codes(plan)


def test_newer_persisted_snapshot_and_structure_revision_block_downgrade():
    snapshot = build_snapshot()
    current = canonical_documents(snapshot)
    current[0]["card_snapshot_revision"] = snapshot["snapshot_revision"] + 1
    current[0]["structure_revision"] = (
        snapshot["event"]["structure_revision"] + 1
    )
    current[0]["reconciliation_fingerprint"] = slot_storage_fingerprint(current[0])

    plan = plan_slot_reconciliation(snapshot, current)

    assert "STALE_DESIRED_SNAPSHOT" in conflict_codes(plan)
    assert not plan.safe_to_apply


def test_equal_canonical_revision_with_different_valid_content_is_conflict():
    snapshot = build_snapshot()
    current = canonical_documents(snapshot)
    current[0]["card_section"] = "prelim"
    current[0]["reconciliation_fingerprint"] = slot_storage_fingerprint(current[0])

    plan = plan_slot_reconciliation(snapshot, current)

    assert "REVISION_CONTENT_CONFLICT" in conflict_codes(plan)


def test_stored_fingerprint_detects_out_of_band_canonical_mutation():
    snapshot = build_snapshot()
    current = canonical_documents(snapshot)
    current[0]["card_section"] = "prelim"

    plan = plan_slot_reconciliation(snapshot, current)

    assert "STORED_FINGERPRINT_MISMATCH" in conflict_codes(plan)


def test_struct_capability_must_be_ready_even_for_valid_contract_snapshot():
    snapshot = build_snapshot()
    snapshot["quality"]["capabilities"]["STRUCT"] = "blocked"
    snapshot["quality"]["overall"] = "degraded"

    plan = plan_slot_reconciliation(snapshot, [])

    assert "STRUCT_CAPABILITY_NOT_READY" in conflict_codes(plan)
    assert not plan.safe_to_apply


def test_invalid_carddata_snapshot_is_rejected_before_planning():
    snapshot = build_snapshot()
    snapshot["card_slots"][1]["order_overall"] = 1

    with pytest.raises(SlotReconciliationInputError, match="ORDER_DUPLICATE"):
        plan_slot_reconciliation(snapshot, [])


def test_atomic_store_detects_event_digest_race_without_partial_application():
    snapshot = build_snapshot()
    current = [legacy_document(slot(snapshot, 301))]
    store = FakeAtomicSlotStore(current)
    plan = plan_slot_reconciliation(snapshot, current)
    store.documents[0]["card_section"] = "prelim"
    before = copy.deepcopy(store.documents)

    with pytest.raises(SlotReconciliationApplyError, match="digest changed"):
        apply_slot_reconciliation(
            plan,
            store,
            dry_run=False,
            expected_plan_id=plan.plan_id,
        )

    assert store.documents == before
    assert store.commit_count == 0


def test_bad_storage_receipt_is_rejected():
    snapshot = build_snapshot()
    plan = plan_slot_reconciliation(snapshot, [])

    class BadReceiptStore:
        def commit_slot_plan(self, received_plan):
            return SlotCommitReceipt(
                plan_id=received_plan.plan_id,
                previous_digest=received_plan.current_digest,
                applied_operation_ids=(),
                final_documents=(),
            )

    with pytest.raises(SlotReconciliationApplyError, match="every planned operation"):
        apply_slot_reconciliation(
            plan,
            BadReceiptStore(),
            dry_run=False,
            expected_plan_id=plan.plan_id,
        )


def test_legacy_projection_is_derived_from_one_canonical_slot():
    snapshot = build_snapshot()

    projection = legacy_bout_slot_projection(slot(snapshot, 301))

    assert projection == {
        "card_section": "main",
        "order_overall": 1,
        "order_section": 1,
        "card_order": 1,
        "is_main_event": True,
        "is_co_main": False,
        "is_co_main_event": False,
        "scheduled_start_time_utc": "2026-08-23T02:00:00Z",
        "automatic_lock_time_utc": "2026-08-23T02:00:00Z",
    }


def test_desired_storage_documents_have_version_fingerprint_and_legacy_flags():
    snapshot = build_snapshot()

    documents = canonical_documents(snapshot)
    main = next(document for document in documents if document["bout_id"] == 301)
    co_main = next(document for document in documents if document["bout_id"] == 302)

    assert main["canonical_slot_version"] == SLOT_STORAGE_VERSION
    assert main["source"] == "card_data_v1"
    assert main["reconciliation_fingerprint"] == slot_storage_fingerprint(main)
    assert main["is_main_event"] is True
    assert co_main["is_co_main"] is False
    assert co_main["is_co_main_event"] is False


def test_module_has_no_database_network_or_current_writer_imports_or_calls():
    source = (
        Path(__file__).parents[1]
        / "tapology_scraper"
        / "slot_reconciliation.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not imported & {
        "pymongo",
        "motor.motor_asyncio",
        "requests",
        "scrapy",
        "tapology_scraper.spiders.espn",
    }
    lowered = source.lower()
    for forbidden in (
        "update_one(",
        "insert_one(",
        "delete_one(",
        "bulk_write(",
        "mongodb_uri",
    ):
        assert forbidden not in lowered
