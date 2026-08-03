import copy
import io
import json
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from tapology_scraper.admin_title_attestation import parse_admin_title_attestation
from tapology_scraper.production_backfill_package import (
    AUTHORIZATION_SCHEMA_VERSION,
    EXIT_CONFIGURATION_ERROR,
    EXIT_PREPARED,
    MongoCardDataBackfillAdapter,
    ProductionBackfillDriftError,
    ProductionBackfillExecutionError,
    ProductionBackfillPackageError,
    TARGET_EVENT_IDS,
    TARGET_SPECS,
    create_preimage_key_file,
    decrypt_preimage_archive,
    encrypt_preimage_archive,
    main,
    parse_production_write_authorization,
    prepare_backfill_run,
    render_manifest_json,
    render_manifest_markdown,
    require_private_path,
    write_encrypted_preimages,
)
from tapology_scraper.production_card_audit import LegacyCardDocuments


PRIVATE_FIGHTER_MARKER = "PRIVATE FIGHTER NAME MUST NEVER APPEAR"
CREATED_AT = "2026-08-01T12:00:00Z"


def fighter(fighter_id):
    return {
        "fighter_id": fighter_id,
        "fighter_name": PRIVATE_FIGHTER_MARKER,
    }


def _sections_for_spec(spec):
    if spec.event_id == 136871:
        return ("main", "main", "prelim", "early_prelim", "early_prelim")
    if "early_prelim" in spec.expected_sections:
        return ("main", "prelim", "early_prelim")
    return ("main", "prelim")


def build_card(spec):
    sections = _sections_for_spec(spec)
    bout_ids = tuple(
        spec.event_id * 10 + index for index in range(1, len(sections) + 1)
    )
    status = "completed" if spec.expected_completed else "scheduled"
    event = {
        "id": spec.event_id,
        "name": spec.expected_name,
        "source": "espn",
        "promotion": "UFC",
        "official_event_date": "2026-08-01",
        "official_date_timezone": "America/New_York",
        "month_key": "2026-08",
        "status": status,
        "listed_bout_count": len(bout_ids),
        "mission_eligible_bout_count": len(bout_ids) - int(spec.event_id == 136871),
        "main_event_bout_id": bout_ids[0],
        "lifecycle_revision": 1,
        "structure_revision": 1,
        "timing_revision": 1,
        "espn_event_id": spec.expected_espn_event_id,
    }
    bouts = []
    slots = []
    section_counts = {}
    for index, (bout_id, section) in enumerate(zip(bout_ids, sections), start=1):
        section_counts[section] = section_counts.get(section, 0) + 1
        cancelled = spec.event_id == 136871 and index == len(bout_ids)
        bout_status = "cancelled" if cancelled else status
        bout = {
            "id": bout_id,
            "event_id": spec.event_id,
            "source": "espn",
            "espn_competition_id": str(bout_id + 900_000),
            "status": bout_status,
            "rounds_scheduled": 5 if index == 1 else 3,
            "fighters": {
                "red": fighter(f"fighter-{bout_id}-red"),
                "blue": fighter(f"fighter-{bout_id}-blue"),
            },
            "is_main_event": index == 1,
            "is_co_main_event": index == 2,
            "card_section": section,
            "order_overall": index,
            "order_section": section_counts[section],
        }
        if spec.expected_completed and not cancelled:
            bout["result"] = {
                "outcome": "red_win",
                "winner": "red",
                "method": "Decision - Unanimous",
                "ending_round": 3,
                "time": "5:00",
            }
        bouts.append(bout)
        slot_id = f"{spec.event_id}:{bout_id}"
        slots.append(
            {
                "_id": slot_id,
                "id": slot_id,
                "event_id": spec.event_id,
                "bout_id": bout_id,
                "source": "espn",
                "is_current": not cancelled,
                "card_section": section,
                "order_overall": index,
                "order_section": section_counts[section],
                "role": "main_event"
                if index == 1
                else "co_main"
                if index == 2
                else "regular",
                "is_main_event": index == 1,
                "is_co_main": index == 2,
                "scheduled_start_time_utc": f"2026-08-02T0{index}:00:00Z",
                "automatic_lock_time_utc": f"2026-08-02T0{index}:00:00Z",
                "structure_revision": 1,
            }
        )
    return LegacyCardDocuments(
        spec=spec,
        event=event,
        bouts=tuple(bouts),
        slots=tuple(slots),
    )


def build_cards():
    return tuple(build_card(spec) for spec in TARGET_SPECS)


def build_attestation(cards=None):
    cards = cards or build_cards()
    by_event = {card.spec.event_id: card for card in cards}
    return parse_admin_title_attestation(
        {
            "schema_version": "admin-title-attestation/v1",
            "attested_by": "Jose",
            "attested_at": "2026-08-01T00:00:00Z",
            "decision_ref": "D-DATA-012",
            "reason": "Complete synthetic package baseline.",
            "scope_event_ids": list(TARGET_EVENT_IDS),
            "title_bouts": [
                {
                    "event_id": 136871,
                    "bout_id": by_event[136871].bouts[0]["id"],
                    "title_type": "bmf",
                },
                {
                    "event_id": 142341,
                    "bout_id": by_event[142341].bouts[0]["id"],
                    "title_type": "undisputed",
                },
                {
                    "event_id": 142341,
                    "bout_id": by_event[142341].bouts[1]["id"],
                    "title_type": "undisputed",
                },
            ],
            "all_other_bouts_non_title": True,
            "authorized_use": "CARD_DATA_BACKFILL_DRY_RUN_ONLY",
            "production_write_authorized": False,
        }
    )


def prepared_run(cards=None):
    cards = cards or build_cards()
    return prepare_backfill_run(
        cards,
        build_attestation(cards),
        created_at=CREATED_AT,
    )


def authorization_for(run, **overrides):
    payload = {
        "schema_version": AUTHORIZATION_SCHEMA_VERSION,
        "authorized_by": "Jose",
        "authorized_at": "2026-08-01T13:00:00Z",
        "reason": "Exact reviewed run approved for test execution.",
        "run_id": run.manifest.run_id,
        "target_event_ids": list(TARGET_EVENT_IDS),
        "slot_plan_ids": [
            {"event_id": item.event_id, "plan_id": item.slot_plan_id}
            for item in run.manifest.event_plans
        ],
        "production_write_authorized": True,
    }
    payload.update(overrides)
    return parse_production_write_authorization(payload)


class FakeCursor:
    def __init__(self, documents):
        self.documents = [copy.deepcopy(item) for item in documents]

    def sort(self, field, direction):
        self.documents.sort(key=lambda item: item.get(field, 0), reverse=direction < 0)
        return self

    def max_time_ms(self, _value):
        return self

    def __iter__(self):
        return iter(copy.deepcopy(self.documents))


class FakeWriteResult:
    def __init__(self, matched_count=1):
        self.matched_count = matched_count
        self.acknowledged = True


def _matches(document, query):
    return all(document.get(key) == value for key, value in query.items())


class FakeCollection:
    def __init__(self, documents):
        self.documents = [copy.deepcopy(item) for item in documents]
        self.write_calls = []

    def find_one(self, query, _projection, **_kwargs):
        return next(
            (copy.deepcopy(item) for item in self.documents if _matches(item, query)),
            None,
        )

    def find(self, query, _projection, **_kwargs):
        return FakeCursor(item for item in self.documents if _matches(item, query))

    def update_one(self, query, update, **_kwargs):
        self.write_calls.append(
            ("update_one", copy.deepcopy(query), copy.deepcopy(update))
        )
        for document in self.documents:
            if _matches(document, query):
                document.update(copy.deepcopy(update["$set"]))
                return FakeWriteResult(1)
        return FakeWriteResult(0)

    def insert_one(self, document, **_kwargs):
        self.write_calls.append(("insert_one", copy.deepcopy(document)))
        if any(item.get("_id") == document.get("_id") for item in self.documents):
            raise RuntimeError("duplicate")
        self.documents.append(copy.deepcopy(document))
        return FakeWriteResult(1)


class FakeDatabase:
    def __init__(self, cards):
        self.collections = {
            "events": FakeCollection(card.event for card in cards),
            "bouts": FakeCollection(bout for card in cards for bout in card.bouts),
            "event_card_slots": FakeCollection(
                slot for card in cards for slot in card.slots
            ),
        }

    def __getitem__(self, name):
        return self.collections[name]

    def snapshot(self):
        return {
            name: copy.deepcopy(collection.documents)
            for name, collection in self.collections.items()
        }

    def restore(self, snapshot):
        for name, documents in snapshot.items():
            self.collections[name].documents = copy.deepcopy(documents)


class FakeTransaction:
    def __init__(self, database):
        self.database = database
        self.before = None

    def __enter__(self):
        self.before = self.database.snapshot()
        return self

    def __exit__(self, exc_type, _exc, _traceback):
        if exc_type is not None:
            self.database.restore(self.before)
        return False


class FakeSession:
    def __init__(self, database):
        self.database = database

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def start_transaction(self, **_kwargs):
        return FakeTransaction(self.database)


class FakeClient:
    def __init__(self, cards):
        self.database = FakeDatabase(cards)
        self.session_count = 0

    def get_database(self, _name):
        return self.database

    def start_session(self):
        self.session_count += 1
        return FakeSession(self.database)


def write_count(database):
    return sum(
        len(collection.write_calls) for collection in database.collections.values()
    )


def test_package_is_deterministic_reviewable_and_uses_compatibility_sidecars():
    cards = build_cards()
    first = prepared_run(cards)
    second = prepare_backfill_run(
        tuple(reversed(copy.deepcopy(cards))),
        build_attestation(cards),
        created_at="2026-08-01T14:00:00Z",
    )

    assert first.manifest.run_id == second.manifest.run_id
    assert first.manifest.target_event_ids == TARGET_EVENT_IDS
    assert len(first.event_plans) == 3
    assert all(item.result.review_status == "REVIEWABLE" for item in first.event_plans)
    assert all(
        item.event_write.changed_fields == ("card_data_v1",)
        for item in first.event_plans
    )
    assert all(
        operation.changed_fields == ("card_data_v1",)
        for item in first.event_plans
        for operation in item.bout_writes
    )
    assert first.manifest.as_dict()["production_write_authorized"] is False
    assert first.manifest.as_dict()["deletes_proposed"] == 0


def test_sanitized_manifest_never_contains_preimages_names_keys_or_paths():
    run = prepared_run()
    combined = render_manifest_json(run.manifest) + render_manifest_markdown(
        run.manifest
    )

    assert PRIVATE_FIGHTER_MARKER not in combined
    assert "fighters" not in combined
    assert "mongodb" not in combined.lower()
    assert "preimage_payload" not in combined
    assert "desired_values" not in combined
    assert '"production_write_authorized": false' in combined


def test_encrypted_preimages_round_trip_and_reject_wrong_key_or_digest():
    run = prepared_run()
    key = Fernet.generate_key()
    ciphertext, digest = encrypt_preimage_archive(run.preimage_archive, key)

    restored = decrypt_preimage_archive(
        ciphertext,
        key,
        expected_preimage_set_digest=run.manifest.preimage_set_digest,
    )

    assert restored["run_id"] == run.manifest.run_id
    assert PRIVATE_FIGHTER_MARKER not in ciphertext.decode("ascii")
    assert digest.startswith("sha256:")
    with pytest.raises(ProductionBackfillPackageError, match="invalid"):
        decrypt_preimage_archive(
            ciphertext,
            Fernet.generate_key(),
            expected_preimage_set_digest=run.manifest.preimage_set_digest,
        )
    with pytest.raises(ProductionBackfillPackageError, match="does not match"):
        decrypt_preimage_archive(
            ciphertext,
            key,
            expected_preimage_set_digest="sha256:" + "0" * 64,
        )


def test_private_artifacts_must_be_outside_workspace_and_never_overwrite(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    private = tmp_path / "private"
    private.mkdir()

    with pytest.raises(ProductionBackfillPackageError, match="outside"):
        require_private_path(workspace / "preimages.bin", (workspace,))

    key_path = private / "key.bin"
    key = create_preimage_key_file(key_path, (workspace,))
    assert key_path.read_bytes().strip() == key
    with pytest.raises(ProductionBackfillPackageError, match="overwrite"):
        create_preimage_key_file(key_path, (workspace,))

    run = write_encrypted_preimages(
        prepared_run(),
        private / "preimages.bin",
        key,
        (workspace,),
    )
    assert run.manifest.encrypted_archive_digest.startswith("sha256:")
    with pytest.raises(ProductionBackfillPackageError, match="overwrite"):
        write_encrypted_preimages(
            run,
            private / "preimages.bin",
            key,
            (workspace,),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "wrong"),
        ("authorized_at", "2026-08-01T07:00:00-06:00"),
        ("target_event_ids", [136871]),
        ("production_write_authorized", False),
    ],
)
def test_production_authorization_rejects_broad_or_unsafe_values(field, value):
    run = prepared_run()
    payload = authorization_for(run).canonical_payload()
    payload[field] = value

    with pytest.raises(ProductionBackfillPackageError):
        parse_production_write_authorization(payload)


def test_authorization_must_name_exact_run_and_every_plan():
    run = prepared_run()
    authorization = authorization_for(run)
    authorization.validate_run(run.manifest)

    wrong_run = authorization_for(run, run_id="different")
    with pytest.raises(ProductionBackfillPackageError, match="run ID"):
        wrong_run.validate_run(run.manifest)

    payload = authorization.canonical_payload()
    payload["slot_plan_ids"][0]["plan_id"] = "different"
    wrong_plan = parse_production_write_authorization(payload)
    with pytest.raises(ProductionBackfillPackageError, match="slot plan"):
        wrong_plan.validate_run(run.manifest)


def test_adapter_is_strictly_no_write_by_default():
    cards = build_cards()
    client = FakeClient(cards)
    run = prepared_run(cards)

    receipt = MongoCardDataBackfillAdapter(client, "ufc").execute(run)

    assert receipt.dry_run is True
    assert receipt.write_executed is False
    assert receipt.transaction_committed is False
    assert client.session_count == 0
    assert write_count(client.database) == 0


def test_adapter_executes_exact_transaction_then_replay_is_idempotent():
    cards = build_cards()
    client = FakeClient(cards)
    run = prepared_run(cards)
    authorization = authorization_for(run)
    adapter = MongoCardDataBackfillAdapter(client, "ufc")

    first = adapter.execute(run, authorization, execute=True)
    writes_after_first = write_count(client.database)
    second = adapter.execute(run, authorization, execute=True)

    assert first.write_executed is True
    assert first.transaction_committed is True
    assert first.post_commit_verified is True
    assert first.replay_converged is True
    assert writes_after_first > 0
    assert second.write_executed is False
    assert all(state == "ALREADY_CONVERGED" for _, state in second.event_states)
    assert write_count(client.database) == writes_after_first
    for event in client.database.collections["events"].documents:
        assert isinstance(event.get("card_data_v1"), dict)
        assert event["name"] in {spec.expected_name for spec in TARGET_SPECS}
    for bout in client.database.collections["bouts"].documents:
        assert isinstance(bout.get("fighters"), dict)
        assert isinstance(bout.get("card_data_v1"), dict)


def test_drift_aborts_before_any_write():
    cards = build_cards()
    client = FakeClient(cards)
    run = prepared_run(cards)
    client.database.collections["events"].documents[0]["status"] = "live"
    before = client.database.snapshot()

    with pytest.raises(ProductionBackfillDriftError):
        MongoCardDataBackfillAdapter(client, "ufc").execute(
            run,
            authorization_for(run),
            execute=True,
        )

    assert client.database.snapshot() == before
    assert write_count(client.database) == 0


def test_execute_requires_separate_authorization_before_starting_session():
    cards = build_cards()
    client = FakeClient(cards)
    run = prepared_run(cards)

    with pytest.raises(ProductionBackfillPackageError, match="separate exact"):
        MongoCardDataBackfillAdapter(client, "ufc").execute(run, execute=True)

    assert client.session_count == 0
    assert write_count(client.database) == 0


def test_mid_transaction_failure_restores_every_collection(monkeypatch):
    cards = build_cards()
    client = FakeClient(cards)
    run = prepared_run(cards)
    before = client.database.snapshot()
    collection = client.database.collections["bouts"]
    original_update = collection.update_one
    calls = {"count": 0}

    def fail_after_first_bout(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("synthetic transaction failure")
        return original_update(*args, **kwargs)

    monkeypatch.setattr(collection, "update_one", fail_after_first_bout)

    with pytest.raises(ProductionBackfillExecutionError, match="RuntimeError"):
        MongoCardDataBackfillAdapter(client, "ufc").execute(
            run,
            authorization_for(run),
            execute=True,
        )

    assert client.database.snapshot() == before


def test_package_rejects_partial_or_extra_event_scope():
    cards = build_cards()
    attestation = build_attestation(cards)

    with pytest.raises(ProductionBackfillPackageError, match="exactly"):
        prepare_backfill_run(cards[:2], attestation, created_at=CREATED_AT)
    with pytest.raises(ProductionBackfillPackageError, match="exactly"):
        prepare_backfill_run(
            (*cards, copy.deepcopy(cards[0])),
            attestation,
            created_at=CREATED_AT,
        )


def test_cli_prepares_only_encrypted_and_sanitized_outputs(monkeypatch, tmp_path):
    import tapology_scraper.production_backfill_package as module

    cards = build_cards()
    run = prepared_run(cards)
    env_file = tmp_path / ".env"
    env_file.write_text("MONGODB_URI=mongodb://not-used", encoding="utf-8")
    attestation_file = tmp_path / "title.json"
    attestation_file.write_text(
        json.dumps(build_attestation(cards).canonical_payload()),
        encoding="utf-8",
    )
    private_dir = tmp_path / "private"
    private_dir.mkdir()
    key_file = private_dir / "key.bin"
    archive = private_dir / "preimages.bin"
    manifest = tmp_path / "manifest.json"
    monkeypatch.setattr(
        module,
        "load_mongo_settings",
        lambda _path: ("mongodb://not-used", "ufc"),
    )
    monkeypatch.setattr(
        module,
        "run_package_read",
        lambda _uri, _database, _attestation, created_at: run,
    )
    stdout = io.StringIO()

    exit_code = main(
        [
            "--env-file",
            str(env_file),
            "--admin-title-attestation",
            str(attestation_file),
            "--preimage-key-file",
            str(key_file),
            "--create-key-file",
            "--preimage-output",
            str(archive),
            "--manifest-output",
            str(manifest),
            "--format",
            "json",
            "--created-at",
            CREATED_AT,
        ],
        stdout=stdout,
        stderr=io.StringIO(),
    )

    value = json.loads(manifest.read_text(encoding="utf-8"))
    assert exit_code == EXIT_PREPARED
    assert archive.is_file() and key_file.is_file() and manifest.is_file()
    assert value["write_executed"] is False
    assert value["production_write_authorized"] is False
    assert value["encrypted_archive"]["written_outside_workspace"] is True
    assert PRIVATE_FIGHTER_MARKER not in manifest.read_text(encoding="utf-8")
    assert "writes_executed=0" in stdout.getvalue()


def test_cli_refuses_existing_outputs_before_reading_production(monkeypatch, tmp_path):
    import tapology_scraper.production_backfill_package as module

    env_file = tmp_path / ".env"
    env_file.write_text("MONGODB_URI=mongodb://not-used", encoding="utf-8")
    attestation_file = tmp_path / "title.json"
    attestation_file.write_text("{}", encoding="utf-8")
    private_dir = tmp_path / "private"
    private_dir.mkdir()
    key_file = private_dir / "key.bin"
    key_file.write_bytes(Fernet.generate_key())
    archive = private_dir / "preimages.bin"
    archive.write_bytes(b"existing")
    manifest = tmp_path / "manifest.json"
    called = {"read": False}

    def unexpected_read(*_args, **_kwargs):
        called["read"] = True
        raise AssertionError

    monkeypatch.setattr(module, "run_package_read", unexpected_read)
    stderr = io.StringIO()

    exit_code = main(
        [
            "--env-file",
            str(env_file),
            "--admin-title-attestation",
            str(attestation_file),
            "--preimage-key-file",
            str(key_file),
            "--preimage-output",
            str(archive),
            "--manifest-output",
            str(manifest),
        ],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == EXIT_CONFIGURATION_ERROR
    assert called["read"] is False
    assert archive.read_bytes() == b"existing"
    assert "overwrite" in stderr.getvalue()


def test_source_cli_has_no_production_execute_switch_and_scope_is_fixed():
    import tapology_scraper.production_backfill_package as module

    source = Path(module.__file__).read_text(encoding="utf-8")

    assert '"--execute"' not in source
    assert "TARGET_EVENT_IDS = (136871, 142341, 142997)" in source
    assert 'TARGET_COLLECTIONS = ("events", "bouts", "event_card_slots")' in source
    assert ".delete_one(" not in source
    assert ".delete_many(" not in source
