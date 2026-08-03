"""Read-only mission-readiness audit for normalized CardData V1 snapshots.

The CLI reads local JSON (or stdin), delegates contract invariants to
``card_data_contract``, and renders deterministic JSON or Markdown.  It has no
MongoDB, network, scraper, or writer dependency.  Without ``--output`` it does
not write any file.

Exit codes:

* 0: every snapshot is contract-valid and all explicitly required
  capabilities are ready;
* 1: the audit completed and found invalid snapshots, duplicate snapshot IDs,
  or a required capability that is not ready;
* 2: arguments or input JSON could not be read safely.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional, TextIO

from tapology_scraper.card_data_contract import (
    CAPABILITIES,
    CAPABILITY_STATES,
    ContractIssue,
    validate_card_data_v1,
)


AUDIT_SCHEMA_VERSION = "mission-readiness-audit/v1"
EXIT_READY = 0
EXIT_FINDINGS = 1
EXIT_INPUT_ERROR = 2


class AuditInputError(ValueError):
    """Raised when local CLI input cannot be interpreted safely."""


@dataclass(frozen=True)
class SnapshotInput:
    source: str
    value: Any


@dataclass(frozen=True)
class CapabilityAudit:
    capability: str
    declared_state: str
    effective_state: str
    blocking_issue_codes: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.effective_state == "ready"

    def as_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "declared_state": self.declared_state,
            "effective_state": self.effective_state,
            "ready": self.ready,
            "blocking_issue_codes": list(self.blocking_issue_codes),
        }


@dataclass(frozen=True)
class DeclaredQualityIssue:
    code: str
    severity: str
    scope_type: str
    scope_id: Optional[str]
    field: str
    message: str
    blocks_capabilities: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["blocks_capabilities"] = list(self.blocks_capabilities)
        return value


@dataclass(frozen=True)
class SnapshotAudit:
    source: str
    snapshot_id: Optional[str]
    event_id: Optional[int]
    snapshot_revision: Optional[int]
    event_name: Optional[str]
    declared_overall: str
    contract_valid: bool
    required_capabilities_ready: bool
    counts: tuple[tuple[str, int], ...]
    capabilities: tuple[CapabilityAudit, ...]
    contract_issues: tuple[ContractIssue, ...]
    declared_quality_issues: tuple[DeclaredQualityIssue, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            "event_id": self.event_id,
            "snapshot_revision": self.snapshot_revision,
            "event_name": self.event_name,
            "declared_overall": self.declared_overall,
            "contract_valid": self.contract_valid,
            "required_capabilities_ready": self.required_capabilities_ready,
            "counts": dict(self.counts),
            "capabilities": [item.as_dict() for item in self.capabilities],
            "contract_issues": [item.as_dict() for item in self.contract_issues],
            "declared_quality_issues": [
                item.as_dict() for item in self.declared_quality_issues
            ],
        }


@dataclass(frozen=True)
class AuditIssue:
    code: str
    source: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ReadinessAuditReport:
    required_capabilities: tuple[str, ...]
    snapshots: tuple[SnapshotAudit, ...]
    audit_issues: tuple[AuditIssue, ...]

    @property
    def passed(self) -> bool:
        return (
            not self.audit_issues
            and all(snapshot.contract_valid for snapshot in self.snapshots)
            and all(
                snapshot.required_capabilities_ready for snapshot in self.snapshots
            )
        )

    @property
    def exit_code(self) -> int:
        return EXIT_READY if self.passed else EXIT_FINDINGS

    def summary(self) -> dict[str, Any]:
        capability_counts: dict[str, dict[str, int]] = {}
        for capability in CAPABILITIES:
            states = Counter(
                next(
                    item.effective_state
                    for item in snapshot.capabilities
                    if item.capability == capability
                )
                for snapshot in self.snapshots
            )
            capability_counts[capability] = {
                state: states.get(state, 0)
                for state in ("ready", "pending", "blocked", "quarantined")
            }

        total = len(self.snapshots)
        valid = sum(snapshot.contract_valid for snapshot in self.snapshots)
        ready_for_required = sum(
            snapshot.contract_valid and snapshot.required_capabilities_ready
            for snapshot in self.snapshots
        )
        return {
            "snapshot_count": total,
            "contract_valid_count": valid,
            "contract_invalid_count": total - valid,
            "ready_for_required_count": ready_for_required,
            "not_ready_for_required_count": total - ready_for_required,
            "audit_issue_count": len(self.audit_issues),
            "capability_counts": capability_counts,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "passed": self.passed,
            "required_capabilities": list(self.required_capabilities),
            "summary": self.summary(),
            "audit_issues": [issue.as_dict() for issue in self.audit_issues],
            "snapshots": [snapshot.as_dict() for snapshot in self.snapshots],
        }


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


def _is_list(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _string(value: Any, fallback: str = "unknown") -> str:
    if isinstance(value, str) and value.strip():
        return value
    return fallback


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if _is_mapping(value) else {}


def _list(value: Any) -> Sequence[Any]:
    return value if _is_list(value) else ()


def _declared_quality_issues(snapshot: Any) -> tuple[DeclaredQualityIssue, ...]:
    root = _mapping(snapshot)
    quality = _mapping(root.get("quality"))
    normalized: list[DeclaredQualityIssue] = []
    for raw_issue in _list(quality.get("issues")):
        issue = _mapping(raw_issue)
        blocks = tuple(
            sorted(
                {
                    item
                    for item in _list(issue.get("blocks_capabilities"))
                    if isinstance(item, str) and item in CAPABILITIES
                }
            )
        )
        scope_id = issue.get("scope_id")
        normalized.append(
            DeclaredQualityIssue(
                code=_string(issue.get("code")),
                severity=_string(issue.get("severity")),
                scope_type=_string(issue.get("scope_type")),
                scope_id=None if scope_id is None else str(scope_id),
                field=_string(issue.get("field")),
                message=_string(issue.get("message")),
                blocks_capabilities=blocks,
            )
        )
    return tuple(
        sorted(
            normalized,
            key=lambda item: (
                item.scope_type,
                "" if item.scope_id is None else item.scope_id,
                item.field,
                item.code,
                item.message,
            ),
        )
    )


def _capability_audits(
    snapshot: Any,
    contract_issues: Sequence[ContractIssue],
    declared_issues: Sequence[DeclaredQualityIssue],
) -> tuple[CapabilityAudit, ...]:
    root = _mapping(snapshot)
    quality = _mapping(root.get("quality"))
    declared_capabilities = _mapping(quality.get("capabilities"))
    audits: list[CapabilityAudit] = []

    for capability in CAPABILITIES:
        declared = declared_capabilities.get(capability)
        declared_state = declared if declared in CAPABILITY_STATES else "unknown"
        contract_codes = {
            issue.code
            for issue in contract_issues
            if capability in issue.blocks_capabilities
        }
        declared_codes = {
            issue.code
            for issue in declared_issues
            if capability in issue.blocks_capabilities
        }
        blocking_codes = tuple(sorted(contract_codes | declared_codes))

        if contract_codes or declared_state == "unknown":
            effective_state = "quarantined"
        elif declared_state == "quarantined":
            effective_state = "quarantined"
        elif declared_codes:
            effective_state = "blocked"
        else:
            effective_state = declared_state

        audits.append(
            CapabilityAudit(
                capability=capability,
                declared_state=declared_state,
                effective_state=effective_state,
                blocking_issue_codes=blocking_codes,
            )
        )
    return tuple(audits)


def _snapshot_counts(snapshot: Any) -> tuple[tuple[str, int], ...]:
    root = _mapping(snapshot)
    bouts = _list(root.get("bouts"))
    slots = _list(root.get("card_slots"))
    eligibility = _mapping(root.get("current_eligibility"))
    eligible_targets = _list(eligibility.get("eligible_targets"))
    final_results = sum(
        1
        for raw_bout in bouts
        if _mapping(_mapping(raw_bout).get("result")).get("status")
        in {"final", "corrected"}
    )
    current_slots = sum(
        1 for raw_slot in slots if _mapping(raw_slot).get("is_current") is True
    )
    return (
        ("bouts", len(bouts)),
        ("current_slots", current_slots),
        ("eligible_targets", len(eligible_targets)),
        ("final_results", final_results),
    )


def audit_snapshot(
    snapshot_input: SnapshotInput,
    required_capabilities: Sequence[str] = (),
) -> SnapshotAudit:
    """Audit one snapshot without changing the supplied object."""

    snapshot = snapshot_input.value
    validation = validate_card_data_v1(snapshot)
    root = _mapping(snapshot)
    event = _mapping(root.get("event"))
    quality = _mapping(root.get("quality"))
    declared_issues = _declared_quality_issues(snapshot)
    capability_audits = _capability_audits(
        snapshot, validation.issues, declared_issues
    )
    capability_by_id = {
        item.capability: item for item in capability_audits
    }
    required_ready = all(
        capability_by_id[capability].ready for capability in required_capabilities
    )

    raw_event_id = event.get("event_id")
    event_id = raw_event_id if type(raw_event_id) is int else None
    raw_revision = root.get("snapshot_revision")
    revision = raw_revision if type(raw_revision) is int else None
    snapshot_id = root.get("snapshot_id")
    event_name = event.get("name")

    return SnapshotAudit(
        source=snapshot_input.source,
        snapshot_id=snapshot_id if isinstance(snapshot_id, str) else None,
        event_id=event_id,
        snapshot_revision=revision,
        event_name=event_name if isinstance(event_name, str) else None,
        declared_overall=_string(quality.get("overall")),
        contract_valid=validation.is_valid,
        required_capabilities_ready=required_ready,
        counts=_snapshot_counts(snapshot),
        capabilities=capability_audits,
        contract_issues=validation.issues,
        declared_quality_issues=declared_issues,
    )


def audit_snapshots(
    snapshot_inputs: Iterable[SnapshotInput],
    required_capabilities: Sequence[str] = (),
) -> ReadinessAuditReport:
    """Build a deterministic readiness report for one or more snapshots."""

    required = normalize_required_capabilities(required_capabilities)
    snapshots = [audit_snapshot(item, required) for item in snapshot_inputs]
    snapshots.sort(
        key=lambda item: (
            item.event_id is None,
            item.event_id if item.event_id is not None else 0,
            item.snapshot_revision is None,
            item.snapshot_revision if item.snapshot_revision is not None else 0,
            item.snapshot_id or "",
            item.source,
        )
    )

    snapshot_sources: dict[str, list[str]] = {}
    for snapshot in snapshots:
        if snapshot.snapshot_id is not None:
            snapshot_sources.setdefault(snapshot.snapshot_id, []).append(snapshot.source)
    audit_issues = [
        AuditIssue(
            code="DUPLICATE_SNAPSHOT_ID",
            source=", ".join(sorted(sources)),
            message=f"Snapshot ID {snapshot_id!r} appears {len(sources)} times.",
        )
        for snapshot_id, sources in sorted(snapshot_sources.items())
        if len(sources) > 1
    ]
    if not snapshots:
        audit_issues.append(
            AuditIssue(
                code="NO_SNAPSHOTS",
                source="<inputs>",
                message="At least one snapshot is required.",
            )
        )

    return ReadinessAuditReport(
        required_capabilities=required,
        snapshots=tuple(snapshots),
        audit_issues=tuple(audit_issues),
    )


def normalize_required_capabilities(values: Sequence[str]) -> tuple[str, ...]:
    """Parse repeated/comma-delimited capability arguments in contract order."""

    requested: set[str] = set()
    for raw_value in values:
        for token in raw_value.split(","):
            capability = token.strip().upper()
            if not capability:
                continue
            if capability == "ALL":
                requested.update(CAPABILITIES)
                continue
            if capability not in CAPABILITIES:
                valid = ", ".join(CAPABILITIES)
                raise AuditInputError(
                    f"Unknown capability {capability!r}; expected one of {valid}, ALL."
                )
            requested.add(capability)
    return tuple(capability for capability in CAPABILITIES if capability in requested)


def _expand_json_value(value: Any, source: str) -> list[SnapshotInput]:
    if _is_mapping(value) and "contract_version" not in value and "snapshots" in value:
        snapshots = value.get("snapshots")
        if not _is_list(snapshots):
            raise AuditInputError(f"{source}: snapshots must be an array.")
        values = list(snapshots)
    elif _is_list(value):
        values = list(value)
    else:
        values = [value]

    if not values:
        raise AuditInputError(f"{source}: input contains no snapshots.")
    if len(values) == 1:
        return [SnapshotInput(source=source, value=values[0])]
    return [
        SnapshotInput(source=f"{source}#{index}", value=item)
        for index, item in enumerate(values, start=1)
    ]


def _load_json_text(text: str, source: str) -> list[SnapshotInput]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AuditInputError(
            f"{source}: invalid JSON at line {exc.lineno}, column {exc.colno}."
        ) from exc
    return _expand_json_value(value, source)


def _input_files(path_values: Sequence[str]) -> list[Path]:
    files: list[Path] = []
    for raw_path in path_values:
        if raw_path == "-":
            continue
        path = Path(raw_path)
        if not path.exists():
            raise AuditInputError(f"Input path does not exist: {raw_path}")
        if path.is_dir():
            directory_files = sorted(
                candidate for candidate in path.rglob("*.json") if candidate.is_file()
            )
            if not directory_files:
                raise AuditInputError(f"Input directory has no JSON files: {raw_path}")
            files.extend(directory_files)
        elif path.is_file():
            files.append(path)
        else:
            raise AuditInputError(f"Input path is not a regular file: {raw_path}")
    return files


def load_snapshot_inputs(
    path_values: Sequence[str],
    stdin: TextIO = sys.stdin,
) -> tuple[SnapshotInput, ...]:
    """Read local files/directories or one stdin stream; never access a DB/network."""

    if not path_values:
        raise AuditInputError("At least one input path or '-' for stdin is required.")
    if path_values.count("-") > 1:
        raise AuditInputError("Stdin ('-') may be specified only once.")

    loaded: list[SnapshotInput] = []
    if "-" in path_values:
        loaded.extend(_load_json_text(stdin.read(), "<stdin>"))
    for path in _input_files(path_values):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise AuditInputError(f"Could not read {path}: {exc}") from exc
        loaded.extend(_load_json_text(text, path.as_posix()))
    return tuple(loaded)


def render_json(report: ReadinessAuditReport) -> str:
    """Render stable machine-readable output with no wall-clock fields."""

    return json.dumps(
        report.as_dict(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def _markdown(value: Any) -> str:
    if value is None:
        return "—"
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _state_counts_cell(counts: Mapping[str, int]) -> str:
    return ", ".join(
        f"{state}={counts.get(state, 0)}"
        for state in ("ready", "pending", "blocked", "quarantined")
    )


def render_markdown(report: ReadinessAuditReport) -> str:
    """Render a compact human-review report from the same structured result."""

    summary = report.summary()
    required = ", ".join(report.required_capabilities) or "None (contract validity only)"
    lines = [
        "# Mission Readiness Audit",
        "",
        f"- Schema: `{AUDIT_SCHEMA_VERSION}`",
        f"- Result: `{'PASS' if report.passed else 'FINDINGS'}`",
        f"- Required capabilities: `{required}`",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|---|---:|",
        f"| Snapshots | {summary['snapshot_count']} |",
        f"| Contract-valid | {summary['contract_valid_count']} |",
        f"| Contract-invalid | {summary['contract_invalid_count']} |",
        f"| Ready for required capabilities | {summary['ready_for_required_count']} |",
        f"| Not ready for required capabilities | {summary['not_ready_for_required_count']} |",
        f"| Audit-level issues | {summary['audit_issue_count']} |",
        "",
        "## Capability coverage",
        "",
        "| Capability | Effective states |",
        "|---|---|",
    ]
    for capability in CAPABILITIES:
        lines.append(
            f"| {capability} | "
            f"{_state_counts_cell(summary['capability_counts'][capability])} |"
        )

    if report.audit_issues:
        lines.extend(
            [
                "",
                "## Audit-level issues",
                "",
                "| Code | Source | Message |",
                "|---|---|---|",
            ]
        )
        for issue in report.audit_issues:
            lines.append(
                f"| {_markdown(issue.code)} | {_markdown(issue.source)} | "
                f"{_markdown(issue.message)} |"
            )

    lines.extend(["", "## Snapshots"])
    for snapshot in report.snapshots:
        title = snapshot.event_name or snapshot.snapshot_id or snapshot.source
        counts = dict(snapshot.counts)
        lines.extend(
            [
                "",
                f"### {_markdown(title)}",
                "",
                f"- Source: `{_markdown(snapshot.source)}`",
                f"- Snapshot: `{_markdown(snapshot.snapshot_id)}`",
                f"- Event/revision: `{_markdown(snapshot.event_id)}` / "
                f"`{_markdown(snapshot.snapshot_revision)}`",
                f"- Contract: `{'VALID' if snapshot.contract_valid else 'INVALID'}`",
                f"- Declared quality: `{_markdown(snapshot.declared_overall)}`",
                f"- Required capabilities: "
                f"`{'READY' if snapshot.required_capabilities_ready else 'NOT READY'}`",
                f"- Counts: bouts={counts['bouts']}, current_slots={counts['current_slots']}, "
                f"eligible_targets={counts['eligible_targets']}, "
                f"final_results={counts['final_results']}",
                "",
                "| Capability | Declared | Effective | Blocking issue codes |",
                "|---|---|---|---|",
            ]
        )
        for capability in snapshot.capabilities:
            blockers = ", ".join(capability.blocking_issue_codes) or "—"
            lines.append(
                f"| {capability.capability} | {capability.declared_state} | "
                f"{capability.effective_state} | {_markdown(blockers)} |"
            )

        lines.extend(
            [
                "",
                "#### Contract issues",
                "",
            ]
        )
        if snapshot.contract_issues:
            lines.extend(
                [
                    "| Code | Scope | Field | Blocks | Message |",
                    "|---|---|---|---|---|",
                ]
            )
            for issue in snapshot.contract_issues:
                scope = f"{issue.scope_type}:{issue.scope_id or '—'}"
                blocks = ", ".join(issue.blocks_capabilities)
                lines.append(
                    f"| {_markdown(issue.code)} | {_markdown(scope)} | "
                    f"{_markdown(issue.field)} | {_markdown(blocks)} | "
                    f"{_markdown(issue.message)} |"
                )
        else:
            lines.append("None.")

        lines.extend(["", "#### Declared data-quality issues", ""])
        if snapshot.declared_quality_issues:
            lines.extend(
                [
                    "| Code | Severity | Scope | Field | Blocks | Message |",
                    "|---|---|---|---|---|---|",
                ]
            )
            for issue in snapshot.declared_quality_issues:
                scope = f"{issue.scope_type}:{issue.scope_id or '—'}"
                blocks = ", ".join(issue.blocks_capabilities) or "—"
                lines.append(
                    f"| {_markdown(issue.code)} | {_markdown(issue.severity)} | "
                    f"{_markdown(scope)} | {_markdown(issue.field)} | "
                    f"{_markdown(blocks)} | {_markdown(issue.message)} |"
                )
        else:
            lines.append("None.")

    return "\n".join(lines) + "\n"


def _output_is_inside(output: Path, candidate_parent: Path) -> bool:
    try:
        output.relative_to(candidate_parent)
    except ValueError:
        return False
    return True


def ensure_safe_output(output_value: str, input_values: Sequence[str]) -> Optional[Path]:
    """Prevent an explicit report from overwriting or entering its input set."""

    if output_value == "-":
        return None
    output = Path(output_value).resolve()
    for raw_input in input_values:
        if raw_input == "-":
            continue
        input_path = Path(raw_input).resolve()
        if input_path.is_file() and output == input_path:
            raise AuditInputError("Output path must not overwrite an input snapshot.")
        if input_path.is_dir() and _output_is_inside(output, input_path):
            raise AuditInputError(
                "Output path must not be inside an audited input directory."
            )
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit normalized CardData V1 JSON snapshots for mission readiness "
            "without MongoDB or network access."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="JSON file/directory paths; use '-' once to read JSON from stdin.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Report format (default: json).",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Explicit report path; default '-' writes only to stdout.",
    )
    parser.add_argument(
        "--require",
        action="append",
        default=[],
        metavar="CAP[,CAP]",
        help=(
            "Fail when listed capabilities are not ready. Repeatable; accepts "
            "EVT, EVT_DATE, BOUT, ELIG, STRUCT, TITLE, RES or ALL."
        ),
    )
    return parser


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        required = normalize_required_capabilities(args.require)
        output_path = ensure_safe_output(args.output, args.inputs)
        inputs = load_snapshot_inputs(args.inputs, stdin=stdin)
        report = audit_snapshots(inputs, required)
        rendered = (
            render_json(report)
            if args.format == "json"
            else render_markdown(report)
        )
        if output_path is None:
            stdout.write(rendered)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered, encoding="utf-8")
        return report.exit_code
    except AuditInputError as exc:
        stderr.write(f"error: {exc}\n")
        return EXIT_INPUT_ERROR
    except OSError as exc:
        stderr.write(f"error: could not write report: {exc}\n")
        return EXIT_INPUT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA_VERSION",
    "EXIT_FINDINGS",
    "EXIT_INPUT_ERROR",
    "EXIT_READY",
    "AuditInputError",
    "CapabilityAudit",
    "ReadinessAuditReport",
    "SnapshotAudit",
    "SnapshotInput",
    "audit_snapshot",
    "audit_snapshots",
    "load_snapshot_inputs",
    "main",
    "normalize_required_capabilities",
    "render_json",
    "render_markdown",
]
