"""
Wave 1 Contract & Schema Compatibility Verification Suite (§3.1–§3.5).

Verifies:
1. All 7 Avro schemas in `contracts/` exist, parse as valid JSON, and follow namespace `org.scipulse.v1`.
2. `paper.v1.avsc` includes arXiv-specific nullable fields `comments` and `journal_ref` (§3.2) so
   GitHub link extraction in `enrich_projects.py` has access to all 3 fields (`abstract`, `comments`, `journal_ref`).
3. `dlq_envelope.v1.avsc` includes `stage` enum (`fetch`, `parse`, `map`, `validate`, `serialize`, `produce`),
   `reason`, `error`, `payload` (bytes), `original_topic`, `original_key`, `attempt`, and `rule_applied`.
4. Avro BACKWARD compatibility rules (§3.5):
   - Adding an optional field (`["null", "string"]` with `"default": null`) is BACKWARD-compatible.
   - Adding a required field without default or changing a field's type breaks BACKWARD compatibility.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest


def resolve_contracts_dir() -> Path:
    candidates = [
        Path(__file__).resolve().parent.parent / "contracts",
        Path(r"C:\Users\beish\orca\workspaces\Sci-Pulse\main\contracts"),
    ]
    for path in candidates:
        if path.is_dir():
            return path
    pytest.fail(f"Could not locate contracts/ directory in {candidates}")


EXPECTED_SCHEMAS: Dict[str, Set[str]] = {
    "paper.v1.avsc": {
        "source",
        "source_id",
        "doi",
        "arxiv_id",
        "title",
        "abstract",
        "comments",
        "journal_ref",
        "authors",
        "topics",
        "published_at",
        "updated_at",
        "cited_by_count",
        "links",
        "ingested_at",
    },
    "dlq_envelope.v1.avsc": {
        "reason",
        "stage",
        "source",
        "original_topic",
        "original_key",
        "payload",
        "error",
        "failed_at",
        "attempt",
        "rule_applied",
    },
    "repo_snapshot.v1.avsc": {
        "repo_full_name",
        "stars",
        "forks",
        "open_issues",
        "archived",
        "pushed_at",
        "snapshot_at",
    },
    "release.v1.avsc": {
        "repo_full_name",
        "tag",
        "name",
        "published_at",
        "prerelease",
        "fetched_at",
    },
    "code_event.v1.avsc": {
        "event_id",
        "type",
        "repo_full_name",
        "actor_login",
        "created_at",
        "replayed_at",
        "source_hour",
    },
    "software_record.v1.avsc": {
        "record_doi",
        "concept_doi",
        "version",
        "title",
        "published_at",
        "related_identifiers",
        "repo_full_name",
    },
    "synthetic_event.v1.avsc": {
        "event_type",
        "country",
        "latency_ms",
        "bytes",
        "produced_at",
        "synthetic",
    },
}


def check_backward_compatibility(
    old_schema: Dict[str, Any], new_schema: Dict[str, Any]
) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
    new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

    for name, nf in new_fields.items():
        if name not in old_fields:
            if "default" not in nf:
                errors.append(
                    f"Field '{name}' added in reader schema without a default value (breaks BACKWARD compatibility)"
                )
        else:
            of = old_fields[name]
            if of["type"] != nf["type"]:
                if not (isinstance(nf["type"], list) and of["type"] in nf["type"]):
                    errors.append(
                        f"Field '{name}' changed type from {of['type']} to {nf['type']} (breaks BACKWARD compatibility)"
                    )

    return len(errors) == 0, errors


@pytest.mark.parametrize("filename,expected_fields", EXPECTED_SCHEMAS.items())
def test_avro_contract_structure(filename: str, expected_fields: Set[str]) -> None:
    contracts_dir = resolve_contracts_dir()
    schema_path = contracts_dir / filename
    assert schema_path.is_file(), f"Missing contract file: {schema_path}"

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema.get("type") == "record"
    assert schema.get("namespace") == "org.scipulse.v1"

    actual_fields = {f["name"] for f in schema.get("fields", [])}
    missing = expected_fields - actual_fields
    assert not missing, f"{filename} is missing required spec fields: {sorted(missing)}"


def test_paper_v1_arxiv_fields_nullable_with_default() -> None:
    contracts_dir = resolve_contracts_dir()
    schema = json.loads((contracts_dir / "paper.v1.avsc").read_text(encoding="utf-8"))
    fields_by_name = {f["name"]: f for f in schema["fields"]}

    for field_name in ("comments", "journal_ref", "abstract", "doi", "arxiv_id"):
        assert field_name in fields_by_name, f"Missing {field_name} in paper.v1.avsc"
        field_spec = fields_by_name[field_name]
        assert field_spec["type"] == ["null", "string"], f"{field_name} must be nullable string union"
        assert field_spec.get("default") is None, f"{field_name} must have default null"


def test_dlq_envelope_v1_stages_and_raw_payload() -> None:
    contracts_dir = resolve_contracts_dir()
    schema = json.loads((contracts_dir / "dlq_envelope.v1.avsc").read_text(encoding="utf-8"))
    fields_by_name = {f["name"]: f for f in schema["fields"]}

    stage_type = fields_by_name["stage"]["type"]
    assert isinstance(stage_type, dict) and stage_type.get("type") == "enum"
    assert set(stage_type.get("symbols", [])) == {
        "fetch",
        "parse",
        "map",
        "validate",
        "serialize",
        "produce",
    }
    assert fields_by_name["payload"]["type"] == "bytes"


def test_backward_compatibility_evolution_rules() -> None:
    contracts_dir = resolve_contracts_dir()
    base_paper = json.loads((contracts_dir / "paper.v1.avsc").read_text(encoding="utf-8"))

    # Case 1: Adding optional nullable field with default=null -> MUST succeed
    valid_evolution = copy.deepcopy(base_paper)
    valid_evolution["fields"].append(
        {"name": "openaire_id", "type": ["null", "string"], "default": None}
    )
    ok, errors = check_backward_compatibility(base_paper, valid_evolution)
    assert ok, f"Expected valid optional field addition to pass BACKWARD check, got {errors}"

    # Case 2: Adding required field without default -> MUST fail
    invalid_required = copy.deepcopy(base_paper)
    invalid_required["fields"].append({"name": "mandatory_field", "type": "string"})
    ok, errors = check_backward_compatibility(base_paper, invalid_required)
    assert not ok and any("mandatory_field" in e for e in errors)

    # Case 3: Changing `title` from string to int -> MUST fail
    invalid_type = copy.deepcopy(base_paper)
    for field in invalid_type["fields"]:
        if field["name"] == "title":
            field["type"] = "int"
    ok, errors = check_backward_compatibility(base_paper, invalid_type)
    assert not ok and any("title" in e for e in errors)
